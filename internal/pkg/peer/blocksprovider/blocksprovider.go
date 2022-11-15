/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider

import (
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/gossip"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/replication"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

type sleeper struct {
	sleep func(time.Duration)
}

func (s sleeper) Sleep(d time.Duration, doneC chan struct{}) {
	if s.sleep == nil {
		timer := time.NewTimer(d)
		select {
		case <-timer.C:
		case <-doneC:
			timer.Stop()
		}
		return
	}
	s.sleep(d)
}

// LedgerInfo an adapter to provide the interface to query
// the ledger committer for current ledger height
//
//go:generate counterfeiter -o fake/ledger_info.go --fake-name LedgerInfo . LedgerInfo
type LedgerInfo interface {
	// LedgerHeight returns current local ledger height
	LedgerHeight() (uint64, error)
}

// GossipServiceAdapter serves to provide basic functionality
// required from gossip service by delivery service
//
//go:generate counterfeiter -o fake/gossip_service_adapter.go --fake-name GossipServiceAdapter . GossipServiceAdapter
type GossipServiceAdapter interface {
	// AddPayload adds payload to the local state sync buffer
	AddPayload(chainID string, payload *gossip.Payload) error

	// Gossip the message across the peers
	Gossip(msg *gossip.GossipMessage)
}

//go:generate counterfeiter -o fake/config_provider.go --fake-name ConfigProvider . ConfigProvider
type ConfigProvider interface {
	// Capabilities defines the capabilities for the application portion of this channel
	Capabilities() channelconfig.ApplicationCapabilities
	// Resources provide the config resource bundle
	Resources() channelconfig.Resources
}

// Deliverer the actual implementation for BlocksProvider interface
type Deliverer struct {
	ChannelID      string
	Gossip         GossipServiceAdapter
	Ledger         LedgerInfo
	ClientConfig   comm.ClientConfig
	DoneC          chan struct{}
	Signer         identity.SignerSerializer
	ConfigProvider ConfigProvider
	Logger         *flogging.FabricLogger
	BCCSP          bccsp.BCCSP

	BlockGossipDisabled bool
	// TLSCertHash should be nil when TLS is not enabled
	TLSCertHash []byte
	sleeper     sleeper
	stop        bool
}

const backoffExponentBase = 1.2

// DeliverBlocks used to pull out blocks from the ordering service to
// distributed them across peers
func (d *Deliverer) DeliverBlocks() {
	if d.BlockGossipDisabled {
		d.Logger.Infof("Will pull blocks without forwarding them to remote peers via gossip")
	}

	fc := &replication.FetcherConfig{ // TODO these value need to be pulled from config
		Channel:                      d.ChannelID,
		PeriodicalShuffleInterval:    200 * time.Second,
		CensorshipSuspicionThreshold: 10 * time.Second,
		FetchTimeout:                 20 * time.Second,
		RetryTimeout:                 5 * time.Millisecond,
		MaxRetries:                   100,
		BufferSize:                   2097152,
		TLSCertHash:                  d.TLSCertHash,
	}

	config := d.ConfigProvider.Resources()

	blockSigVerifier := &replication.BlockValidationPolicyVerifier{
		Logger:    d.Logger,
		PolicyMgr: config.PolicyManager(),
		Channel:   fc.Channel,
		BCCSP:     d.BCCSP,
	}
	verifyBlockSequence := func(blocks []*common.Block, _ string) error {
		return replication.VerifyBlocks(blocks, blockSigVerifier)
	}

	fetcher, err := replication.NewBlockFetcher(*fc, d.Signer, d.BCCSP, config, d.ClientConfig, verifyBlockSequence, d.DoneC)
	if err != nil {
		d.Logger.Error("Upable to instantiate block fetcher, something is critically wrong", err)
		return
	}

	ledgerHeight, err := d.Ledger.LedgerHeight()
	if err != nil {
		d.Logger.Error("Did not return ledger height, something is critically wrong", err)
		return
	}

	for !d.stop {
		d.Logger.Debugw("Waiting for next block", "ledgerHeight", ledgerHeight)
		block := fetcher.PullBlock(ledgerHeight)
		if block == nil {
			d.sleeper.Sleep(time.Second, d.DoneC) // TODO experimental
			continue
		}

		// if it's a config block, wait until the block height has incremented before processing it
		// so that any potential signature changes will have been committed.
		if protoutil.IsConfigBlock(block) {
			seq := block.GetHeader().GetNumber()
			currentHeight, err := d.Ledger.LedgerHeight()
			if err != nil {
				d.Logger.Error("Did not return ledger height, something is critically wrong", err)
				return
			}
			d.Logger.Infow("Received config block, wait for height to be incremented", "seq", seq)
			for currentHeight < seq {
				d.Logger.Debugw("current height", "currentHeight", currentHeight)
				d.sleeper.Sleep(10*time.Millisecond, d.DoneC) // TODO
				currentHeight, err = d.Ledger.LedgerHeight()
				if err != nil {
					d.Logger.Error("Did not return ledger height, something is critically wrong", err)
					return
				}
			}
		}

		err = d.processBlock(block, ledgerHeight)
		if err != nil {
			d.Logger.Error("Failed to process block", err)
		}
		ledgerHeight++
	}
}

func (d *Deliverer) processBlock(block *common.Block, seq uint64) error {
	blockNum := block.GetHeader().Number

	marshaledBlock, err := proto.Marshal(block)
	if err != nil {
		return errors.WithMessage(err, "block from orderer could not be re-marshaled")
	}

	// Create payload with a block received
	payload := &gossip.Payload{
		Data:   marshaledBlock,
		SeqNum: blockNum,
	}

	// Use payload to create gossip message
	gossipMsg := &gossip.GossipMessage{
		Nonce:   0,
		Tag:     gossip.GossipMessage_CHAN_AND_ORG,
		Channel: []byte(d.ChannelID),
		Content: &gossip.GossipMessage_DataMsg{
			DataMsg: &gossip.DataMessage{
				Payload: payload,
			},
		},
	}

	d.Logger.Debugf("Adding payload to local buffer, blockNum = [%d]", blockNum)
	// Add payload to local state payloads buffer
	if err := d.Gossip.AddPayload(d.ChannelID, payload); err != nil {
		d.Logger.Warningf("Block [%d] received from ordering service wasn't added to payload buffer: %v", blockNum, err)
		return errors.WithMessage(err, "could not add block as payload")
	}
	if d.BlockGossipDisabled {
		return nil
	}
	// Gossip messages with other nodes
	d.Logger.Debugf("Gossiping block [%d]", blockNum)
	d.Gossip.Gossip(gossipMsg)
	return nil
}

// Stop stops blocks delivery provider
func (d *Deliverer) Stop() {
	// this select is not race-safe, but it prevents a panic
	// for careless callers multiply invoking stop
	d.stop = true
	select {
	case <-d.DoneC:
	default:
		close(d.DoneC)
	}
}
