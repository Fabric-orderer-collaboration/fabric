/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package etcdraft

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/follower"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/pkg/errors"
)

const (
	// compute endpoint shuffle timeout from FetchTimeout
	// endpoint shuffle timeout should be shuffleTimeoutMultiplier times FetchTimeout
	shuffleTimeoutMultiplier = 10
	// timeout expressed as a percentage of FetchtimeOut
	// if PullBlock returns before (shuffleTimeoutPercentage% of FetchTimeOut of blockfetcher)
	// the source is shuffled.
	shuffleTimeoutPercentage = int64(50)
)

// LedgerBlockPuller pulls blocks upon demand, or fetches them from the ledger
type LedgerBlockPuller struct {
	BlockPuller
	BlockRetriever cluster.BlockRetriever
	Height         func() uint64
}

func (lp *LedgerBlockPuller) PullBlock(seq uint64) *common.Block {
	lastSeq := lp.Height() - 1
	if lastSeq >= seq {
		return lp.BlockRetriever.Block(seq)
	}
	return lp.BlockPuller.PullBlock(seq)
}

func lastConfigBlockFromSupport(support consensus.ConsenterSupport) (*common.Block, error) {
	lastBlockSeq := support.Height() - 1
	lastBlock := support.Block(lastBlockSeq)
	if lastBlock == nil {
		return nil, errors.Errorf("unable to retrieve block [%d]", lastBlockSeq)
	}
	lastConfigBlock, err := cluster.LastConfigBlock(lastBlock, support)
	if err != nil {
		return nil, err
	}
	return lastConfigBlock, nil
}

// NewBlockFetcher creates a new block fetcher
func NewBlockFetcher(support consensus.ConsenterSupport,
	dialerConfig comm.ClientConfig,
	clusterConfig localconfig.Cluster,
	bccsp bccsp.BCCSP,
) (BlockPuller, error) {
	lastConfigBlock, err := lastConfigBlockFromSupport(support)
	if err != nil {
		return nil, err
	}

	bpf, err := follower.NewBlockPullerCreator(
		support.ChannelID(),
		flogging.MustGetLogger("etcdraft.puller").With("channel", support.ChannelID()),
		support,
		dialerConfig,
		clusterConfig,
		bccsp,
	)
	if err != nil {
		return nil, err
	}

	err = bpf.UpdateVerifierFromConfigBlock(lastConfigBlock)
	if err != nil {
		return nil, err
	}

	bf, err := bpf.BlockPuller(lastConfigBlock, make(chan struct{}))
	if err != nil {
		return nil, err
	}

	return &LedgerBlockPuller{
		Height:         support.Height,
		BlockRetriever: support,
		BlockPuller:    bf,
	}, nil
}
