/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package replication

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
)

// checkResources makes sure that the channel config is compatible with this binary and logs sanity checks
func CheckResources(res channelconfig.Resources) error {
	channelconfig.LogSanityChecks(res)
	oc, ok := res.OrdererConfig()
	if !ok {
		return errors.New("config does not contain orderer config")
	}
	if err := oc.Capabilities().Supported(); err != nil {
		return errors.WithMessagef(err, "config requires unsupported orderer capabilities: %s", err)
	}
	if err := res.ChannelConfig().Capabilities().Supported(); err != nil {
		return errors.WithMessagef(err, "config requires unsupported channel capabilities: %s", err)
	}
	return nil
}

// checkResourcesOrPanic invokes checkResources and panics if an error is returned
func checkResourcesOrPanic(res channelconfig.Resources) {
	if err := CheckResources(res); err != nil {
		logger.Panicf("[channel %s] %s", res.ConfigtxValidator().ChannelID(), err)
	}
}

type MutableResources interface {
	channelconfig.Resources
	Update(*channelconfig.Bundle)
}

type configResources struct {
	MutableResources
	bccsp bccsp.BCCSP
}

func (cr *configResources) CreateBundle(channelID string, config *common.Config) (*channelconfig.Bundle, error) {
	return channelconfig.NewBundle(channelID, config, cr.bccsp)
}

func (cr *configResources) Update(bndl *channelconfig.Bundle) {
	checkResourcesOrPanic(bndl)
	cr.MutableResources.Update(bndl)
}

func (cr *configResources) SharedConfig() channelconfig.Orderer {
	oc, ok := cr.OrdererConfig()
	if !ok {
		logger.Panicf("[channel %s] has no orderer configuration", cr.ConfigtxValidator().ChannelID())
	}
	return oc
}

type LedgerResources struct {
	*configResources
	blockledger.ReadWriter
}

func NewLedgerResources(resources MutableResources, bccsp bccsp.BCCSP, readWriter blockledger.ReadWriter) *LedgerResources {
	return &LedgerResources{
		configResources: &configResources{
			MutableResources: resources,
			bccsp:            bccsp,
		},
		ReadWriter: readWriter,
	}
}

// ChannelID passes through to the underlying configtx.Validator
func (lr *LedgerResources) ChannelID() string {
	return lr.ConfigtxValidator().ChannelID()
}

// VerifyBlockSignature verifies a signature of a block.
// It has an optional argument of a configuration envelope which would make the block verification to use validation
// rules based on the given configuration in the ConfigEnvelope. If the config envelope passed is nil, then the
// validation rules used are the ones that were applied at commit of previous blocks.
func (lr *LedgerResources) VerifyBlockSignature(sd []*protoutil.SignedData, envelope *common.ConfigEnvelope) error {
	policyMgr := lr.PolicyManager()
	// If the envelope passed isn't nil, we should use a different policy manager.
	if envelope != nil {
		bundle, err := channelconfig.NewBundle(lr.ChannelID(), envelope.Config, lr.bccsp)
		if err != nil {
			return err
		}
		policyMgr = bundle.PolicyManager()
	}
	policy, exists := policyMgr.GetPolicy(policies.BlockValidation)
	if !exists {
		return errors.Errorf("policy %s wasn't found", policies.BlockValidation)
	}
	err := policy.EvaluateSignedData(sd)
	if err != nil {
		return errors.WithMessage(err, "block verification failed")
	}
	return nil
}

// Block returns a block with the following number, or nil if such a block doesn't exist.
func (lr *LedgerResources) Block(number uint64) *common.Block {
	if lr.Height() <= number {
		return nil
	}
	return blockledger.GetBlock(lr, number)
}
