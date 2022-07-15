/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package follower

import (
	"encoding/pem"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/orderer/common/cluster"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/pkg/errors"
)

// TODO: keep these constants at one place tp
// to prevent code duplication. These would be needed in the
// consensus blockpuller.
const (
	// compute endpoint shuffle timeout from FetchTimeout
	// endpoint shuffle timeout should be shuffleTimeoutMultiplier times FetchTimeout
	shuffleTimeoutMultiplier = 10
	// timeout expressed as a percentage of FetchtimeOut
	// if PullBlock returns before (shuffleTimeoutPercentage% of FetchTimeOut of blockfetcher)
	// the source is shuffled.
	shuffleTimeoutPercentage = int64(50)
)

//go:generate counterfeiter -o mocks/channel_puller.go -fake-name ChannelPuller . ChannelPuller

// ChannelPuller pulls blocks for a channel
type ChannelPuller interface {
	PullBlock(seq uint64) *common.Block
	HeightsByEndpoints() (map[string]uint64, error)
	UpdateEndpoints(endpoints []cluster.EndpointCriteria)
	Close()
}

// BlockPullerCreator creates a ChannelPuller on demand.
// It also maintains a link to a block signature verifier, and exposes a method to update it on incoming config blocks.
// The ChannelPuller generated by this factory always accesses the updated verifier, since it is generated
// with a link to the factory's VerifyBlockSequence method.
type BlockPullerCreator struct {
	channelID               string
	bccsp                   bccsp.BCCSP
	blockSigVerifierFactory cluster.VerifierFactory // Creates a new block signature verifier
	blockSigVerifier        cluster.BlockVerifier   // The current block signature verifier, from the latest channel config
	clusterConfig           localconfig.Cluster
	signer                  identity.SignerSerializer
	der                     *pem.Block
	stdDialer               *cluster.StandardDialer
	ClusterVerifyBlocks     ClusterVerifyBlocksFunc // Default: cluster.VerifyBlocks, or a mock for testing
}

// ClusterVerifyBlocksFunc is a function that matches the signature of cluster.VerifyBlocks, and allows mocks for testing.
type ClusterVerifyBlocksFunc func(blockBuff []*common.Block, signatureVerifier cluster.BlockVerifier) error

// NewBlockPullerCreator creates a new BlockPullerCreator, using the configuration details that do not change during
// the life cycle of the orderer.
func NewBlockPullerCreator(
	channelID string,
	logger *flogging.FabricLogger,
	signer identity.SignerSerializer,
	baseDialer *cluster.PredicateDialer,
	clusterConfig localconfig.Cluster,
	bccsp bccsp.BCCSP,
) (*BlockPullerCreator, error) {
	stdDialer := &cluster.StandardDialer{
		Config: baseDialer.Config,
	}
	stdDialer.Config.AsyncConnect = false
	stdDialer.Config.SecOpts.VerifyCertificate = nil

	der, _ := pem.Decode(stdDialer.Config.SecOpts.Certificate)
	if der == nil {
		return nil, errors.Errorf("client certificate isn't in PEM format: %v",
			string(stdDialer.Config.SecOpts.Certificate))
	}

	factory := &BlockPullerCreator{
		channelID: channelID,
		bccsp:     bccsp,
		blockSigVerifierFactory: &cluster.BlockVerifierAssembler{
			Logger: logger,
			BCCSP:  bccsp,
		},
		clusterConfig:       clusterConfig,
		signer:              signer,
		stdDialer:           stdDialer,
		der:                 der,
		ClusterVerifyBlocks: cluster.VerifyBlocks, // The default block sequence verification method.
	}

	return factory, nil
}

// BlockPuller creates a block puller on demand, taking the endpoints from the config block.
func (creator *BlockPullerCreator) BlockPuller(configBlock *common.Block, stopChannel chan struct{}) (ChannelPuller, error) {
	// Extract the TLS CA certs and endpoints from the join-block
	endpoints, err := cluster.EndpointconfigFromConfigBlock(configBlock, creator.bccsp)
	if err != nil {
		return nil, errors.WithMessage(err, "error extracting endpoints from config block")
	}

	bp := &cluster.BlockPuller{
		VerifyBlockSequence: creator.VerifyBlockSequence,
		Logger:              flogging.MustGetLogger("orderer.common.cluster.puller").With("channel", creator.channelID),
		RetryTimeout:        creator.clusterConfig.ReplicationRetryTimeout,
		MaxTotalBufferBytes: creator.clusterConfig.ReplicationBufferSize,
		MaxPullBlockRetries: uint64(creator.clusterConfig.ReplicationMaxRetries),
		FetchTimeout:        creator.clusterConfig.ReplicationPullTimeout,
		Endpoints:           endpoints,
		Signer:              creator.signer,
		TLSCert:             creator.der.Bytes,
		Channel:             creator.channelID,
		Dialer:              creator.stdDialer,
		StopChannel:         stopChannel,
	}

	return bp, nil
}

// BlockFetcher creates a block fetcher on demand, taking the endpoints from the config block.
func (creator *BlockPullerCreator) BlockFetcher(configBlock *common.Block, stopChannel chan struct{}) (ChannelPuller, error) {
	// Extract the TLS CA certs and endpoints from the join-block
	endpoints, err := cluster.EndpointconfigFromConfigBlock(configBlock, creator.bccsp)
	if err != nil {
		return nil, errors.WithMessage(err, "error extracting endpoints from config block")
	}

	fc := cluster.FetcherConfig{
		Channel:                      creator.channelID,
		TLSCert:                      creator.der.Bytes,
		Endpoints:                    endpoints,
		FetchTimeout:                 creator.clusterConfig.ReplicationPullTimeout,
		CensorshipSuspicionThreshold: time.Duration((int64(creator.clusterConfig.ReplicationPullTimeout) * shuffleTimeoutPercentage / 100)),
		PeriodicalShuffleInterval:    shuffleTimeoutMultiplier * creator.clusterConfig.ReplicationPullTimeout,
		MaxRetries:                   uint64(creator.clusterConfig.ReplicationMaxRetries),
	}

	// To tolerate byzantine behaviour of `f` faulty nodes, we need atleast of `3f + 1` nodes.
	// check for bft enable and update `MaxByzantineNodes`
	// accordingly.
	bftEnabled, f, err := cluster.BFTEnabledInConfig(configBlock, creator.bccsp)
	if err != nil {
		return nil, err
	}

	if bftEnabled && f > 0 {
		fc.MaxByzantineNodes = f
	}

	verifierFactory := cluster.BlockVerifierBuilder(creator.bccsp)
	bf_logger := flogging.MustGetLogger("orderer.common.cluster.puller").With("channel", creator.channelID)

	bf := &cluster.BlockFetcher{
		FetcherConfig:        fc,
		LastConfigBlock:      configBlock,
		BlockVerifierFactory: verifierFactory,
		VerifyBlock:          verifierFactory(configBlock),
		AttestationSourceFactory: func(c cluster.FetcherConfig, latestConfigBlock *common.Block) (cluster.AttestationSource, error) {
			fc, err := cluster.UpdateFetcherConfigFromConfigBlock(c, latestConfigBlock)
			bf_logger.Errorf("Could not update FetcherConfig fom Config Block: %v", err)
			return &cluster.AttestationPuller{
				Logger: flogging.MustGetLogger("orderer.common.cluster.attestationpuller").With("channel", creator.channelID),
				Signer: creator.signer,
				Dialer: creator.stdDialer,
				Config: fc,
			}, err
		},
		BlockSourceFactory: func(c cluster.FetcherConfig, latestConfigBlock *common.Block) (cluster.BlockSource, error) {
			fc, err := cluster.UpdateFetcherConfigFromConfigBlock(c, latestConfigBlock)
			bf_logger.Errorf("Could not update FetcherConfig from Config Block: %v", err)
			return &cluster.BlockPuller{
				MaxPullBlockRetries: uint64(creator.clusterConfig.ReplicationMaxRetries),
				MaxTotalBufferBytes: creator.clusterConfig.ReplicationBufferSize,
				Signer:              creator.signer,
				TLSCert:             creator.der.Bytes,
				Channel:             fc.Channel,
				FetchTimeout:        creator.clusterConfig.ReplicationPullTimeout,
				RetryTimeout:        creator.clusterConfig.ReplicationRetryTimeout,
				Logger:              flogging.MustGetLogger("orderer.common.cluster.puller").With("channel", fc.Channel),
				Dialer:              creator.stdDialer,
				VerifyBlockSequence: creator.VerifyBlockSequence,
				Endpoints:           fc.Endpoints,
				StopChannel:         stopChannel,
			}, err
		},
		Logger:  bf_logger,
		TimeNow: time.Now,
		Signer:  creator.signer,
		Dialer:  creator.stdDialer,
	}

	return bf, nil
}

// UpdateVerifierFromConfigBlock creates a new block signature verifier from the config block and updates the internal
// link to said verifier.
func (creator *BlockPullerCreator) UpdateVerifierFromConfigBlock(configBlock *common.Block) error {
	configEnv, err := cluster.ConfigFromBlock(configBlock)
	if err != nil {
		return errors.WithMessage(err, "failed to extract config envelope from block")
	}
	verifier, err := creator.blockSigVerifierFactory.VerifierFromConfig(configEnv, creator.channelID)
	if err != nil {
		return errors.WithMessage(err, "failed to construct a block signature verifier from config envelope")
	}
	creator.blockSigVerifier = verifier
	return nil
}

// VerifyBlockSequence verifies a sequence of blocks, using the internal block signature verifier. It also bootstraps
// the block sig verifier form the genesis block if it does not exist, and skips verifying the genesis block.
func (creator *BlockPullerCreator) VerifyBlockSequence(blocks []*common.Block, _ string) error {
	if len(blocks) == 0 {
		return errors.New("buffer is empty")
	}
	if blocks[0] == nil {
		return errors.New("first block is nil")
	}
	if blocks[0].Header == nil {
		return errors.New("first block header is nil")
	}
	if blocks[0].Header.Number == 0 {
		configEnv, err := cluster.ConfigFromBlock(blocks[0])
		if err != nil {
			return errors.WithMessage(err, "failed to extract config envelope from genesis block")
		}
		// Bootstrap the verifier from the genesis block, as it will be used to verify
		// the subsequent blocks in the batch.
		creator.blockSigVerifier, err = creator.blockSigVerifierFactory.VerifierFromConfig(configEnv, creator.channelID)
		if err != nil {
			return errors.WithMessage(err, "failed to construct a block signature verifier from genesis block")
		}
		blocksAfterGenesis := blocks[1:]
		if len(blocksAfterGenesis) == 0 {
			return nil
		}
		return creator.ClusterVerifyBlocks(blocksAfterGenesis, creator.blockSigVerifier)
	}

	if creator.blockSigVerifier == nil {
		return errors.New("nil block signature verifier")
	}

	return creator.ClusterVerifyBlocks(blocks, creator.blockSigVerifier)
}
