/*
Copyright IBM Corp. 2022 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package replication

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/internal/pkg/identity"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

type BlockSourceOp int

const (
	ShuffleSource BlockSourceOp = iota
	CurrentSource
)

var logger = flogging.MustGetLogger("replication")

//go:generate mockery -dir . -name BlockSource -case underscore -output mocks/

type BlockSource interface {
	PullBlock(seq uint64) *common.Block
	HeightsByEndpoints() (map[string]uint64, error)
	UpdateEndpoints(endpoints []EndpointCriteria)
	Close()
}

type attestationErrorAndResponse struct {
	err  error
	resp *orderer.BlockAttestationResponse
}

//go:generate mockery -dir . -name AttestationSource -case underscore -output mocks/
type AttestationSource interface {
	PullAttestation(seq uint64) (*orderer.BlockAttestation, error)
	Close()
}

type suspectSet struct {
	entries []string
	max     int
}

func (s *suspectSet) insert(entry string) {
	if s.has(entry) {
		return
	}

	// evict the first entry if the set is full
	for len(s.entries) >= s.max {
		s.entries = s.entries[1:]
	}

	s.entries = append(s.entries, entry)
}

func (s suspectSet) has(entry string) bool {
	for _, e := range s.entries {
		if e == entry {
			return true
		}
	}
	return false
}

// AttestationPuller pulls attestation blocks from remote ordering nodes.
// Its operations are not thread safe.
type AttestationPuller struct {
	Logger *flogging.FabricLogger
	Signer identity.SignerSerializer
	Dialer Dialer
	Config FetcherConfig
	// Internal state
	stream *impatientAttestationStream
	lock   sync.Mutex
}

func (p *AttestationPuller) seekNextEnvelope(startSeq uint64) (*common.Envelope, error) {
	return protoutil.CreateSignedEnvelopeWithTLSBinding(
		common.HeaderType_DELIVER_SEEK_INFO,
		p.Config.Channel,
		p.Signer,
		nextSeekInfo(startSeq),
		int32(0),
		uint64(0),
		util.ComputeSHA256(p.Config.TLSCert),
	)
}

// Close closes the attestation puller connections.
func (p *AttestationPuller) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.stream != nil {
		p.stream.cancel()
		p.stream.CloseSend()
		p.stream = nil
	}
}

func (p *AttestationPuller) pullAttestationBlock(ec EndpointCriteria, env *common.Envelope) (*orderer.BlockAttestation, error) {
	conn, err := p.Dialer.Dial(ec)
	if err != nil {
		p.Logger.Errorf("Failed to Dial [%s]: [%v]", ec.Endpoint, err)
		return nil, err
	} else {
		defer conn.Close()
	}

	p.stream, err = newImpatientStream(conn, p.Config.FetchTimeout, env)
	if err != nil {
		p.Logger.Errorf("Failed to create stream: [%v]", err)
		return nil, err
	}

	defer p.stream.cancel()

	resp, err := p.stream.Recv()
	if err != nil {
		p.Logger.Warningf("Received %v from %s: %v", resp, ec.Endpoint, err)
		return nil, err
	}

	p.stream.CloseSend()
	return extractAttestationFromResponse(resp)
}

func (p *AttestationPuller) PullAttestation(seq uint64) (*orderer.BlockAttestation, error) {
	if len(p.Config.Endpoints) == 0 {
		p.Logger.Errorf("No endpoints set")
		return nil, errors.Errorf("no endpoints set")
	}

	ec := p.Config.Endpoints[0]

	p.Logger.Infof("Sending request for attestation block [%d] to [%s]", seq, ec.Endpoint)
	env, err := p.seekNextEnvelope(seq)
	if err != nil {
		p.Logger.Errorf("error creating envelope: %v", err)
		return nil, err
	}

	attestation, err := p.pullAttestationBlock(ec, env)
	if err != nil {
		return nil, err
	}

	if attestation.Header.Number != seq {
		return nil, errors.Errorf("received attestation for block %d instead of %d", attestation.Header.Number, seq)
	}

	return attestation, nil
}

func extractAttestationFromResponse(resp *orderer.BlockAttestationResponse) (*orderer.BlockAttestation, error) {
	if resp == nil {
		return nil, errors.Errorf("nil BlockAttestationResponse")
	}
	switch resp.Type.(type) {
	case *orderer.BlockAttestationResponse_BlockAttestation:
		attestation := resp.GetBlockAttestation()
		if attestation == nil {
			return nil, errors.New("attestation block is nil")
		}
		if attestation.Header == nil {
			return nil, errors.New("attestation block header is nil")
		}
		if attestation.Metadata == nil || len(attestation.Metadata.Metadata) == 0 {
			return nil, errors.New("attestation block metadata is empty")
		}
		return attestation, nil
	case *orderer.BlockAttestationResponse_Status:
		status := resp.GetStatus()
		return nil, errors.Errorf("did not receive a block, got status %v instead", status)
	default:
		return nil, errors.Errorf("response is of type %v, but expected a block", reflect.TypeOf(resp.Type))
	}
}

// impatientAttestationStream aborts the stream if it waits for too long for a message.
type impatientAttestationStream struct {
	waitTimeout time.Duration
	orderer.BlockAttestations_BlockAttestationsClient
	cancel func()
}

// Recv blocks until a response is received from the stream or the
// timeout expires.
func (stream *impatientAttestationStream) Recv() (*orderer.BlockAttestationResponse, error) {
	// Initialize a timeout to cancel the stream when it expires
	timeout := time.NewTimer(stream.waitTimeout)
	defer timeout.Stop()

	responseChan := make(chan attestationErrorAndResponse, 1)

	// receive waitGroup ensures the goroutine below exits before
	// this function exits.
	var receive sync.WaitGroup
	receive.Add(1)
	defer receive.Wait()

	go func() {
		defer receive.Done()
		resp, err := stream.BlockAttestations_BlockAttestationsClient.Recv()
		responseChan <- attestationErrorAndResponse{err: err, resp: resp}
	}()

	select {
	case <-timeout.C:
		stream.cancel()
		return nil, errors.Errorf("didn't receive a response within %v", stream.waitTimeout)
	case respAndErr := <-responseChan:
		return respAndErr.resp, respAndErr.err
	}
}

// newImpatientStream returns a ImpatientStreamCreator that creates impatientStreams.
func newImpatientStream(conn *grpc.ClientConn, waitTimeout time.Duration, env *common.Envelope) (*impatientAttestationStream, error) {
	abc := orderer.NewBlockAttestationsClient(conn)
	ctx, cancel := context.WithCancel(context.Background())

	stream, err := abc.BlockAttestations(ctx, env)
	if err != nil {
		cancel()
		return nil, err
	}

	once := &sync.Once{}
	return &impatientAttestationStream{
		waitTimeout: waitTimeout,
		// The stream might be canceled while Close() is being called, but also
		// while a timeout expires, so ensure it's only called once.
		cancel: func() {
			once.Do(cancel)
		},
		BlockAttestations_BlockAttestationsClient: stream,
	}, nil
}

// FetcherConfig stores the configuration parameters needed to create a BlockFetcher
type FetcherConfig struct {
	Channel                      string
	TLSCert                      []byte
	Endpoints                    []EndpointCriteria
	FetchTimeout                 time.Duration
	RetryTimeout                 time.Duration
	CensorshipSuspicionThreshold time.Duration
	PeriodicalShuffleInterval    time.Duration
	MaxRetries                   uint64
	MaxByzantineNodes            int
	BufferSize                   int
}

// UpdateFetcherConfigFromConfigBlock updates the endpoints in fetcherconfig from a config block.
// if it is unable to fetch endpoints from a config block, it doesn't udate the endpoints in FetcherConfig.
// the error returned may be logged.
func UpdateFetcherConfigFromConfigBlock(c FetcherConfig, latestConfig channelconfig.Resources) (FetcherConfig, error) {
	fetcherConfig := c
	endpoints, err := EndpointconfigFromConfig(latestConfig)

	if err == nil {
		// update endpoints from config block
		fetcherConfig.Endpoints = endpoints
	}
	return fetcherConfig, err
}

type TimeFunc func() time.Time

// BlockFetcher can be used to fetch blocks from orderers in a byzantine fault tolerant way.
type BlockFetcher struct {
	// Configuration
	FetcherConfig
	LastConfig               channelconfig.Resources
	ConfigSequence           uint64
	BlockVerifierFactory     func(config channelconfig.Resources) protoutil.BlockVerifierFunc
	VerifyBlock              protoutil.BlockVerifierFunc
	AttestationSourceFactory func(fc FetcherConfig, latestConfig channelconfig.Resources) (AttestationSource, error)
	BlockSourceFactory       func(fc FetcherConfig, latestConfig channelconfig.Resources) (BlockSource, error)
	Logger                   *flogging.FabricLogger
	TimeNow                  TimeFunc
	Signer                   identity.SignerSerializer
	Dialer                   Dialer
	bccsp                    bccsp.BCCSP
	// State
	currentEndpoint         EndpointCriteria
	lastShuffledAt          time.Time
	setupExecuted           bool
	blockSourceOp           BlockSourceOp
	shuffleIndex            int
	currentBlockSource      BlockSource
	suspects                suspectSet // a set of bft suspected nodes.
	setupVerifierInProgress bool
}

func NewBlockFetcher(fc FetcherConfig, signer identity.SignerSerializer, bccsp bccsp.BCCSP, lastConfig channelconfig.Resources, dialerConfig comm.ClientConfig, verifyBlockSequence func(blocks []*common.Block, _ string) error, stopChannel chan struct{}) (*BlockFetcher, error) {
	// Extract the TLS CA certs and endpoints from the configuration,
	// bundle, err := BundleFromConfigBlock(lastConfigBlock, bccsp)
	// if err != nil {
	// 	return nil, err
	// }
	var err error
	fc.Endpoints, err = EndpointconfigFromConfig(lastConfig)
	if err != nil {
		return nil, err
	}

	der, _ := pem.Decode(dialerConfig.SecOpts.Certificate)
	if der == nil {
		return nil, errors.Errorf("client certificate isn't in PEM format: %v",
			string(dialerConfig.SecOpts.Certificate))
	}
	fc.TLSCert = der.Bytes

	stdDialer := &StandardDialer{
		Config: dialerConfig,
	}
	stdDialer.Config.AsyncConnect = false
	stdDialer.Config.SecOpts.VerifyCertificate = nil

	bf_logger := flogging.MustGetLogger("common.replication.puller").With("channel", fc.Channel)

	// modify the verifier builder with middleware to skip verifying the genesis block
	// in the follower chain
	modifiedVerifierBuilder := func(blockVerifierFunction protoutil.BlockVerifierFunc) protoutil.BlockVerifierFunc {
		return protoutil.BlockVerifierFunc(
			func(header *common.BlockHeader, metadata *common.BlockMetadata) error {
				// don't verify the genesis block
				if header.Number == 0 {
					bf_logger.Debugf("Not verfying genesis block for follower chain")
					return nil
				}

				return blockVerifierFunction(header, metadata)
			})
	}

	// wrap the verifier factory to stop verifying genesis block
	verifierFactory := func(config channelconfig.Resources) protoutil.BlockVerifierFunc {
		vf := BlockVerifierBuilder()
		return modifiedVerifierBuilder(vf(config))
	}

	// To tolerate byzantine behaviour of `f` faulty nodes, we need atleast of `3f + 1` nodes.
	// check for bft enable and update `MaxByzantineNodes`
	// accordingly.
	bftEnabled, f, err := bftEnabledInConfig(lastConfig)
	if err != nil {
		return nil, err
	}

	var verifyBlockFunc protoutil.BlockVerifierFunc
	if bftEnabled && f > 0 {
		fc.MaxByzantineNodes = f
		// setting verifyBlockFunc, will make BlockFetcher pull genesis block in a bft setting and
		// intialize the VerifyBlock function
		verifyBlockFunc = nil
	} else {
		verifyBlockFunc = verifierFactory(lastConfig)
	}

	bf := &BlockFetcher{
		FetcherConfig:        fc,
		LastConfig:           lastConfig,
		BlockVerifierFactory: verifierFactory,
		VerifyBlock:          verifyBlockFunc,
		AttestationSourceFactory: func(c FetcherConfig, latestConfig channelconfig.Resources) (AttestationSource, error) {
			fc, err := UpdateFetcherConfigFromConfigBlock(c, latestConfig)
			if err != nil {
				bf_logger.Errorf("Could not update FetcherConfig fom Config Block: %v", err)
			}
			return &AttestationPuller{
				Config: fc,
				Signer: signer,
				Dialer: stdDialer,
				Logger: flogging.MustGetLogger("orderer.common.cluster.attestationpuller").With("channel", fc.Channel),
			}, err
		},
		BlockSourceFactory: func(c FetcherConfig, latestConfig channelconfig.Resources) (BlockSource, error) {
			// update FetcherConfig from latestConfigBlock
			fc, err := UpdateFetcherConfigFromConfigBlock(c, latestConfig)
			if err != nil {
				bf_logger.Errorf("Could not update FetcherConfig fom Config Block: %v", err)
			}
			return &BlockPuller{
				VerifyBlockSequence: verifyBlockSequence,
				Logger:              flogging.MustGetLogger("orderer.common.cluster.puller").With("channel", fc.Channel),
				RetryTimeout:        fc.RetryTimeout,
				MaxTotalBufferBytes: fc.BufferSize,
				FetchTimeout:        fc.FetchTimeout,
				Endpoints:           fc.Endpoints,
				Signer:              signer,
				TLSCert:             fc.TLSCert,
				Channel:             fc.Channel,
				Dialer:              stdDialer,
				StopChannel:         stopChannel,
			}, err
		},
		Logger:  bf_logger,
		Signer:  signer,
		Dialer:  stdDialer,
		TimeNow: time.Now,
		bccsp:   bccsp,
	}
	return bf, nil
}

func (bf *BlockFetcher) getBlockSource() (BlockSource, error) {
	blockSourceOp := bf.blockSourceOp
	// reset blocksource
	bf.blockSourceOp = CurrentSource

	switch blockSourceOp {
	case CurrentSource:
		return bf.currentBlockSource, nil
	case ShuffleSource:
		prevEndpoint := bf.currentEndpoint
		err := bf.shuffleEndpoint()
		if err != nil {
			return nil, err
		}
		newEndpoint := bf.currentEndpoint
		if prevEndpoint.Endpoint == "" {
			bf.Logger.Debugf("Picked an endpoint to pull blocks from: %s", newEndpoint)
		} else {
			bf.Logger.Debugf("Shuffled endpoint: %s --> %s", prevEndpoint, newEndpoint)
		}
		return bf.currentBlockSource, nil
	}
	panic(fmt.Sprintf("invalid block source option: %v", bf.blockSourceOp))
}

func (bf *BlockFetcher) setBlockSource(ec EndpointCriteria) error {
	bf.Logger.Debugf("Set [%s] as block source", ec.Endpoint)
	if bf.currentBlockSource != nil {
		bf.currentBlockSource.Close()
	}
	config := bf.FetcherConfig
	config.Endpoints = []EndpointCriteria{ec}
	blockSource, err := bf.BlockSourceFactory(config, bf.LastConfig)
	bf.currentBlockSource = blockSource
	return err
}

func (bf *BlockFetcher) maybeUpdateLatestConfigBlock(block *common.Block) {
	if !protoutil.IsConfigBlock(block) {
		return
	}

	bundle, err := BundleFromConfigBlock(block, bf.bccsp)
	if err != nil {
		bf.VerifyBlock = createErrorFunc(err)
	}

	seq := block.Header.Number
	bf.Logger.Infof("Block %d contains a config transaction, updating block verification reference", seq)
	bf.VerifyBlock = bf.BlockVerifierFactory(bundle)

	if bf.ConfigSequence < seq {
		bf.Logger.Infof("Config block %d received is later than last config block %d, updating its reference", seq, bf.ConfigSequence)
		bf.ConfigSequence = seq
		bf.LastConfig = bundle
		endpoints, err := EndpointconfigFromConfig(bundle)
		if err != nil {
			bf.Logger.Errorf("Failed parsing orderer endpoints from block %d: %v", block.Header.Number, err)
			return
		}
		bf.UpdateEndpoints(endpoints)
	}
}

func (bf *BlockFetcher) shuffleEndpoint() error {
	candidates := bf.blockSourceCandidates()
	// handle case when candidates list is empty
	if len(candidates) == 0 {
		bf.Logger.Info("Can't shuffle blockpuller endpoint. Not enough endpoints available")
		return errors.New("can't shuffle blockpuller endpoint. Not enough endpoints available")
	}
	bf.shuffleIndex++
	effectiveIndex := bf.shuffleIndex % len(candidates)
	bf.currentEndpoint = candidates[effectiveIndex]
	bf.Logger.Debugf("Shuffled block puller endpoint. New endpoint: [%s]", bf.currentEndpoint)
	err := bf.setBlockSource(bf.currentEndpoint)
	bf.lastShuffledAt = bf.TimeNow()
	return err
}

func (bf *BlockFetcher) blockSourceCandidates() []EndpointCriteria {
	var candidates []EndpointCriteria
	for _, e := range bf.Endpoints {
		// If candidate is suspected to be censoring,
		// or if it's the previous endpoint, don't pick it.
		if bf.suspects.has(e.Endpoint) || e.Endpoint == bf.currentEndpoint.Endpoint {
			continue
		}
		candidates = append(candidates, e)
	}
	return candidates
}

func (bf *BlockFetcher) probeForAttestation(seq uint64) (bool, error) {
	bf.Logger.Infof("Checking whether %s is withholding blocks", bf.currentEndpoint)
	candidates := bf.blockSourceCandidates()
	return bf.pullAndVerifyAttestations(seq, candidates)
}

func (bf *BlockFetcher) pullAndVerifyAttestations(seq uint64, candidates []EndpointCriteria) (bool, error) {
	var wg sync.WaitGroup
	wg.Add(len(candidates))

	var lock sync.Mutex
	var foundAttestation bool
	var sourceError error
	var attestationSources []AttestationSource

	for i := 0; i < len(candidates); i++ {
		go func(candidate EndpointCriteria) {
			defer wg.Done()
			config := bf.FetcherConfig

			config.Endpoints = []EndpointCriteria{candidate}

			attestationSource, err := bf.AttestationSourceFactory(config, bf.LastConfig)
			if err != nil {
				bf.Logger.Errorf("Failed to create attestation source: %v", err)
				lock.Lock()
				// update sourceError only if not already updated.
				if sourceError == nil {
					sourceError = err
				}
				lock.Unlock()
				return
			}
			defer attestationSource.Close()

			lock.Lock()
			attestationSources = append(attestationSources, attestationSource)
			lock.Unlock()

			attestation, err := attestationSource.PullAttestation(seq)
			if err != nil || attestation == nil {
				lock.Lock()
				bf.Logger.Debugf("Failed pulling block attestation for %d from %s: %v", seq, candidate.Endpoint, err)
				if !foundAttestation {
					bf.Logger.Warnf("Failed pulling block attestation for %d from %s: %v", seq, candidate.Endpoint, err)
				}
				lock.Unlock()
				return
			}

			if err := bf.VerifyBlock(attestation.Header, attestation.Metadata); err != nil {
				bf.Logger.Warnf("Got invalid attestation on %d from %s: %v", seq, candidate.Endpoint, err)
				return
			}

			bf.Logger.Infof("Received a valid attestation on %d from %s", seq, candidate.Endpoint)

			lock.Lock()
			defer lock.Unlock()

			foundAttestation = true
			// Close all attestation sources that may be in progress of pulling blocks,
			// as we already know about a valid attestation.
			for _, source := range attestationSources {
				source.Close()
			}
		}(candidates[i])
	}

	wg.Wait()
	return foundAttestation, sourceError
}

func (bf *BlockFetcher) setup() {
	if bf.setupExecuted {
		return
	}

	bf.Logger.Infof("Setting up BlockFetcher: %+v, ", bf.FetcherConfig)
	defer func() {
		bf.setupExecuted = true
	}()

	bf.suspects = suspectSet{max: bf.FetcherConfig.MaxByzantineNodes}

	/* If the `VerifyBlock` is not set, BlockFetcher tries to pull genesis block and intialize the `VerifyBlock` function
	based on it. It will try to pull genesis block(block number 0) till it suceeds.*/

	if bf.VerifyBlock == nil {
		bf.Logger.Debugf("VerifyBlock Function is not set.")
		bf.setupVerifierInProgress = true
		for bf.setupVerifierInProgress {
			// keep polling till we get a genesis block.
			ok := bf.setUpVerifierFromGenesisBlock()
			bf.Logger.Debugf("Try to pull genesis block %v", ok)
			if ok {
				bf.Logger.Debugf("Successfully pulled a genesis block.")
				bf.setupVerifierInProgress = false
			}
		}
	}
}

// setUpVerifierFromGenesisBlock connects to orderers and tries to pull genesis block from f+1 distinct
// orderers and sets up verifier
func (bf *BlockFetcher) setUpVerifierFromGenesisBlock() bool {
	candidates := bf.blockSourceCandidates()

	var wg sync.WaitGroup
	wg.Add(len(candidates))
	var lock sync.Mutex
	var blocks []*common.Block

	for i := 0; i < len(candidates); i++ {
		go func(candidate EndpointCriteria) {
			defer wg.Done()
			config := bf.FetcherConfig
			config.Endpoints = []EndpointCriteria{candidate}
			blockSource, err := bf.BlockSourceFactory(config, bf.LastConfig)
			if err != nil {
				bf.Logger.Errorf("Failed to create block source: %v", err)
				return
			}
			defer blockSource.Close()

			block := blockSource.PullBlock(0)
			// verify data hash on the block
			if !bytes.Equal(protoutil.BlockDataHash(block.Data), block.Header.DataHash) {
				bf.Logger.Warnf("Block data hash mismatch on block %d", block.Header.Number)
				return
			}
			lock.Lock()
			blocks = append(blocks, block)
			lock.Unlock()
		}(candidates[i])
	}
	wg.Wait()

	histogram := make(map[string]int) // maps block hashes to number of nodes that returned them.
	var genesisBlock *common.Block
	// Search for a candidate with f+1 votes
	for _, genesisBlockCandidate := range blocks {
		currentBlockHash := hex.EncodeToString(protoutil.BlockHeaderHash(genesisBlockCandidate.Header))
		histogram[currentBlockHash]++
		votes := histogram[currentBlockHash]
		if votes >= bf.MaxByzantineNodes+1 && genesisBlock == nil {
			bf.Logger.Debugf("%d votes for block with hash %s, genesis block found", votes, currentBlockHash)
			genesisBlock = genesisBlockCandidate
			break
		}
	}

	if genesisBlock == nil {
		bf.Logger.Errorf("Failed to get at least %d votes for any genesis block among %v", bf.MaxByzantineNodes+1, candidates)
		return false
	}

	// Setup BlockVerifierFactory and LastConfigBlock from genesisBlock
	bundle, err := BundleFromConfigBlock(genesisBlock, bf.bccsp)
	if err != nil {
		bf.VerifyBlock = createErrorFunc(err)
	} else {
		bf.VerifyBlock = bf.BlockVerifierFactory(bundle)
	}
	if bf.LastConfig == nil {
		bf.LastConfig = bundle
	}
	return true
}

// PullBlock pulls blocks from orderers in spite of block censorship.
func (bf *BlockFetcher) PullBlock(seq uint64) *common.Block {
	bf.setup()

	retriesLeft := bf.MaxRetries

	for {
		if retriesLeft == 0 && bf.MaxRetries > 0 {
			bf.Logger.Errorf("Failed pulling block [%d]: retry count exhausted(%d)", seq, bf.MaxRetries)
			return nil
		}

		blkSource, err := bf.getBlockSource()
		if err != nil {
			bf.Logger.Errorf("Failed setting block source: %v", err)
			return nil
		}

		startedPulling := bf.TimeNow()
		timeSinceLastShuffle := startedPulling.Sub(bf.lastShuffledAt)
		if startedPulling.After(bf.lastShuffledAt.Add(bf.PeriodicalShuffleInterval)) {
			bf.Logger.Infof("Last shuffle was %v ago, pull time limit (%v) for %s expired, will shuffle and connect to a different orderer node",
				timeSinceLastShuffle, bf.PeriodicalShuffleInterval, bf.currentEndpoint.Endpoint)
			bf.blockSourceOp = ShuffleSource
			continue
		}
		block := blkSource.PullBlock(seq)

		elapsed := bf.TimeNow().Sub(startedPulling)

		if block != nil {
			bf.Logger.Debugf("Got block %d from %s", block.Header.Number, bf.currentEndpoint.Endpoint)
			if err := bf.VerifyBlock(block.Header, block.Metadata); err != nil {
				bf.Logger.Warnf("Failed verifying signature on block %d received from %s: %v", block.Header.Number, bf.currentEndpoint.Endpoint, err)
				return nil
			}
			if !bytes.Equal(protoutil.BlockDataHash(block.Data), block.Header.DataHash) {
				bf.Logger.Warnf("Block data hash mismatch on block %d", block.Header.Number)
			}

			bf.maybeUpdateLatestConfigBlock(block)

			return block
		}

		if bf.MaxByzantineNodes > 0 && elapsed > bf.CensorshipSuspicionThreshold {
			bf.Logger.Warnf("Did not receive block %d from %s for %v, suspecting it is withholding blocks", seq, bf.currentEndpoint, elapsed)
			blockwithheld, err := bf.probeForAttestation(seq)
			if err != nil {
				bf.Logger.Errorf("Failed to probe for attestations: %v", err)
				return nil
			}
			if blockwithheld {
				bf.Logger.Warnf("Detected withholding of block %d by %s", seq, bf.currentEndpoint.Endpoint)
				bf.suspects.insert(bf.currentEndpoint.Endpoint)
				bf.blockSourceOp = ShuffleSource
			}
		}

		retriesLeft--
	}
}

// HeightsByEndpoints returns the block heights by endpoints of orderers
func (bf BlockFetcher) HeightsByEndpoints() (map[string]uint64, error) {
	bs, err := bf.BlockSourceFactory(bf.FetcherConfig, bf.LastConfig)
	if err != nil {
		return nil, err
	}
	defer bs.Close()
	return bs.HeightsByEndpoints()
}

// UpdateEndpoints assigns the new endpoints.
func (p *BlockFetcher) UpdateEndpoints(endpoints []EndpointCriteria) {
	p.Logger.Debugf("Updating endpoints: %v", endpoints)
	p.FetcherConfig.Endpoints = endpoints
	p.currentBlockSource.UpdateEndpoints(endpoints)
}

// Close closes the blocksource of blockfetcher.
func (bf BlockFetcher) Close() {
	if bf.currentBlockSource != nil {
		bf.currentBlockSource.Close()
	}
}
