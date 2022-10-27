/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package replication

import (
	"bytes"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-config/protolator"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/internal/pkg/comm"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// StandardDialer wraps an ClientConfig, and provides
// a means to connect according to given EndpointCriteria.
type StandardDialer struct {
	Config comm.ClientConfig
}

// Dial dials an address according to the given EndpointCriteria
func (dialer *StandardDialer) Dial(endpointCriteria EndpointCriteria) (*grpc.ClientConn, error) {
	clientConfigCopy := dialer.Config
	clientConfigCopy.SecOpts.ServerRootCAs = endpointCriteria.TLSRootCAs

	return clientConfigCopy.Dial(endpointCriteria.Endpoint)
}

// BlockSequenceVerifier verifies that the given consecutive sequence
// of blocks is valid.
type BlockSequenceVerifier func(blocks []*common.Block, channel string) error

// Dialer creates a gRPC connection to a remote address
type Dialer interface {
	Dial(endpointCriteria EndpointCriteria) (*grpc.ClientConn, error)
}

//go:generate mockery -dir . -name BlockVerifier -case underscore -output ./mocks/

// BlockVerifier verifies block signatures.
type BlockVerifier interface {
	// VerifyBlockSignature verifies a signature of a block.
	// It has an optional argument of a configuration envelope
	// which would make the block verification to use validation rules
	// based on the given configuration in the ConfigEnvelope.
	// If the config envelope passed is nil, then the validation rules used
	// are the ones that were applied at commit of previous blocks.
	VerifyBlockSignature(sd []*protoutil.SignedData, config *common.ConfigEnvelope) error
}

//go:generate mockery -dir . -name VerifierFactory -case underscore -output ./mocks/

// VerifierFactory creates BlockVerifiers.
type VerifierFactory interface {
	// VerifierFromConfig creates a BlockVerifier from the given configuration.
	VerifierFromConfig(configuration *common.ConfigEnvelope, channel string) (BlockVerifier, error)
}

// VerifyBlocks verifies the given consecutive sequence of blocks is valid,
// and returns nil if it's valid, else an error.
func VerifyBlocks(blockBuff []*common.Block, signatureVerifier BlockVerifier) error {
	if len(blockBuff) == 0 {
		return errors.New("buffer is empty")
	}
	// First, we verify that the block hash in every block is:
	// Equal to the hash in the header
	// Equal to the previous hash in the succeeding block
	for i := range blockBuff {
		if err := VerifyBlockHash(i, blockBuff); err != nil {
			return err
		}
	}

	var config *common.ConfigEnvelope
	var isLastBlockConfigBlock bool
	// Verify all configuration blocks that are found inside the block batch,
	// with the configuration that was committed (nil) or with one that is picked up
	// during iteration over the block batch.
	for _, block := range blockBuff {
		configFromBlock, err := ConfigFromBlock(block)
		if err == ErrNotAConfig {
			isLastBlockConfigBlock = false
			continue
		}
		if err != nil {
			return err
		}
		// The block is a configuration block, so verify it
		if err := VerifyBlockSignature(block, signatureVerifier, config); err != nil {
			return err
		}
		config = configFromBlock
		isLastBlockConfigBlock = true
	}

	// Verify the last block's signature
	lastBlock := blockBuff[len(blockBuff)-1]

	// If last block is a config block, we verified it using the policy of the previous block, so it's valid.
	if isLastBlockConfigBlock {
		return nil
	}

	return VerifyBlockSignature(lastBlock, signatureVerifier, config)
}

var ErrNotAConfig = errors.New("not a config block")

// ConfigFromBlock returns a ConfigEnvelope if exists, or a *NotAConfigBlock error.
// It may also return some other error in case parsing failed.
func ConfigFromBlock(block *common.Block) (*common.ConfigEnvelope, error) {
	if block == nil || block.Data == nil || len(block.Data.Data) == 0 {
		return nil, errors.New("empty block")
	}
	txn := block.Data.Data[0]
	env, err := protoutil.GetEnvelopeFromBlock(txn)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	payload, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if block.Header.Number == 0 {
		configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
		if err != nil {
			return nil, errors.Wrap(err, "invalid config envelope")
		}
		return configEnvelope, nil
	}
	if payload.Header == nil {
		return nil, errors.New("nil header in payload")
	}
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if common.HeaderType(chdr.Type) != common.HeaderType_CONFIG {
		return nil, ErrNotAConfig
	}
	configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
	if err != nil {
		return nil, errors.Wrap(err, "invalid config envelope")
	}
	return configEnvelope, nil
}

// VerifyBlockHash verifies the hash chain of the block with the given index
// among the blocks of the given block buffer.
func VerifyBlockHash(indexInBuffer int, blockBuff []*common.Block) error {
	if len(blockBuff) <= indexInBuffer {
		return errors.Errorf("index %d out of bounds (total %d blocks)", indexInBuffer, len(blockBuff))
	}
	block := blockBuff[indexInBuffer]
	if block.Header == nil {
		return errors.New("missing block header")
	}
	seq := block.Header.Number
	dataHash := protoutil.BlockDataHash(block.Data)
	// Verify data hash matches the hash in the header
	if !bytes.Equal(dataHash, block.Header.DataHash) {
		computedHash := hex.EncodeToString(dataHash)
		claimedHash := hex.EncodeToString(block.Header.DataHash)
		return errors.Errorf("computed hash of block (%d) (%s) doesn't match claimed hash (%s)",
			seq, computedHash, claimedHash)
	}
	// We have a previous block in the buffer, ensure current block's previous hash matches the previous one.
	if indexInBuffer > 0 {
		prevBlock := blockBuff[indexInBuffer-1]
		currSeq := block.Header.Number
		if prevBlock.Header == nil {
			return errors.New("previous block header is nil")
		}
		prevSeq := prevBlock.Header.Number
		if prevSeq+1 != currSeq {
			return errors.Errorf("sequences %d and %d were received consecutively", prevSeq, currSeq)
		}
		if !bytes.Equal(block.Header.PreviousHash, protoutil.BlockHeaderHash(prevBlock.Header)) {
			claimedPrevHash := hex.EncodeToString(block.Header.PreviousHash)
			actualPrevHash := hex.EncodeToString(protoutil.BlockHeaderHash(prevBlock.Header))
			return errors.Errorf("block [%d]'s hash (%s) mismatches block [%d]'s prev block hash (%s)",
				prevSeq, actualPrevHash, currSeq, claimedPrevHash)
		}
	}
	return nil
}

// SignatureSetFromBlock creates a signature set out of a block.
func SignatureSetFromBlock(block *common.Block) ([]*protoutil.SignedData, error) {
	if block.Metadata == nil || len(block.Metadata.Metadata) <= int(common.BlockMetadataIndex_SIGNATURES) {
		return nil, errors.New("no metadata in block")
	}
	metadata, err := protoutil.GetMetadataFromBlock(block, common.BlockMetadataIndex_SIGNATURES)
	if err != nil {
		return nil, errors.Errorf("failed unmarshalling medatata for signatures: %v", err)
	}

	var signatureSet []*protoutil.SignedData
	for _, metadataSignature := range metadata.Signatures {
		sigHdr, err := protoutil.UnmarshalSignatureHeader(metadataSignature.SignatureHeader)
		if err != nil {
			return nil, errors.Errorf("failed unmarshalling signature header for block with id %d: %v",
				block.Header.Number, err)
		}
		signatureSet = append(signatureSet,
			&protoutil.SignedData{
				Identity: sigHdr.Creator,
				Data: util.ConcatenateBytes(metadata.Value,
					metadataSignature.SignatureHeader, protoutil.BlockHeaderBytes(block.Header)),
				Signature: metadataSignature.Signature,
			},
		)
	}
	return signatureSet, nil
}

// VerifyBlockSignature verifies the signature on the block with the given BlockVerifier and the given config.
func VerifyBlockSignature(block *common.Block, verifier BlockVerifier, config *common.ConfigEnvelope) error {
	signatureSet, err := SignatureSetFromBlock(block)
	if err != nil {
		return err
	}
	return verifier.VerifyBlockSignature(signatureSet, config)
}

// EndpointCriteria defines criteria of how to connect to a remote orderer node.
type EndpointCriteria struct {
	Endpoint   string   // Endpoint of the form host:port
	TLSRootCAs [][]byte // PEM encoded TLS root CA certificates
}

// String returns a string representation of this EndpointCriteria
func (ep EndpointCriteria) String() string {
	var formattedCAs []interface{}
	for _, rawCAFile := range ep.TLSRootCAs {
		var bl *pem.Block
		pemContent := rawCAFile
		for {
			bl, pemContent = pem.Decode(pemContent)
			if bl == nil {
				break
			}
			cert, err := x509.ParseCertificate(bl.Bytes)
			if err != nil {
				break
			}

			issuedBy := cert.Issuer.String()
			if cert.Issuer.String() == cert.Subject.String() {
				issuedBy = "self"
			}

			info := make(map[string]interface{})
			info["Expired"] = time.Now().After(cert.NotAfter)
			info["Subject"] = cert.Subject.String()
			info["Issuer"] = issuedBy
			formattedCAs = append(formattedCAs, info)
		}
	}

	formattedEndpointCriteria := make(map[string]interface{})
	formattedEndpointCriteria["Endpoint"] = ep.Endpoint
	formattedEndpointCriteria["CAs"] = formattedCAs

	rawJSON, err := json.Marshal(formattedEndpointCriteria)
	if err != nil {
		return fmt.Sprintf("{\"Endpoint\": \"%s\"}", ep.Endpoint)
	}

	return string(rawJSON)
}

// // EndpointconfigFromConfigBlock retrieves TLS CA certificates and endpoints
// // from a config block.
// func EndpointconfigFromConfigBlock(block *common.Block, bccsp bccsp.BCCSP) ([]EndpointCriteria, error) {
// 	if block == nil {
// 		return nil, errors.New("nil block")
// 	}
// 	envelopeConfig, err := protoutil.ExtractEnvelope(block, 0)
// 	if err != nil {
// 		return nil, err
// 	}

// 	bundle, err := channelconfig.NewBundleFromEnvelope(envelopeConfig, bccsp)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "failed extracting bundle from envelope")
// 	}

// 	return EndpointconfigFromConfig(bundle)
// }

// EndpointconfigFromConfig retrieves TLS CA certificates and endpoints
// from a config block.
func EndpointconfigFromConfig(resources channelconfig.Resources) ([]EndpointCriteria, error) {
	msps, err := resources.MSPManager().GetMSPs()
	if err != nil {
		return nil, errors.Wrap(err, "failed obtaining MSPs from MSPManager")
	}
	ordererConfig, ok := resources.OrdererConfig()
	if !ok {
		return nil, errors.New("failed obtaining orderer config from bundle")
	}

	mspIDsToCACerts := make(map[string][][]byte)
	var aggregatedTLSCerts [][]byte
	for _, org := range ordererConfig.Organizations() {
		// Validate that every orderer org has a corresponding MSP instance in the MSP Manager.
		msp, exists := msps[org.MSPID()]
		if !exists {
			return nil, errors.Errorf("no MSP found for MSP with ID of %s", org.MSPID())
		}

		// Build a per org mapping of the TLS CA certs for this org,
		// and aggregate all TLS CA certs into aggregatedTLSCerts to be used later on.
		var caCerts [][]byte
		caCerts = append(caCerts, msp.GetTLSIntermediateCerts()...)
		caCerts = append(caCerts, msp.GetTLSRootCerts()...)
		mspIDsToCACerts[org.MSPID()] = caCerts
		aggregatedTLSCerts = append(aggregatedTLSCerts, caCerts...)
	}

	endpointsPerOrg := perOrgEndpoints(ordererConfig, mspIDsToCACerts)
	if len(endpointsPerOrg) > 0 {
		return endpointsPerOrg, nil
	}

	return globalEndpointsFromConfig(aggregatedTLSCerts, resources), nil
}

func perOrgEndpoints(ordererConfig channelconfig.Orderer, mspIDsToCerts map[string][][]byte) []EndpointCriteria {
	var endpointsPerOrg []EndpointCriteria

	for _, org := range ordererConfig.Organizations() {
		for _, endpoint := range org.Endpoints() {
			endpointsPerOrg = append(endpointsPerOrg, EndpointCriteria{
				TLSRootCAs: mspIDsToCerts[org.MSPID()],
				Endpoint:   endpoint,
			})
		}
	}

	return endpointsPerOrg
}

func globalEndpointsFromConfig(aggregatedTLSCerts [][]byte, bundle channelconfig.Resources) []EndpointCriteria {
	var globalEndpoints []EndpointCriteria
	for _, endpoint := range bundle.ChannelConfig().OrdererAddresses() {
		globalEndpoints = append(globalEndpoints, EndpointCriteria{
			Endpoint:   endpoint,
			TLSRootCAs: aggregatedTLSCerts,
		})
	}
	return globalEndpoints
}

// BlockVerifierAssembler creates a BlockVerifier out of a config envelope
type BlockVerifierAssembler struct {
	Logger *flogging.FabricLogger
	BCCSP  bccsp.BCCSP
}

// VerifierFromConfig creates a BlockVerifier from the given configuration.
func (bva *BlockVerifierAssembler) VerifierFromConfig(configuration *common.ConfigEnvelope, channel string) (BlockVerifier, error) {
	bundle, err := channelconfig.NewBundle(channel, configuration.Config, bva.BCCSP)
	if err != nil {
		return nil, errors.Wrap(err, "failed extracting bundle from envelope")
	}
	policyMgr := bundle.PolicyManager()

	return &BlockValidationPolicyVerifier{
		Logger:    bva.Logger,
		PolicyMgr: policyMgr,
		Channel:   channel,
		BCCSP:     bva.BCCSP,
	}, nil
}

// BlockValidationPolicyVerifier verifies signatures based on the block validation policy.
type BlockValidationPolicyVerifier struct {
	Logger    *flogging.FabricLogger
	Channel   string
	PolicyMgr policies.Manager
	BCCSP     bccsp.BCCSP
}

// VerifyBlockSignature verifies the signed data associated to a block, optionally with the given config envelope.
func (bv *BlockValidationPolicyVerifier) VerifyBlockSignature(sd []*protoutil.SignedData, envelope *common.ConfigEnvelope) error {
	policyMgr := bv.PolicyMgr
	// If the envelope passed isn't nil, we should use a different policy manager.
	if envelope != nil {
		bundle, err := channelconfig.NewBundle(bv.Channel, envelope.Config, bv.BCCSP)
		if err != nil {
			buff := &bytes.Buffer{}
			protolator.DeepMarshalJSON(buff, envelope.Config)
			bv.Logger.Errorf("Failed creating a new bundle for channel %s, Config content is: %s", bv.Channel, buff.String())
			return err
		}
		bv.Logger.Infof("Initializing new PolicyManager for channel %s", bv.Channel)
		policyMgr = bundle.PolicyManager()
	}
	policy, exists := policyMgr.GetPolicy(policies.BlockValidation)
	if !exists {
		return errors.Errorf("policy %s wasn't found", policies.BlockValidation)
	}
	return policy.EvaluateSignedData(sd)
}

// ErrSkipped denotes that replicating a chain was skipped
var ErrSkipped = errors.New("skipped")

// ErrForbidden denotes that an ordering node refuses sending blocks due to access control.
var ErrForbidden = errors.New("forbidden pulling the channel")

// ErrServiceUnavailable denotes that an ordering node is not servicing at the moment.
var ErrServiceUnavailable = errors.New("service unavailable")

// ErrNotInChannel denotes that an ordering node is not in the channel
var ErrNotInChannel = errors.New("not in the channel")

var ErrRetryCountExhausted = errors.New("retry attempts exhausted")

func BundleFromConfigBlock(block *common.Block, bccsp bccsp.BCCSP) (*channelconfig.Bundle, error) {
	if block.Data == nil || len(block.Data.Data) == 0 {
		return nil, errors.New("block contains no data")
	}

	env := &common.Envelope{}
	if err := proto.Unmarshal(block.Data.Data[0], env); err != nil {
		return nil, err
	}

	bundle, err := channelconfig.NewBundleFromEnvelope(env, bccsp)
	if err != nil {
		return nil, err
	}

	return bundle, nil
}

// getConsentersAndPolicyFromConfigBlock returns a tuple of (bftEnabled, consenters, policy, error)
func getConsentersAndPolicyFromConfig(config channelconfig.Resources) (bool, []*common.Consenter, policies.Policy, error) {
	policy, exists := config.PolicyManager().GetPolicy(policies.BlockValidation)
	if !exists {
		return false, nil, nil, errors.New("no policies in config block")
	}

	bftEnabled := config.ChannelConfig().Capabilities().ConsensusTypeBFT()

	var consenters []*common.Consenter
	if bftEnabled {
		cfg, ok := config.OrdererConfig()
		if !ok {
			return false, nil, nil, errors.New("no orderer section in config block")
		}
		consenters = cfg.Consenters()
	}

	return bftEnabled, consenters, policy, nil
}

// BFTEnabledInConfig takes a config block as input and returns true if consenter type is BFT and also returns 'f', max byzantine suspected nodes
func bftEnabledInConfig(config channelconfig.Resources) (bool, int, error) {
	bftEnabled, consenters, _, err := getConsentersAndPolicyFromConfig(config)
	// in a bft setting, total consenter nodes should be atleast `3f+1`, to tolerate f failures
	f := int((len(consenters) - 1) / 3)
	return bftEnabled, f, err
}

// // endpointconfigFromConfigBlockV3 retrieves TLS CA certificates and endpoints from a config block.
// // Unlike the EndpointconfigFromConfigBlockV function, it doesn't use a BCCSP and also doesn't honor global orderer addresses.
// func endpointconfigFromConfigBlockV3(block *common.Block) ([]EndpointCriteria, error) {
// 	if block == nil {
// 		return nil, errors.New("nil block")
// 	}

// 	envelope, err := protoutil.ExtractEnvelope(block, 0)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// unmarshal the payload bytes
// 	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// unmarshal the config envelope bytes
// 	configEnv := &common.ConfigEnvelope{}
// 	if err := proto.Unmarshal(payload.Data, configEnv); err != nil {
// 		return nil, err
// 	}

// 	if configEnv.Config == nil || configEnv.Config.ChannelGroup == nil || configEnv.Config.ChannelGroup.Groups == nil ||
// 		configEnv.Config.ChannelGroup.Groups[channelconfig.OrdererGroupKey] == nil || configEnv.Config.ChannelGroup.Groups[channelconfig.OrdererGroupKey].Groups == nil {
// 		return nil, errors.Errorf("invalid config, orderer groups is empty")
// 	}

// 	ordererGrp := configEnv.Config.ChannelGroup.Groups[channelconfig.OrdererGroupKey].Groups

// 	return perOrgEndpointsByMSPID(ordererGrp)
// }

func BlockVerifierBuilder() func(config channelconfig.Resources) protoutil.BlockVerifierFunc {
	return func(config channelconfig.Resources) protoutil.BlockVerifierFunc {
		// bundle, err := BundleFromConfigBlock(block, bccsp)
		// if err != nil {
		// 	return createErrorFunc(err)
		// }

		policy, exists := config.PolicyManager().GetPolicy(policies.BlockValidation)
		if !exists {
			return createErrorFunc(errors.New("no policies in config block"))
		}

		bftEnabled := config.ChannelConfig().Capabilities().ConsensusTypeBFT()

		var consenters []*common.Consenter
		if bftEnabled {
			cfg, ok := config.OrdererConfig()
			if !ok {
				return createErrorFunc(errors.New("no orderer section in config block"))
			}
			consenters = cfg.Consenters()
		}

		return protoutil.BlockSignatureVerifier(bftEnabled, consenters, policy)
	}
}

func createErrorFunc(err error) protoutil.BlockVerifierFunc {
	return func(_ *common.BlockHeader, _ *common.BlockMetadata) error {
		return errors.Wrap(err, "initialized with an invalid config block")
	}
}

// func searchConsenterIdentityByID(consenters []*common.Consenter, identifier uint32) []byte {
// 	for _, consenter := range consenters {
// 		if consenter.Id == identifier {
// 			return protoutil.MarshalOrPanic(&msp.SerializedIdentity{
// 				Mspid:   consenter.MspId,
// 				IdBytes: consenter.Identity,
// 			})
// 		}
// 	}
// 	return nil
// }

// // perOrgEndpointsByMSPID returns the per orderer org endpoints
// func perOrgEndpointsByMSPID(ordererGrp map[string]*common.ConfigGroup) ([]EndpointCriteria, error) {
// 	var res []EndpointCriteria

// 	for _, group := range ordererGrp {
// 		mspConfig := &msp.MSPConfig{}
// 		if err := proto.Unmarshal(group.Values[channelconfig.MSPKey].Value, mspConfig); err != nil {
// 			return nil, errors.Wrap(err, "failed parsing MSPConfig")
// 		}
// 		// Skip non fabric MSPs, they cannot be orderers.
// 		if mspConfig.Type != int32(mspconstants.FABRIC) {
// 			continue
// 		}

// 		fabricConfig := &msp.FabricMSPConfig{}
// 		if err := proto.Unmarshal(mspConfig.Config, fabricConfig); err != nil {
// 			return nil, errors.Wrap(err, "failed marshaling FabricMSPConfig")
// 		}

// 		var rootCAs [][]byte

// 		rootCAs = append(rootCAs, fabricConfig.TlsRootCerts...)
// 		rootCAs = append(rootCAs, fabricConfig.TlsIntermediateCerts...)

// 		if perOrgAddresses := group.Values[channelconfig.EndpointsKey]; perOrgAddresses != nil {
// 			ordererEndpoints := &common.OrdererAddresses{}
// 			if err := proto.Unmarshal(perOrgAddresses.Value, ordererEndpoints); err != nil {
// 				return nil, errors.Wrap(err, "failed unmarshalling orderer addresses")
// 			}

// 			for _, endpoint := range ordererEndpoints.Addresses {
// 				res = append(res, EndpointCriteria{
// 					TLSRootCAs: rootCAs,
// 					Endpoint:   endpoint,
// 				})
// 			}
// 		}
// 	}

// 	return res, nil
// }
