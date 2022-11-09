/*
Copyright IBM Corp. 2022 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package replication_test

import (
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/common/replication"
	"github.com/hyperledger/fabric/common/replication/mocks"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestVerifyBlockSignature(t *testing.T) {
	verifier := &mocks.BlockVerifier{}
	var nilConfigEnvelope *common.ConfigEnvelope
	verifier.On("VerifyBlockSignature", mock.Anything, nilConfigEnvelope).Return(nil)

	block := createBlockChain(3, 3)[0]

	// The block should have a valid structure
	err := replication.VerifyBlockSignature(block, verifier, nil)
	require.NoError(t, err)

	for _, testCase := range []struct {
		name          string
		mutateBlock   func(*common.Block) *common.Block
		errorContains string
	}{
		{
			name:          "nil metadata",
			errorContains: "no metadata in block",
			mutateBlock: func(block *common.Block) *common.Block {
				block.Metadata = nil
				return block
			},
		},
		{
			name:          "zero metadata slice",
			errorContains: "no metadata in block",
			mutateBlock: func(block *common.Block) *common.Block {
				block.Metadata.Metadata = nil
				return block
			},
		},
		{
			name:          "nil metadata",
			errorContains: "failed unmarshalling medatata for signatures",
			mutateBlock: func(block *common.Block) *common.Block {
				block.Metadata.Metadata[0] = []byte{1, 2, 3}
				return block
			},
		},
		{
			name:          "bad signature header",
			errorContains: "failed unmarshalling signature header",
			mutateBlock: func(block *common.Block) *common.Block {
				metadata := protoutil.GetMetadataFromBlockOrPanic(block, common.BlockMetadataIndex_SIGNATURES)
				metadata.Signatures[0].SignatureHeader = []byte{1, 2, 3}
				block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(metadata)
				return block
			},
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			// Create a copy of the block
			blockCopy := &common.Block{}
			err := proto.Unmarshal(protoutil.MarshalOrPanic(block), blockCopy)
			require.NoError(t, err)
			// Mutate the block to sabotage it
			blockCopy = testCase.mutateBlock(blockCopy)
			err = replication.VerifyBlockSignature(blockCopy, verifier, nil)
			require.Contains(t, err.Error(), testCase.errorContains)
		})
	}
}

func TestVerifyBlockHash(t *testing.T) {
	var start uint64 = 3
	var end uint64 = 23

	verify := func(blockchain []*common.Block) error {
		for i := 0; i < len(blockchain); i++ {
			err := replication.VerifyBlockHash(i, blockchain)
			if err != nil {
				return err
			}
		}
		return nil
	}

	// Verify that createBlockChain() creates a valid blockchain
	require.NoError(t, verify(createBlockChain(start, end)))

	twoBlocks := createBlockChain(2, 3)
	twoBlocks[0].Header = nil
	require.EqualError(t, replication.VerifyBlockHash(1, twoBlocks), "previous block header is nil")

	// Index out of bounds
	blockchain := createBlockChain(start, end)
	err := replication.VerifyBlockHash(100, blockchain)
	require.EqualError(t, err, "index 100 out of bounds (total 21 blocks)")

	for _, testCase := range []struct {
		name                string
		mutateBlockSequence func([]*common.Block) []*common.Block
		errorContains       string
	}{
		{
			name:          "non consecutive sequences",
			errorContains: "sequences 12 and 666 were received consecutively",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				blockSequence[len(blockSequence)/2].Header.Number = 666
				assignHashes(blockSequence)
				return blockSequence
			},
		},
		{
			name: "data hash mismatch",
			errorContains: "computed hash of block (13) (dcb2ec1c5e482e4914cb953ff8eedd12774b244b12912afbe6001ba5de9ff800)" +
				" doesn't match claimed hash (07)",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				blockSequence[len(blockSequence)/2].Header.DataHash = []byte{7}
				return blockSequence
			},
		},
		{
			name: "prev hash mismatch",
			errorContains: "block [12]'s hash " +
				"(866351705f1c2f13e10d52ead9d0ca3b80689ede8cc8bf70a6d60c67578323f4) " +
				"mismatches block [13]'s prev block hash (07)",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				blockSequence[len(blockSequence)/2].Header.PreviousHash = []byte{7}
				return blockSequence
			},
		},
		{
			name:          "nil block header",
			errorContains: "missing block header",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				blockSequence[0].Header = nil
				return blockSequence
			},
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			blockchain := createBlockChain(start, end)
			blockchain = testCase.mutateBlockSequence(blockchain)
			err := verify(blockchain)
			require.EqualError(t, err, testCase.errorContains)
		})
	}
}

func TestVerifyBlocks(t *testing.T) {
	var sigSet1 []*protoutil.SignedData
	var sigSet2 []*protoutil.SignedData

	configEnvelope1 := &common.ConfigEnvelope{
		Config: &common.Config{
			Sequence: 1,
		},
	}
	configEnvelope2 := &common.ConfigEnvelope{
		Config: &common.Config{
			Sequence: 2,
		},
	}
	configTransaction := func(envelope *common.ConfigEnvelope) *common.Envelope {
		return &common.Envelope{
			Payload: protoutil.MarshalOrPanic(&common.Payload{
				Data: protoutil.MarshalOrPanic(envelope),
				Header: &common.Header{
					ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
						Type: int32(common.HeaderType_CONFIG),
					}),
				},
			}),
		}
	}

	for _, testCase := range []struct {
		name                  string
		configureVerifier     func(*mocks.BlockVerifier)
		mutateBlockSequence   func([]*common.Block) []*common.Block
		expectedError         string
		verifierExpectedCalls int
	}{
		{
			name: "empty sequence",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				return nil
			},
			expectedError: "buffer is empty",
		},
		{
			name: "prev hash mismatch",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				blockSequence[len(blockSequence)/2].Header.PreviousHash = []byte{7}
				return blockSequence
			},
			expectedError: "block [74]'s hash " +
				"(5cb4bd1b6a73f81afafd96387bb7ff4473c2425929d0862586f5fbfa12d762dd) " +
				"mismatches block [75]'s prev block hash (07)",
		},
		{
			name: "bad signature",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				return blockSequence
			},
			configureVerifier: func(verifier *mocks.BlockVerifier) {
				var nilEnvelope *common.ConfigEnvelope
				verifier.On("VerifyBlockSignature", mock.Anything, nilEnvelope).Return(errors.New("bad signature"))
			},
			expectedError:         "bad signature",
			verifierExpectedCalls: 1,
		},
		{
			name: "block that its type cannot be classified",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				blockSequence[len(blockSequence)/2].Data = &common.BlockData{
					Data: [][]byte{protoutil.MarshalOrPanic(&common.Envelope{})},
				}
				blockSequence[len(blockSequence)/2].Header.DataHash = protoutil.BlockDataHash(blockSequence[len(blockSequence)/2].Data)
				assignHashes(blockSequence)
				return blockSequence
			},
			expectedError: "nil header in payload",
		},
		{
			name: "config blocks in the sequence need to be verified and one of them is improperly signed",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				var err error
				// Put a config transaction in block n / 4
				blockSequence[len(blockSequence)/4].Data = &common.BlockData{
					Data: [][]byte{protoutil.MarshalOrPanic(configTransaction(configEnvelope1))},
				}
				blockSequence[len(blockSequence)/4].Header.DataHash = protoutil.BlockDataHash(blockSequence[len(blockSequence)/4].Data)

				// Put a config transaction in block n / 2
				blockSequence[len(blockSequence)/2].Data = &common.BlockData{
					Data: [][]byte{protoutil.MarshalOrPanic(configTransaction(configEnvelope2))},
				}
				blockSequence[len(blockSequence)/2].Header.DataHash = protoutil.BlockDataHash(blockSequence[len(blockSequence)/2].Data)

				assignHashes(blockSequence)

				sigSet1, err = replication.SignatureSetFromBlock(blockSequence[len(blockSequence)/4])
				require.NoError(t, err)
				sigSet2, err = replication.SignatureSetFromBlock(blockSequence[len(blockSequence)/2])
				require.NoError(t, err)

				return blockSequence
			},
			configureVerifier: func(verifier *mocks.BlockVerifier) {
				var nilEnvelope *common.ConfigEnvelope
				// The first config block, validates correctly.
				verifier.On("VerifyBlockSignature", sigSet1, nilEnvelope).Return(nil).Once()
				// However, the second config block - validates incorrectly.
				confEnv1 := &common.ConfigEnvelope{}
				proto.Unmarshal(protoutil.MarshalOrPanic(configEnvelope1), confEnv1)
				verifier.On("VerifyBlockSignature", sigSet2, confEnv1).Return(errors.New("bad signature")).Once()
			},
			expectedError:         "bad signature",
			verifierExpectedCalls: 2,
		},
		{
			name: "config block in the sequence needs to be verified, and it is properly signed",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				var err error
				// Put a config transaction in block n / 4
				blockSequence[len(blockSequence)/4].Data = &common.BlockData{
					Data: [][]byte{protoutil.MarshalOrPanic(configTransaction(configEnvelope1))},
				}
				blockSequence[len(blockSequence)/4].Header.DataHash = protoutil.BlockDataHash(blockSequence[len(blockSequence)/4].Data)

				assignHashes(blockSequence)

				sigSet1, err = replication.SignatureSetFromBlock(blockSequence[len(blockSequence)/4])
				require.NoError(t, err)

				sigSet2, err = replication.SignatureSetFromBlock(blockSequence[len(blockSequence)-1])
				require.NoError(t, err)

				return blockSequence
			},
			configureVerifier: func(verifier *mocks.BlockVerifier) {
				var nilEnvelope *common.ConfigEnvelope
				confEnv1 := &common.ConfigEnvelope{}
				proto.Unmarshal(protoutil.MarshalOrPanic(configEnvelope1), confEnv1)
				verifier.On("VerifyBlockSignature", sigSet1, nilEnvelope).Return(nil).Once()
				verifier.On("VerifyBlockSignature", sigSet2, confEnv1).Return(nil).Once()
			},
			// We have a single config block in the 'middle' of the chain, so we have 2 verifications total:
			// The last block, and the config block.
			verifierExpectedCalls: 2,
		},
		{
			name: "last two blocks are config blocks, last block only verified once",
			mutateBlockSequence: func(blockSequence []*common.Block) []*common.Block {
				var err error
				// Put a config transaction in block n-2 and in n-1
				blockSequence[len(blockSequence)-2].Data = &common.BlockData{
					Data: [][]byte{protoutil.MarshalOrPanic(configTransaction(configEnvelope1))},
				}
				blockSequence[len(blockSequence)-2].Header.DataHash = protoutil.BlockDataHash(blockSequence[len(blockSequence)-2].Data)

				blockSequence[len(blockSequence)-1].Data = &common.BlockData{
					Data: [][]byte{protoutil.MarshalOrPanic(configTransaction(configEnvelope2))},
				}
				blockSequence[len(blockSequence)-1].Header.DataHash = protoutil.BlockDataHash(blockSequence[len(blockSequence)-1].Data)

				assignHashes(blockSequence)

				sigSet1, err = replication.SignatureSetFromBlock(blockSequence[len(blockSequence)-2])
				require.NoError(t, err)

				sigSet2, err = replication.SignatureSetFromBlock(blockSequence[len(blockSequence)-1])
				require.NoError(t, err)

				return blockSequence
			},
			configureVerifier: func(verifier *mocks.BlockVerifier) {
				var nilEnvelope *common.ConfigEnvelope
				confEnv1 := &common.ConfigEnvelope{}
				proto.Unmarshal(protoutil.MarshalOrPanic(configEnvelope1), confEnv1)
				verifier.On("VerifyBlockSignature", sigSet1, nilEnvelope).Return(nil).Once()
				// We ensure that the signature set of the last block is verified using the config envelope of the block
				// before it.
				verifier.On("VerifyBlockSignature", sigSet2, confEnv1).Return(nil).Once()
				// Note that we do not record a call to verify the last block, with the config envelope extracted from the block itself.
			},
			// We have 2 config blocks, yet we only verify twice- the first config block, and the next config block, but no more,
			// since the last block is a config block.
			verifierExpectedCalls: 2,
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			blockchain := createBlockChain(50, 100)
			blockchain = testCase.mutateBlockSequence(blockchain)
			verifier := &mocks.BlockVerifier{}
			if testCase.configureVerifier != nil {
				testCase.configureVerifier(verifier)
			}
			err := replication.VerifyBlocks(blockchain, verifier)
			if testCase.expectedError != "" {
				require.EqualError(t, err, testCase.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

//go:generate counterfeiter -o mocks/bccsp.go --fake-name BCCSP . iBCCSP

type iBCCSP interface {
	bccsp.BCCSP
}

func TestBlockVerifierBuilderEmptyBlock(t *testing.T) {
	bvfunc := replication.BlockVerifierBuilder(&mocks.BCCSP{})
	block := &common.Block{}
	verifier := bvfunc(block)
	require.ErrorContains(t, verifier(nil, nil), "initialized with an invalid config block: block contains no data")
}

func TestBlockVerifierBuilderNoConfigBlock(t *testing.T) {
	bvfunc := replication.BlockVerifierBuilder(&mocks.BCCSP{})
	block := createBlockChain(3, 3)[0]
	verifier := bvfunc(block)
	md := &common.BlockMetadata{}
	require.ErrorContains(t, verifier(nil, md), "initialized with an invalid config block: channelconfig Config cannot be nil")
}

func TestBlockVerifierFunc(t *testing.T) {
	block := sampleConfigBlock()
	bvfunc := replication.BlockVerifierBuilder(&mocks.BCCSP{})

	verifier := bvfunc(block)

	header := &common.BlockHeader{}
	md := &common.BlockMetadata{
		Metadata: [][]byte{
			protoutil.MarshalOrPanic(&common.Metadata{Signatures: []*common.MetadataSignature{
				{
					Signature:        []byte{},
					IdentifierHeader: protoutil.MarshalOrPanic(&common.IdentifierHeader{Identifier: 1}),
				},
			}}),
		},
	}

	err := verifier(header, md)
	require.NoError(t, err)
}

func sampleConfigBlock() *common.Block {
	return &common.Block{
		Header: &common.BlockHeader{
			PreviousHash: []byte("foo"),
		},
		Data: &common.BlockData{
			Data: [][]byte{
				protoutil.MarshalOrPanic(&common.Envelope{
					Payload: protoutil.MarshalOrPanic(&common.Payload{
						Header: &common.Header{
							ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{
								Type:      int32(common.HeaderType_CONFIG),
								ChannelId: "mychannel",
							}),
						},
						Data: protoutil.MarshalOrPanic(&common.ConfigEnvelope{
							Config: &common.Config{
								ChannelGroup: &common.ConfigGroup{
									Values: map[string]*common.ConfigValue{
										"Capabilities": {
											Value: protoutil.MarshalOrPanic(&common.Capabilities{
												Capabilities: map[string]*common.Capability{"V3_0": {}},
											}),
										},
										"HashingAlgorithm": {
											Value: protoutil.MarshalOrPanic(&common.HashingAlgorithm{Name: "SHA256"}),
										},
										"BlockDataHashingStructure": {
											Value: protoutil.MarshalOrPanic(&common.BlockDataHashingStructure{Width: math.MaxUint32}),
										},
									},
									Groups: map[string]*common.ConfigGroup{
										"Orderer": {
											Policies: map[string]*common.ConfigPolicy{
												"BlockValidation": {
													Policy: &common.Policy{
														Type: 3,
													},
												},
											},
											Values: map[string]*common.ConfigValue{
												"BatchSize": {
													Value: protoutil.MarshalOrPanic(&orderer.BatchSize{
														MaxMessageCount:   500,
														AbsoluteMaxBytes:  10485760,
														PreferredMaxBytes: 2097152,
													}),
												},
												"BatchTimeout": {
													Value: protoutil.MarshalOrPanic(&orderer.BatchTimeout{
														Timeout: "2s",
													}),
												},
												"Capabilities": {
													Value: protoutil.MarshalOrPanic(&common.Capabilities{
														Capabilities: map[string]*common.Capability{"V3_0": {}},
													}),
												},
												"ConsensusType": {
													Value: protoutil.MarshalOrPanic(&common.BlockData{Data: [][]byte{[]byte("BFT")}}),
												},
												"Orderers": {
													Value: protoutil.MarshalOrPanic(&common.Orderers{
														ConsenterMapping: []*common.Consenter{
															{
																Id:       1,
																Host:     "host1",
																Port:     8001,
																MspId:    "msp1",
																Identity: []byte("identity1"),
															},
														},
													}),
												},
											},
										},
									},
								},
							},
						}),
					}),
					Signature: []byte("bar"),
				}),
			},
		},
	}
}

func assignHashes(blockchain []*common.Block) {
	for i := 1; i < len(blockchain); i++ {
		blockchain[i].Header.PreviousHash = protoutil.BlockHeaderHash(blockchain[i-1].Header)
	}
}

func createBlockChain(start, end uint64) []*common.Block {
	newBlock := func(seq uint64) *common.Block {
		sHdr := &common.SignatureHeader{
			Creator: []byte{1, 2, 3},
			Nonce:   []byte{9, 5, 42, 66},
		}
		block := protoutil.NewBlock(seq, nil)
		blockSignature := &common.MetadataSignature{
			SignatureHeader: protoutil.MarshalOrPanic(sHdr),
		}
		block.Metadata.Metadata[common.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&common.Metadata{
			Value: nil,
			Signatures: []*common.MetadataSignature{
				blockSignature,
			},
		})

		txn := protoutil.MarshalOrPanic(&common.Envelope{
			Payload: protoutil.MarshalOrPanic(&common.Payload{
				Header: &common.Header{},
			}),
		})
		block.Data.Data = append(block.Data.Data, txn)
		return block
	}
	var blockchain []*common.Block
	for seq := uint64(start); seq <= uint64(end); seq++ {
		block := newBlock(seq)
		block.Data.Data = append(block.Data.Data, make([]byte, 100))
		block.Header.DataHash = protoutil.BlockDataHash(block.Data)
		blockchain = append(blockchain, block)
	}
	assignHashes(blockchain)
	return blockchain
}
