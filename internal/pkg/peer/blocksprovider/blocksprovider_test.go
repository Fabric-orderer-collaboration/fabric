/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blocksprovider_test

import (
	"fmt"
	"math"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/gossip"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/replication"
	"github.com/hyperledger/fabric/common/replication/mocks"
	"github.com/hyperledger/fabric/internal/pkg/peer/blocksprovider"
	"github.com/hyperledger/fabric/internal/pkg/peer/blocksprovider/fake"
	"github.com/hyperledger/fabric/protoutil"
)

var _ = Describe("Blocksprovider", func() {
	var (
		d                        *blocksprovider.Deliverer
		fakeGossipServiceAdapter *fake.GossipServiceAdapter
		fakeLedgerInfo           *fake.LedgerInfo
		fakeSigner               *fake.Signer
		fakeDeliverClient        *fake.DeliverClient
		fakeSleeper              *fake.Sleeper
		fakeConfigProvider       *fake.ConfigProvider
		doneC                    chan struct{}
		recvStep                 chan struct{}
		endC                     chan struct{}
		mutex                    sync.Mutex
	)

	BeforeEach(func() {
		doneC = make(chan struct{})
		recvStep = make(chan struct{})

		// appease the race detector
		recvStep := recvStep
		doneC := doneC

		fakeGossipServiceAdapter = &fake.GossipServiceAdapter{}
		fakeSigner = &fake.Signer{}

		fakeLedgerInfo = &fake.LedgerInfo{}
		fakeLedgerInfo.LedgerHeightReturns(7, nil)

		fakeDeliverClient = &fake.DeliverClient{}
		fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
			select {
			case <-recvStep:
				return nil, fmt.Errorf("fake-recv-step-error")
			case <-doneC:
				return nil, nil
			}
		}

		fakeDeliverClient.CloseSendStub = func() error {
			select {
			case recvStep <- struct{}{}:
			case <-doneC:
			}
			return nil
		}

		config, _ := replication.BundleFromConfigBlock(sampleConfigBlock(), &mocks.BCCSP{})
		fakeConfigProvider = &fake.ConfigProvider{}
		fakeConfigProvider.ResourcesStub = func() channelconfig.Resources {
			return config
		}

		d = &blocksprovider.Deliverer{
			ChannelID:      "channel-id",
			Gossip:         fakeGossipServiceAdapter,
			Ledger:         fakeLedgerInfo,
			DoneC:          doneC,
			Signer:         fakeSigner,
			Logger:         flogging.MustGetLogger("blocksprovider"),
			TLSCertHash:    []byte("tls-cert-hash"),
			ConfigProvider: fakeConfigProvider,
			BCCSP:          &mocks.BCCSP{},
		}

		fakeSleeper = &fake.Sleeper{}
		blocksprovider.SetSleeper(d, fakeSleeper)
	})

	JustBeforeEach(func() {
		endC = make(chan struct{})
		go func() {
			d.DeliverBlocks()
			close(endC)
		}()
		go func() {
			time.Sleep(10 * time.Second)
			d.Stop()
		}()
	})

	AfterEach(func() {
		close(doneC)
		<-endC
	})

	It("waits patiently for new blocks from the orderer", func() {
		Consistently(endC).ShouldNot(BeClosed())
		mutex.Lock()
		defer mutex.Unlock()
		// Expect(ccs[0].GetState()).NotTo(Equal(connectivity.Shutdown))
	})

	It("checks the ledger height", func() {
		Eventually(fakeLedgerInfo.LedgerHeightCallCount).Should(Equal(1))
	})

	When("the ledger returns an error", func() {
		BeforeEach(func() {
			fakeLedgerInfo.LedgerHeightReturns(0, fmt.Errorf("fake-ledger-error"))
		})

		It("exits the loop", func() {
			Eventually(endC).Should(BeClosed())
		})
	})

	// When("the signer returns an error", func() {
	// 	BeforeEach(func() {
	// 		fakeSigner.SignReturns(nil, fmt.Errorf("fake-signer-error"))
	// 	})

	// 	It("exits the loop", func() {
	// 		Eventually(endC).Should(BeClosed())
	// 	})
	// })

	// It("constructs a deliver client", func() {
	// 	Eventually(fakeDeliverStreamer.DeliverCallCount).Should(Equal(1))
	// })

	// When("the deliver client cannot be created", func() {
	// 	BeforeEach(func() {
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(0, nil, fmt.Errorf("deliver-error"))
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(1, fakeDeliverClient, nil)
	// 	})

	// 	It("closes the grpc connection, sleeps, and tries again", func() {
	// 		Eventually(fakeDeliverStreamer.DeliverCallCount).Should(Equal(2))
	// 		Expect(fakeSleeper.SleepCallCount()).To(Equal(1))
	// 		Expect(fakeSleeper.SleepArgsForCall(0)).To(Equal(100 * time.Millisecond))
	// 	})
	// })

	// When("there are consecutive errors", func() {
	// 	BeforeEach(func() {
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(0, nil, fmt.Errorf("deliver-error"))
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(1, nil, fmt.Errorf("deliver-error"))
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(2, nil, fmt.Errorf("deliver-error"))
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(3, fakeDeliverClient, nil)
	// 	})

	// 	It("sleeps in an exponential fashion and retries until dial is successful", func() {
	// 		Eventually(fakeDeliverStreamer.DeliverCallCount).Should(Equal(4))
	// 		Expect(fakeSleeper.SleepCallCount()).To(Equal(3))
	// 		Expect(fakeSleeper.SleepArgsForCall(0)).To(Equal(100 * time.Millisecond))
	// 		Expect(fakeSleeper.SleepArgsForCall(1)).To(Equal(120 * time.Millisecond))
	// 		Expect(fakeSleeper.SleepArgsForCall(2)).To(Equal(144 * time.Millisecond))
	// 	})
	// })

	// When("the consecutive errors are unbounded and the peer is not a static leader", func() {
	// 	BeforeEach(func() {
	// 		fakeDeliverStreamer.DeliverReturns(nil, fmt.Errorf("deliver-error"))
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(500, fakeDeliverClient, nil)
	// 	})

	// 	It("hits the maximum sleep time value in an exponential fashion and retries until exceeding the max retry duration", func() {
	// 		d.YieldLeadership = true
	// 		Eventually(fakeSleeper.SleepCallCount).Should(Equal(380))
	// 		Expect(fakeSleeper.SleepArgsForCall(25)).To(Equal(9539 * time.Millisecond))
	// 		Expect(fakeSleeper.SleepArgsForCall(26)).To(Equal(10 * time.Second))
	// 		Expect(fakeSleeper.SleepArgsForCall(27)).To(Equal(10 * time.Second))
	// 		Expect(fakeSleeper.SleepArgsForCall(379)).To(Equal(10 * time.Second))
	// 	})
	// })

	// When("the consecutive errors are unbounded and the peer is static leader", func() {
	// 	BeforeEach(func() {
	// 		fakeDeliverStreamer.DeliverReturns(nil, fmt.Errorf("deliver-error"))
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(500, fakeDeliverClient, nil)
	// 	})

	// 	It("hits the maximum sleep time value in an exponential fashion and retries indefinitely", func() {
	// 		d.YieldLeadership = false
	// 		Eventually(fakeSleeper.SleepCallCount).Should(Equal(500))
	// 		Expect(fakeSleeper.SleepArgsForCall(25)).To(Equal(9539 * time.Millisecond))
	// 		Expect(fakeSleeper.SleepArgsForCall(26)).To(Equal(10 * time.Second))
	// 		Expect(fakeSleeper.SleepArgsForCall(27)).To(Equal(10 * time.Second))
	// 		Expect(fakeSleeper.SleepArgsForCall(379)).To(Equal(10 * time.Second))
	// 	})
	// })

	// When("an error occurs, then a block is successfully delivered", func() {
	// 	BeforeEach(func() {
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(0, nil, fmt.Errorf("deliver-error"))
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(1, fakeDeliverClient, nil)
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(1, nil, fmt.Errorf("deliver-error"))
	// 		fakeDeliverStreamer.DeliverReturnsOnCall(2, nil, fmt.Errorf("deliver-error"))
	// 	})

	// 	It("sleeps in an exponential fashion and retries until dial is successful", func() {
	// 		Eventually(fakeDeliverStreamer.DeliverCallCount).Should(Equal(4))
	// 		Expect(fakeSleeper.SleepCallCount()).To(Equal(3))
	// 		Expect(fakeSleeper.SleepArgsForCall(0)).To(Equal(100 * time.Millisecond))
	// 		Expect(fakeSleeper.SleepArgsForCall(1)).To(Equal(120 * time.Millisecond))
	// 		Expect(fakeSleeper.SleepArgsForCall(2)).To(Equal(144 * time.Millisecond))
	// 	})
	// })

	// It("sends a request to the block fetcher for new blocks", func() {
	// 	Eventually(fakeDeliverClient.SendCallCount).Should(Equal(1))
	// 	mutex.Lock()
	// 	defer mutex.Unlock()
	// 	Expect(len(ccs)).To(Equal(1))
	// })

	// When("the send fails", func() {
	// 	BeforeEach(func() {
	// 		fakeDeliverClient.SendReturnsOnCall(0, fmt.Errorf("fake-send-error"))
	// 		fakeDeliverClient.SendReturnsOnCall(1, nil)
	// 		fakeDeliverClient.CloseSendStub = nil
	// 	})

	// 	It("disconnects, sleeps and retries until the send is successful", func() {
	// 		Eventually(fakeDeliverClient.SendCallCount).Should(Equal(2))
	// 		Expect(fakeDeliverClient.CloseSendCallCount()).To(Equal(1))
	// 		Expect(fakeSleeper.SleepCallCount()).To(Equal(1))
	// 		Expect(fakeSleeper.SleepArgsForCall(0)).To(Equal(100 * time.Millisecond))
	// 		mutex.Lock()
	// 		defer mutex.Unlock()
	// 		Expect(len(ccs)).To(Equal(2))
	// 		Eventually(ccs[0].GetState).Should(Equal(connectivity.Shutdown))
	// 	})
	// })

	// It("attempts to read blocks from the deliver stream", func() {
	// 	Eventually(fakeDeliverClient.RecvCallCount).Should(Equal(1))
	// })

	// When("reading blocks from the deliver stream fails", func() {
	// 	BeforeEach(func() {
	// 		// appease the race detector
	// 		doneC := doneC
	// 		recvStep := recvStep
	// 		fakeDeliverClient := fakeDeliverClient

	// 		fakeDeliverClient.CloseSendStub = nil
	// 		fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
	// 			if fakeDeliverClient.RecvCallCount() == 1 {
	// 				return nil, fmt.Errorf("fake-recv-error")
	// 			}
	// 			select {
	// 			case <-recvStep:
	// 				return nil, fmt.Errorf("fake-recv-step-error")
	// 			case <-doneC:
	// 				return nil, nil
	// 			}
	// 		}
	// 	})

	// 	It("disconnects, sleeps, and retries until the recv is successful", func() {
	// 		Eventually(fakeDeliverClient.RecvCallCount).Should(Equal(2))
	// 		Expect(fakeSleeper.SleepCallCount()).To(Equal(1))
	// 		Expect(fakeSleeper.SleepArgsForCall(0)).To(Equal(100 * time.Millisecond))
	// 	})
	// })

	// When("reading blocks from the deliver stream fails and then recovers", func() {
	// 	BeforeEach(func() {
	// 		// appease the race detector
	// 		doneC := doneC
	// 		recvStep := recvStep
	// 		fakeDeliverClient := fakeDeliverClient

	// 		fakeDeliverClient.CloseSendStub = func() error {
	// 			if fakeDeliverClient.CloseSendCallCount() >= 5 {
	// 				select {
	// 				case <-doneC:
	// 				case recvStep <- struct{}{}:
	// 				}
	// 			}
	// 			return nil
	// 		}
	// 		fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
	// 			switch fakeDeliverClient.RecvCallCount() {
	// 			case 1, 2, 4:
	// 				return nil, fmt.Errorf("fake-recv-error")
	// 			case 3:
	// 				return &orderer.DeliverResponse{
	// 					Type: &orderer.DeliverResponse_Block{
	// 						Block: &common.Block{
	// 							Header: &common.BlockHeader{
	// 								Number: 8,
	// 							},
	// 						},
	// 					},
	// 				}, nil
	// 			default:
	// 				select {
	// 				case <-recvStep:
	// 					return nil, fmt.Errorf("fake-recv-step-error")
	// 				case <-doneC:
	// 					return nil, nil
	// 				}
	// 			}
	// 		}
	// 	})

	// 	It("disconnects, sleeps, and retries until the recv is successful and resets the failure count", func() {
	// 		Eventually(fakeDeliverClient.RecvCallCount).Should(Equal(5))
	// 		Expect(fakeSleeper.SleepCallCount()).To(Equal(3))
	// 		Expect(fakeSleeper.SleepArgsForCall(0)).To(Equal(100 * time.Millisecond))
	// 		Expect(fakeSleeper.SleepArgsForCall(1)).To(Equal(120 * time.Millisecond))
	// 		Expect(fakeSleeper.SleepArgsForCall(2)).To(Equal(100 * time.Millisecond))
	// 	})
	// })

	// TODO rewrite these to test the new implementation

	When("the deliver client returns a block", func() {
		BeforeEach(func() {
			// appease the race detector
			doneC := doneC
			recvStep := recvStep
			fakeDeliverClient := fakeDeliverClient

			fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
				if fakeDeliverClient.RecvCallCount() == 1 {
					return &orderer.DeliverResponse{
						Type: &orderer.DeliverResponse_Block{
							Block: &common.Block{
								Header: &common.BlockHeader{
									Number: 8,
								},
							},
						},
					}, nil
				}
				select {
				case <-recvStep:
					return nil, fmt.Errorf("fake-recv-step-error")
				case <-doneC:
					return nil, nil
				}
			}
		})

		// It("receives the block and loops, not sleeping", func() {
		// 	Eventually(fakeDeliverClient.RecvCallCount).Should(Equal(2))
		// 	Expect(fakeSleeper.SleepCallCount()).To(Equal(0))
		// })

		// It("checks the validity of the block", func() {
		// 	Eventually(fakeBlockVerifier.VerifyBlockCallCount).Should(Equal(1))
		// 	channelID, blockNum, block := fakeBlockVerifier.VerifyBlockArgsForCall(0)
		// 	Expect(channelID).To(Equal(gossipcommon.ChannelID("channel-id")))
		// 	Expect(blockNum).To(Equal(uint64(8)))
		// 	Expect(proto.Equal(block, &common.Block{
		// 		Header: &common.BlockHeader{
		// 			Number: 8,
		// 		},
		// 	})).To(BeTrue())
		// })

		// When("the block is invalid", func() {
		// 	BeforeEach(func() {
		// 		fakeBlockVerifier.VerifyBlockReturns(fmt.Errorf("fake-verify-error"))
		// 	})

		// 	It("disconnects, sleeps, and tries again", func() {
		// 		Eventually(fakeSleeper.SleepCallCount).Should(Equal(1))
		// 		Expect(fakeDeliverClient.CloseSendCallCount()).To(Equal(1))
		// 		mutex.Lock()
		// 		defer mutex.Unlock()
		// 		Expect(len(ccs)).To(Equal(2))
		// 	})
		// })

		It("adds the payload to gossip", func() {
			Eventually(fakeGossipServiceAdapter.AddPayloadCallCount).Should(Equal(1))
			channelID, payload := fakeGossipServiceAdapter.AddPayloadArgsForCall(0)
			Expect(channelID).To(Equal("channel-id"))
			Expect(payload).To(Equal(&gossip.Payload{
				Data: protoutil.MarshalOrPanic(&common.Block{
					Header: &common.BlockHeader{
						Number: 8,
					},
				}),
				SeqNum: 8,
			}))
		})

		When("adding the payload fails", func() {
			BeforeEach(func() {
				fakeGossipServiceAdapter.AddPayloadReturns(fmt.Errorf("payload-error"))
			})

			It("disconnects, sleeps, and tries again", func() {
				Eventually(fakeSleeper.SleepCallCount).Should(Equal(1))
				Expect(fakeDeliverClient.CloseSendCallCount()).To(Equal(1))
				mutex.Lock()
				defer mutex.Unlock()
				// Expect(len(ccs)).To(Equal(2))
			})
		})

		It("gossips the block to the other peers", func() {
			Eventually(fakeGossipServiceAdapter.GossipCallCount).Should(Equal(1))
			msg := fakeGossipServiceAdapter.GossipArgsForCall(0)
			Expect(msg).To(Equal(&gossip.GossipMessage{
				Nonce:   0,
				Tag:     gossip.GossipMessage_CHAN_AND_ORG,
				Channel: []byte("channel-id"),
				Content: &gossip.GossipMessage_DataMsg{
					DataMsg: &gossip.DataMessage{
						Payload: &gossip.Payload{
							Data: protoutil.MarshalOrPanic(&common.Block{
								Header: &common.BlockHeader{
									Number: 8,
								},
							}),
							SeqNum: 8,
						},
					},
				},
			}))
		})

		When("gossip dissemination is disabled", func() {
			BeforeEach(func() {
				d.BlockGossipDisabled = true
			})

			It("doesn't gossip, only adds to the payload buffer", func() {
				Eventually(fakeGossipServiceAdapter.AddPayloadCallCount).Should(Equal(1))
				channelID, payload := fakeGossipServiceAdapter.AddPayloadArgsForCall(0)
				Expect(channelID).To(Equal("channel-id"))
				Expect(payload).To(Equal(&gossip.Payload{
					Data: protoutil.MarshalOrPanic(&common.Block{
						Header: &common.BlockHeader{
							Number: 8,
						},
					}),
					SeqNum: 8,
				}))

				Consistently(fakeGossipServiceAdapter.GossipCallCount).Should(Equal(0))
			})
		})
	})

	When("the deliver client returns a status", func() {
		var status common.Status

		BeforeEach(func() {
			// appease the race detector
			doneC := doneC
			recvStep := recvStep
			fakeDeliverClient := fakeDeliverClient

			status = common.Status_SUCCESS
			fakeDeliverClient.RecvStub = func() (*orderer.DeliverResponse, error) {
				if fakeDeliverClient.RecvCallCount() == 1 {
					return &orderer.DeliverResponse{
						Type: &orderer.DeliverResponse_Status{
							Status: status,
						},
					}, nil
				}
				select {
				case <-recvStep:
					return nil, fmt.Errorf("fake-recv-step-error")
				case <-doneC:
					return nil, nil
				}
			}
		})

		// It("disconnects with an error, and sleeps because the block request is infinite and should never complete", func() {
		// 	Eventually(fakeSleeper.SleepCallCount).Should(Equal(1))
		// })

		// When("the status is not successful", func() {
		// 	BeforeEach(func() {
		// 		status = common.Status_FORBIDDEN
		// 	})

		// 	It("still disconnects with an error", func() {
		// 		Eventually(fakeSleeper.SleepCallCount).Should(Equal(1))
		// 	})
		// })
	})
})

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
