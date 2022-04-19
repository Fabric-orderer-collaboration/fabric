// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import (
	"github.com/hyperledger/fabric-protos-go/orderer"
	mock "github.com/stretchr/testify/mock"
)

// AttestationSource is an autogenerated mock type for the AttestationSource type
type AttestationSource struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *AttestationSource) Close() {
	_m.Called()
}

// PullAttestations provides a mock function with given fields: seq
func (_m *AttestationSource) PullAttestation(seq uint64) (*orderer.BlockAttestation, error) {
	ret := _m.Called(seq)

	var r0 *orderer.BlockAttestation
	if rf, ok := ret.Get(0).(func(uint64) *orderer.BlockAttestation); ok {
		r0 = rf(seq)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*orderer.BlockAttestation)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint64) error); ok {
		r1 = rf(seq)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}