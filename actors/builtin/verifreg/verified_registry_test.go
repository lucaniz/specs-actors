package verifreg_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/specs-actors/actors/abi/big"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/verifreg"
	"github.com/filecoin-project/specs-actors/actors/runtime/exitcode"
	"github.com/filecoin-project/specs-actors/actors/util/adt"
	"github.com/filecoin-project/specs-actors/support/mock"
	tutil "github.com/filecoin-project/specs-actors/support/testing"
	"github.com/stretchr/testify/require"
)

func TestExports(t *testing.T) {
	mock.CheckActorExports(t, verifreg.Actor{})
}

func TestConstruction(t *testing.T) {
	actor := verifreg.Actor{}
	receiver := tutil.NewIDAddr(t, 100)
	builder := mock.NewBuilder(context.Background(), receiver).
		WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID)

	rt := builder.Build(t)

	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)

	raddr := tutil.NewIDAddr(t, 101)

	ret := rt.Call(actor.Constructor, &raddr).(*adt.EmptyValue)
	require.Nil(t, ret)
	rt.Verify()

	store := adt.AsStore(rt)

	emptyMap, err := adt.MakeEmptyMap(store).Root()
	require.NoError(t, err)

	var state verifreg.State
	rt.GetState(&state)

	require.Equal(t, emptyMap, state.VerifiedClients)
	require.Equal(t, emptyMap, state.Verifiers)
	require.Equal(t, raddr, state.RootKey)
}

func TestAddVerifier(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	va := tutil.NewIDAddr(t, 201)
	allowance := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(42))

	t.Run("fails when caller is not the root key", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		verifier := ac.mkVerifierParams(va, allowance)

		rt.ExpectValidateCallerAddr(ac.rootkey)

		rt.SetCaller(tutil.NewIDAddr(t, 501), builtin.VerifiedRegistryActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.AddVerifier, verifier)
		})
		rt.Verify()

	})

	t.Run("successfully add a verifier", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		ac.addNewVerifier(rt, va, allowance)
	})
}

func TestRemoveVerifier(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	va := tutil.NewIDAddr(t, 201)
	allowance := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(42))

	t.Run("fails when caller is not the root key", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		// add a verifier
		v := ac.addNewVerifier(rt, va, allowance)

		rt.ExpectValidateCallerAddr(ac.rootkey)

		rt.SetCaller(tutil.NewIDAddr(t, 501), builtin.VerifiedRegistryActorCodeID)
		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.RemoveVerifier, &v.Address)
		})
		rt.Verify()
	})

	t.Run("fails when verifier does not exist", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		rt.ExpectValidateCallerAddr(ac.rootkey)

		rt.SetCaller(ac.rootkey, builtin.VerifiedRegistryActorCodeID)
		v := tutil.NewIDAddr(t, 501)
		rt.ExpectAbort(exitcode.ErrIllegalState, func() {
			rt.Call(ac.RemoveVerifier, &v)
		})
		rt.Verify()
	})

	t.Run("successfully remove a verifier", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		ac.addNewVerifier(rt, va, allowance)

		ac.removeVerifier(rt, va)
	})
}

func TestAddVerifiedClient(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	clientAddr := tutil.NewIDAddr(t, 201)
	clientAddr2 := tutil.NewIDAddr(t, 202)
	clientAddr3 := tutil.NewIDAddr(t, 203)
	clientAddr4 := tutil.NewIDAddr(t, 204)

	verifierAddr := tutil.NewIDAddr(t, 301)
	verifierAddr2 := tutil.NewIDAddr(t, 302)
	allowance := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(42))
	clientAllowance := big.Sub(allowance, big.NewInt(1))

	t.Run("successfully add multiple verified clients from different verifiers", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		c1 := ac.mkClientParams(clientAddr, clientAllowance)
		c2 := ac.mkClientParams(clientAddr2, clientAllowance)
		c3 := ac.mkClientParams(clientAddr3, clientAllowance)
		c4 := ac.mkClientParams(clientAddr4, clientAllowance)

		// verifier 1 should have enough allowance for both clients
		verifier := ac.mkVerifierParams(verifierAddr, allowance)
		verifier.Allowance = big.Sum(c1.Allowance, c2.Allowance)
		ac.addVerifier(rt, verifier.Address, verifier.Allowance)

		// verifier 2 should have enough allowance for both clients
		verifier2 := ac.mkVerifierParams(verifierAddr2, allowance)
		verifier2.Allowance = big.Sum(c3.Allowance, c4.Allowance)
		ac.addVerifier(rt, verifier2.Address, verifier2.Allowance)

		// add client 1 & 2
		ac.addVerifiedClient(rt, verifier.Address, c1.Address, c1.Allowance)
		ac.addVerifiedClient(rt, verifier.Address, c2.Address, c2.Allowance)

		// add clients 3 & 4
		ac.addVerifiedClient(rt, verifier2.Address, c3.Address, c3.Allowance)
		ac.addVerifiedClient(rt, verifier2.Address, c4.Address, c4.Allowance)

		// all clients should exist and verifiers should have no more allowance left
		require.EqualValues(t, c1.Allowance, ac.getClientCap(rt, c1.Address))
		require.EqualValues(t, c2.Allowance, ac.getClientCap(rt, c2.Address))
		require.EqualValues(t, c3.Allowance, ac.getClientCap(rt, c3.Address))
		require.EqualValues(t, c4.Allowance, ac.getClientCap(rt, c4.Address))

		require.EqualValues(t, big.Zero(), ac.getVerifierCap(rt, verifierAddr))
		require.EqualValues(t, big.Zero(), ac.getVerifierCap(rt, verifierAddr2))
	})

	t.Run("verifier successfully adds a verified client and then fails on adding another verified client because of low allowance", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		c1 := ac.mkClientParams(clientAddr, clientAllowance)
		c2 := ac.mkClientParams(clientAddr2, clientAllowance)

		// verifier only has enough balance for one client
		verifier := ac.mkVerifierParams(verifierAddr, allowance)
		verifier.Allowance = c1.Allowance
		ac.addVerifier(rt, verifier.Address, verifier.Allowance)

		// add client 1 works
		ac.addVerifiedClient(rt, verifier.Address, c1.Address, c1.Allowance)

		// add client 2 fails
		rt.SetCaller(verifier.Address, builtin.VerifiedRegistryActorCodeID)
		rt.ExpectValidateCallerAny()
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.AddVerifiedClient, c2)
		})
		rt.Verify()

		// one client should exist and verifier should have no more allowance left
		require.EqualValues(t, c1.Allowance, ac.getClientCap(rt, c1.Address))
		require.EqualValues(t, big.Zero(), ac.getVerifierCap(rt, verifierAddr))
	})

	t.Run("fails when allowance is less than MinVerifiedDealSize", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		allowance := big.Sub(verifreg.MinVerifiedDealSize, big.NewInt(1))
		p := &verifreg.AddVerifiedClientParams{tutil.NewIDAddr(t, 501), allowance}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.AddVerifiedClient, p)
		})
		rt.Verify()
	})

	t.Run("fails when caller is not a verifier", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		client := ac.mkClientParams(clientAddr, clientAllowance)
		ac.addNewVerifier(rt, verifierAddr, allowance)

		nc := tutil.NewIDAddr(t, 209)
		rt.SetCaller(nc, builtin.VerifiedRegistryActorCodeID)
		rt.ExpectValidateCallerAny()

		rt.ExpectAbort(exitcode.ErrNotFound, func() {
			rt.Call(ac.AddVerifiedClient, client)
		})
		rt.Verify()
	})

	t.Run("fails when verifier cap is less than client allowance", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		verifier := ac.addNewVerifier(rt, verifierAddr, allowance)

		rt.SetCaller(verifier.Address, builtin.VerifiedRegistryActorCodeID)
		rt.ExpectValidateCallerAny()

		client := ac.mkClientParams(clientAddr, clientAllowance)
		client.Allowance = big.Add(verifier.Allowance, big.NewInt(1))
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.AddVerifiedClient, client)
		})
		rt.Verify()
	})

	t.Run("fails when verified client already exists", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		// add verified client with caller 1
		verifier := ac.addNewVerifier(rt, verifierAddr, allowance)
		client := ac.mkClientParams(clientAddr, clientAllowance)
		ac.addVerifiedClient(rt, verifier.Address, client.Address, client.Allowance)

		// add verified client with caller 2
		verifier2 := ac.addNewVerifier(rt, verifierAddr, allowance)
		rt.SetCaller(verifier2.Address, builtin.VerifiedRegistryActorCodeID)
		rt.ExpectValidateCallerAny()
		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.AddVerifiedClient, client)
		})
		rt.Verify()
	})
}

func TestUseBytes(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	clientAddr := tutil.NewIDAddr(t, 201)
	clientAddr2 := tutil.NewIDAddr(t, 202)
	clientAddr3 := tutil.NewIDAddr(t, 203)

	verifierAddr := tutil.NewIDAddr(t, 301)
	vallow := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(100))

	t.Run("successfully consume deal bytes for deals from different verified clients", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		ca1 := big.Mul(verifreg.MinVerifiedDealSize, big.NewInt(3))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr, vallow, ca1)

		ca2 := big.Mul(verifreg.MinVerifiedDealSize, big.NewInt(2))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr2, vallow, ca2)

		ca3 := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(1))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr3, vallow, ca3)

		dSize := verifreg.MinVerifiedDealSize
		bal1 := big.Sub(ca1, dSize)
		bal2 := big.Sub(ca2, dSize)
		// client 1 uses bytes
		ac.useBytes(rt, clientAddr, dSize, &capExpectation{expectedCap: bal1})
		// client 2 uses bytes
		ac.useBytes(rt, clientAddr2, dSize, &capExpectation{expectedCap: bal2})
		// client 3 uses bytes
		ac.useBytes(rt, clientAddr3, dSize, &capExpectation{removed: true})

		// verify
		require.EqualValues(t, bal1, ac.getClientCap(rt, clientAddr))
		require.EqualValues(t, bal2, ac.getClientCap(rt, clientAddr2))
		ac.assertClientRemoved(rt, clientAddr3)

		// client 1 adds a deal and it works
		bal1 = big.Sub(bal1, dSize)
		ac.useBytes(rt, clientAddr, dSize, &capExpectation{expectedCap: bal1})
		// client 2 adds a deal and it works
		ac.useBytes(rt, clientAddr2, dSize, &capExpectation{removed: true})

		// verify
		require.EqualValues(t, bal1, ac.getClientCap(rt, clientAddr))
		ac.assertClientRemoved(rt, clientAddr2)
	})

	t.Run("successfully consume deal bytes for verified client and then fail on next attempt because it does NOT have enough allowance", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, verifreg.MinVerifiedDealSize, big.NewInt(1))

		// add verified client
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr, vallow, clientAllowance)

		// use bytes
		dSize1 := verifreg.MinVerifiedDealSize
		ac.useBytes(rt, clientAddr, dSize1, &capExpectation{expectedCap: big.Sub(clientAllowance, dSize1)})

		// fails now because client does NOT have enough capacity for second deal
		dSize2 := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(2))
		param := &verifreg.UseBytesParams{clientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			ac.useBytes(rt, param.Address, param.DealSize, nil)
		})

		rt.Verify()
	})

	t.Run("successfully consume deal for verified client and then fail on next attempt because it has been removed", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, big.NewInt(1))

		// add verified client
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr, vallow, clientAllowance)

		// use bytes
		dSize1 := verifreg.MinVerifiedDealSize
		ac.useBytes(rt, clientAddr, dSize1, &capExpectation{removed: true})

		// fails now because client has been removed
		dSize2 := verifreg.MinVerifiedDealSize
		param := &verifreg.UseBytesParams{clientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			ac.useBytes(rt, param.Address, param.DealSize, nil)

		})

		rt.Verify()
	})

	t.Run("fail if caller is not storage market actor", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
		param := &verifreg.UseBytesParams{clientAddr, verifreg.MinVerifiedDealSize}

		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.UseBytes, param)
		})

		rt.Verify()
	})

	t.Run("fail if deal size is less than min verified deal size", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		dSize2 := big.Sub(verifreg.MinVerifiedDealSize, big.NewInt(1))
		param := &verifreg.UseBytesParams{clientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			ac.useBytes(rt, param.Address, param.DealSize, nil)
		})

		rt.Verify()
	})

	t.Run("fail if verified client does not exist", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		dSize2 := verifreg.MinVerifiedDealSize
		param := &verifreg.UseBytesParams{clientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			ac.useBytes(rt, param.Address, param.DealSize, nil)

		})

		rt.Verify()
	})

	t.Run("fail if deal size is greater than verified client cap", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, big.NewInt(1))

		// add verified client
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr, vallow, clientAllowance)

		// use bytes
		dSize := big.Add(clientAllowance, big.NewInt(1))
		param := &verifreg.UseBytesParams{clientAddr, dSize}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			ac.useBytes(rt, param.Address, param.DealSize, nil)
		})

		rt.Verify()
	})
}

func TestRestoreBytes(t *testing.T) {
	root := tutil.NewIDAddr(t, 101)
	clientAddr := tutil.NewIDAddr(t, 201)
	clientAddr2 := tutil.NewIDAddr(t, 202)
	clientAddr3 := tutil.NewIDAddr(t, 203)
	verifierAddr := tutil.NewIDAddr(t, 301)
	vallow := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(100))

	t.Run("successfully restore deal bytes for different verified clients", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)

		ca1 := big.Mul(verifreg.MinVerifiedDealSize, big.NewInt(3))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr, vallow, ca1)

		ca2 := big.Mul(verifreg.MinVerifiedDealSize, big.NewInt(2))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr2, vallow, ca2)

		ca3 := big.Add(verifreg.MinVerifiedDealSize, big.NewInt(1))
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr3, vallow, ca3)

		dSize := verifreg.MinVerifiedDealSize
		bal1 := big.Add(ca1, dSize)
		bal2 := big.Add(ca2, dSize)
		bal3 := big.Add(ca3, dSize)

		// client 1 restores bytes
		ac.restoreBytes(rt, clientAddr, dSize, &capExpectation{expectedCap: bal1})
		// client 2 restores bytes
		ac.restoreBytes(rt, clientAddr2, dSize, &capExpectation{expectedCap: bal2})
		// client 3 restores bytes
		ac.restoreBytes(rt, clientAddr3, dSize, &capExpectation{expectedCap: bal3})

		// verify cap for all three clients
		require.EqualValues(t, bal1, ac.getClientCap(rt, clientAddr))
		require.EqualValues(t, bal2, ac.getClientCap(rt, clientAddr2))
		require.EqualValues(t, bal3, ac.getClientCap(rt, clientAddr3))

		bal1 = big.Sub(bal1, dSize)
		bal2 = big.Sub(bal2, dSize)
		// client1 and client2 use bytes
		ac.useBytes(rt, clientAddr, dSize, &capExpectation{expectedCap: bal1})
		ac.useBytes(rt, clientAddr2, dSize, &capExpectation{expectedCap: bal2})

		require.EqualValues(t, bal1, ac.getClientCap(rt, clientAddr))
		require.EqualValues(t, bal2, ac.getClientCap(rt, clientAddr2))
		require.EqualValues(t, bal3, ac.getClientCap(rt, clientAddr3))

		bal1 = big.Add(bal1, dSize)
		bal2 = big.Add(bal2, dSize)
		bal3 = big.Add(bal3, dSize)
		// restore bytes for client1, 2 and 3
		ac.restoreBytes(rt, clientAddr, dSize, &capExpectation{expectedCap: bal1})
		ac.restoreBytes(rt, clientAddr2, dSize, &capExpectation{expectedCap: bal2})
		ac.restoreBytes(rt, clientAddr3, dSize, &capExpectation{expectedCap: bal3})
		require.EqualValues(t, bal1, ac.getClientCap(rt, clientAddr))
		require.EqualValues(t, bal2, ac.getClientCap(rt, clientAddr2))
		require.EqualValues(t, bal3, ac.getClientCap(rt, clientAddr3))
	})

	t.Run("successfully restore bytes after using bytes reduces a client's cap", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, verifreg.MinVerifiedDealSize)

		// add verified client -> use bytes
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr, vallow, clientAllowance)
		dSize1 := verifreg.MinVerifiedDealSize
		bal := big.Sub(clientAllowance, dSize1)
		ac.useBytes(rt, clientAddr, dSize1, &capExpectation{expectedCap: bal})

		sz := verifreg.MinVerifiedDealSize
		ac.restoreBytes(rt, clientAddr, sz, &capExpectation{expectedCap: big.Add(bal, sz)})
	})

	t.Run("successfully restore bytes after using bytes removes a client", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		clientAllowance := big.Sum(verifreg.MinVerifiedDealSize, big.NewInt(1))

		// add verified client -> use bytes -> client is removed
		ac.generateAndAddVerifierAndVerifiedClient(rt, verifierAddr, clientAddr, vallow, clientAllowance)
		dSize1 := verifreg.MinVerifiedDealSize
		ac.useBytes(rt, clientAddr, dSize1, &capExpectation{removed: true})

		sz := verifreg.MinVerifiedDealSize
		ac.restoreBytes(rt, clientAddr, sz, &capExpectation{expectedCap: sz})
	})

	t.Run("fail if caller is not storage market actor", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StoragePowerActorAddr, builtin.StoragePowerActorCodeID)
		param := &verifreg.RestoreBytesParams{clientAddr, verifreg.MinVerifiedDealSize}

		rt.ExpectAbort(exitcode.ErrForbidden, func() {
			rt.Call(ac.RestoreBytes, param)
		})

		rt.Verify()
	})

	t.Run("fail if deal size is less than min verified deal size", func(t *testing.T) {
		rt, ac := basicVerifRegSetup(t, root)
		dSize2 := big.Sub(verifreg.MinVerifiedDealSize, big.NewInt(1))
		rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
		rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)
		param := &verifreg.RestoreBytesParams{clientAddr, dSize2}

		rt.ExpectAbort(exitcode.ErrIllegalArgument, func() {
			rt.Call(ac.RestoreBytes, param)
		})

		rt.Verify()
	})
}

type verifRegActorTestHarness struct {
	rootkey address.Address
	verifreg.Actor
	t testing.TB
}

func basicVerifRegSetup(t *testing.T, root address.Address) (*mock.Runtime, *verifRegActorTestHarness) {
	builder := mock.NewBuilder(context.Background(), builtin.StorageMarketActorAddr).
		WithCaller(builtin.SystemActorAddr, builtin.InitActorCodeID).
		WithActorType(root, builtin.VerifiedRegistryActorCodeID)

	rt := builder.Build(t)

	actor := verifRegActorTestHarness{t: t, rootkey: root}
	actor.constructAndVerify(rt)

	return rt, &actor
}

func (h *verifRegActorTestHarness) constructAndVerify(rt *mock.Runtime) {
	rt.ExpectValidateCallerAddr(builtin.SystemActorAddr)
	ret := rt.Call(h.Constructor, &h.rootkey)
	require.Nil(h.t, ret)
	rt.Verify()
}

func (h *verifRegActorTestHarness) mkVerifierParams(a address.Address, allowance verifreg.DataCap) *verifreg.AddVerifierParams {
	return &verifreg.AddVerifierParams{Address: a, Allowance: allowance}
}

func (h *verifRegActorTestHarness) mkClientParams(a address.Address, cap verifreg.DataCap) *verifreg.AddVerifiedClientParams {
	return &verifreg.AddVerifiedClientParams{a, cap}
}

func (h *verifRegActorTestHarness) addNewVerifier(rt *mock.Runtime, a address.Address, allowance verifreg.DataCap) *verifreg.AddVerifierParams {
	v := h.mkVerifierParams(a, allowance)
	h.addVerifier(rt, v.Address, v.Allowance)
	return v
}

func (h *verifRegActorTestHarness) generateAndAddVerifierAndVerifiedClient(rt *mock.Runtime, verifierAddr address.Address, clientAddr address.Address,
	verifierAllowance verifreg.DataCap, clientAllowance verifreg.DataCap) {

	// add verifier with greater allowance than client
	verifier := h.mkVerifierParams(verifierAddr, verifierAllowance)
	verifier.Allowance = big.Add(verifier.Allowance, clientAllowance)
	h.addVerifier(rt, verifier.Address, verifier.Allowance)

	// add client
	client := h.mkClientParams(clientAddr, clientAllowance)
	client.Allowance = clientAllowance
	h.addVerifiedClient(rt, verifier.Address, client.Address, client.Allowance)
}

func (h *verifRegActorTestHarness) addVerifiedClient(rt *mock.Runtime, verifier, client address.Address, allowance verifreg.DataCap) {
	rt.SetCaller(verifier, builtin.VerifiedRegistryActorCodeID)
	rt.ExpectValidateCallerAny()

	params := &verifreg.AddVerifiedClientParams{client, allowance}
	rt.Call(h.AddVerifiedClient, params)
	rt.Verify()

	require.EqualValues(h.t, allowance, h.getClientCap(rt, client))
}

func (h *verifRegActorTestHarness) addVerifier(rt *mock.Runtime, verifier address.Address, datacap verifreg.DataCap) {
	param := verifreg.AddVerifierParams{Address: verifier, Allowance: datacap}

	rt.ExpectValidateCallerAddr(h.rootkey)

	rt.SetCaller(h.rootkey, builtin.VerifiedRegistryActorCodeID)
	ret := rt.Call(h.AddVerifier, &param)
	rt.Verify()

	require.Nil(h.t, ret)
	require.EqualValues(h.t, datacap, h.getVerifierCap(rt, verifier))
}

func (h *verifRegActorTestHarness) removeVerifier(rt *mock.Runtime, verifier address.Address) {
	rt.ExpectValidateCallerAddr(h.rootkey)

	rt.SetCaller(h.rootkey, builtin.VerifiedRegistryActorCodeID)
	ret := rt.Call(h.RemoveVerifier, &verifier)
	rt.Verify()

	require.Nil(h.t, ret)
	h.assertVerifierRemoved(rt, verifier)
}

type capExpectation struct {
	expectedCap verifreg.DataCap
	removed     bool
}

func (h *verifRegActorTestHarness) useBytes(rt *mock.Runtime, a address.Address, dealSize verifreg.DataCap, expectedCap *capExpectation) {
	rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
	rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)

	param := &verifreg.UseBytesParams{a, dealSize}

	ret := rt.Call(h.UseBytes, param)
	rt.Verify()
	require.Nil(h.t, ret)

	// assert client cap now
	if expectedCap.removed {
		h.assertClientRemoved(rt, a)
	} else {
		require.EqualValues(h.t, expectedCap.expectedCap, h.getClientCap(rt, a))
	}
}

func (h *verifRegActorTestHarness) restoreBytes(rt *mock.Runtime, a address.Address, dealSize verifreg.DataCap, expectedCap *capExpectation) {
	rt.ExpectValidateCallerAddr(builtin.StorageMarketActorAddr)
	rt.SetCaller(builtin.StorageMarketActorAddr, builtin.StorageMinerActorCodeID)

	// call RestoreBytes
	param := &verifreg.RestoreBytesParams{a, dealSize}
	ret := rt.Call(h.RestoreBytes, param)
	rt.Verify()
	require.Nil(h.t, ret)

	// assert client cap now
	require.EqualValues(h.t, expectedCap.expectedCap, h.getClientCap(rt, a))
}

func (h *verifRegActorTestHarness) getVerifierCap(rt *mock.Runtime, a address.Address) verifreg.DataCap {
	var st verifreg.State
	rt.GetState(&st)

	v, err := adt.AsMap(adt.AsStore(rt), st.Verifiers)
	require.NoError(h.t, err)

	var dc verifreg.DataCap
	found, err := v.Get(verifreg.AddrKey(a), &dc)
	require.NoError(h.t, err)
	require.True(h.t, found)
	return dc
}

func (h *verifRegActorTestHarness) getClientCap(rt *mock.Runtime, a address.Address) verifreg.DataCap {
	var st verifreg.State
	rt.GetState(&st)

	v, err := adt.AsMap(adt.AsStore(rt), st.VerifiedClients)
	require.NoError(h.t, err)

	var dc verifreg.DataCap
	found, err := v.Get(verifreg.AddrKey(a), &dc)
	require.NoError(h.t, err)
	require.True(h.t, found)
	return dc
}

func (h *verifRegActorTestHarness) assertVerifierRemoved(rt *mock.Runtime, a address.Address) {
	var st verifreg.State
	rt.GetState(&st)

	v, err := adt.AsMap(adt.AsStore(rt), st.Verifiers)
	require.NoError(h.t, err)

	var dc verifreg.DataCap
	found, err := v.Get(verifreg.AddrKey(a), &dc)
	require.NoError(h.t, err)
	require.False(h.t, found)
}

func (h *verifRegActorTestHarness) assertClientRemoved(rt *mock.Runtime, a address.Address) {
	var st verifreg.State
	rt.GetState(&st)

	v, err := adt.AsMap(adt.AsStore(rt), st.VerifiedClients)
	require.NoError(h.t, err)

	var dc verifreg.DataCap
	found, err := v.Get(verifreg.AddrKey(a), &dc)
	require.NoError(h.t, err)
	require.False(h.t, found)
}
