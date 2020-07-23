package builtin

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// The built-in actor code IDs
var (
	SystemActorCodeID           cid.Cid
	InitActorCodeID             cid.Cid
	CronActorCodeID             cid.Cid
	AccountActorCodeID          cid.Cid
	StoragePowerActorCodeID     cid.Cid
	StorageMinerActorCodeID     cid.Cid
	StorageMarketActorCodeID    cid.Cid
	PaymentChannelActorCodeID   cid.Cid
	MultisigActorCodeID         cid.Cid
	RewardActorCodeID           cid.Cid
	VerifiedRegistryActorCodeID cid.Cid
	CallerTypesSignable         []cid.Cid
)

var builtinActors map[cid.Cid]string

func init() {
	builder := cid.V1Builder{Codec: cid.Raw, MhType: mh.IDENTITY}
	builtinActors = make(map[cid.Cid]string)

	for id, name := range map[*cid.Cid]string{
		&SystemActorCodeID:           "fil/1/system",
		&InitActorCodeID:             "fil/1/init",
		&CronActorCodeID:             "fil/1/cron",
		&AccountActorCodeID:          "fil/1/account",
		&StoragePowerActorCodeID:     "fil/1/storagepower",
		&StorageMinerActorCodeID:     "fil/1/storageminer",
		&StorageMarketActorCodeID:    "fil/1/storagemarket",
		&PaymentChannelActorCodeID:   "fil/1/paymentchannel",
		&MultisigActorCodeID:         "fil/1/multisig",
		&RewardActorCodeID:           "fil/1/reward",
		&VerifiedRegistryActorCodeID: "fil/1/verifiedregistry",
	} {
		c, err := builder.Sum([]byte(name))
		if err != nil {
			panic(err)
		}
		*id = c
		builtinActors[c] = name
	}

	// Set of actor code types that can represent external signing parties.
	CallerTypesSignable = []cid.Cid{AccountActorCodeID, MultisigActorCodeID}
}

// IsBuiltinActor returns true if the code belongs to an actor defined in this repo.
func IsBuiltinActor(code cid.Cid) bool {
	_, isBuiltin := builtinActors[code]
	return isBuiltin
}

// ActorNameByCode returns the (string) name of the actor given a cid code.
func ActorNameByCode(code cid.Cid) string {
	if !code.Defined() {
		return "<undefined>"
	}

	name, ok := builtinActors[code]
	if !ok {
		return "<unknown>"
	}
	return name
}

// Tests whether a code CID represents an actor that can be an external principal: i.e. an account or multisig.
// We could do something more sophisticated here: https://github.com/filecoin-project/specs-actors/issues/178
func IsPrincipal(code cid.Cid) bool {
	for _, c := range CallerTypesSignable {
		if c.Equals(code) {
			return true
		}
	}
	return false
}
