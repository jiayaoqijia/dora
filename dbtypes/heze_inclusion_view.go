package dbtypes

import (
	"bytes"
	"errors"
	"slices"
	"strconv"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

const hezeInclusionListCommitteeSize = 16
const hezeInclusionListBytesLimit = 8192

type HezeInclusionView struct {
	BlockRoot     phase0.Root                `json:"block_root"`
	Slot          uint64                     `json:"slot,string"`
	DependentRoot phase0.Root                `json:"dependent_root"`
	Committee     []string                   `json:"committee"`
	Lists         []HezeInclusionObservation `json:"inclusion_lists"`
}

type HezeInclusionObservation struct {
	SignedList  HezeSignedInclusionList `json:"signed_inclusion_list"`
	Timely      bool                    `json:"timely"`
	Equivocated bool                    `json:"equivocated"`
}

type HezeSignedInclusionList struct {
	Message   HezeInclusionMessage `json:"message"`
	Signature phase0.BLSSignature  `json:"signature"`
}

type HezeInclusionMessage struct {
	Slot           uint64          `json:"slot,string"`
	ValidatorIndex uint64          `json:"validator_index,string"`
	DependentRoot  phase0.Root     `json:"dependent_root"`
	Transactions   []hexutil.Bytes `json:"transactions"`
}

func (v *HezeInclusionView) Positions(validator uint64) []int {
	encoded := strconv.FormatUint(validator, 10)
	var positions []int
	for i, member := range v.Committee {
		if member == encoded {
			positions = append(positions, i)
		}
	}
	return positions
}

func (v *HezeInclusionView) Validate(root phase0.Root, blockSlot phase0.Slot) error {
	if v.BlockRoot != root || blockSlot == 0 || v.Slot != uint64(blockSlot)-1 {
		return errors.New("inclusion view does not match the requested block")
	}
	if len(v.Committee) != hezeInclusionListCommitteeSize {
		return errors.New("invalid inclusion-list committee size")
	}
	for _, member := range v.Committee {
		index, err := strconv.ParseUint(member, 10, 64)
		if err != nil || strconv.FormatUint(index, 10) != member {
			return errors.New("invalid committee validator index")
		}
	}
	seen := make(map[uint64]bool)
	for _, list := range v.Lists {
		msg := list.SignedList.Message
		if msg.Slot != v.Slot || msg.DependentRoot != v.DependentRoot ||
			len(v.Positions(msg.ValidatorIndex)) == 0 || seen[msg.ValidatorIndex] {
			return errors.New("inclusion list does not match the committee view")
		}
		seen[msg.ValidatorIndex] = true
		remaining := hezeInclusionListBytesLimit
		for _, tx := range msg.Transactions {
			if len(tx) == 0 || len(tx) > remaining {
				return errors.New("invalid inclusion-list transaction data")
			}
			remaining -= len(tx)
		}
		if remaining == hezeInclusionListBytesLimit {
			return errors.New("empty signed inclusion list")
		}
	}
	return nil
}

// Merge combines observations from peers while preserving the first signed
// list and any equivocation observed for its validator.
func (v *HezeInclusionView) Merge(view *HezeInclusionView) error {
	if v.BlockRoot != view.BlockRoot || v.Slot != view.Slot ||
		v.DependentRoot != view.DependentRoot || !slices.Equal(v.Committee, view.Committee) {
		return errors.New("conflicting inclusion-list committee views")
	}
	for _, incoming := range view.Lists {
		index := slices.IndexFunc(v.Lists, func(stored HezeInclusionObservation) bool {
			return stored.SignedList.Message.ValidatorIndex == incoming.SignedList.Message.ValidatorIndex
		})
		if index < 0 {
			v.Lists = append(v.Lists, incoming)
			continue
		}
		stored := &v.Lists[index]
		same := slices.EqualFunc(stored.SignedList.Message.Transactions, incoming.SignedList.Message.Transactions,
			func(a, b hexutil.Bytes) bool { return bytes.Equal(a, b) })
		stored.Equivocated = stored.Equivocated || incoming.Equivocated || !same
		if same {
			stored.Timely = stored.Timely || incoming.Timely
		}
	}
	slices.SortFunc(v.Lists, func(a, b HezeInclusionObservation) int {
		return v.Positions(a.SignedList.Message.ValidatorIndex)[0] -
			v.Positions(b.SignedList.Message.ValidatorIndex)[0]
	})
	return nil
}
