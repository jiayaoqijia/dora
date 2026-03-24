package dbtypes

import "time"

// InclusionList represents a stored inclusion list from the IL committee.
// Each slot, 16 validators are selected to produce inclusion lists.
// These lists contain transactions that proposers must include in their blocks.
type InclusionList struct {
	Slot                       uint64    `db:"slot"`
	ValidatorIndex             uint64    `db:"validator_index"`
	InclusionListCommitteeRoot []byte    `db:"inclusion_list_committee_root"`
	Transactions               []byte    `db:"transactions"`      // SSZ encoded transactions
	TransactionsCount          uint32    `db:"transactions_count"`
	TotalSize                  uint32    `db:"total_size"`        // Total size in bytes (max 8 KiB)
	Signature                  []byte    `db:"signature"`         // BLS signature
	IsEquivocated              bool      `db:"is_equivocated"`    // True if validator equivocated
	ReceivedAt                 time.Time `db:"received_at"`       // When this IL was received
	BlockRoot                  []byte    `db:"block_root"`        // Block that satisfied this IL (nullable)
	IsSatisfied                bool      `db:"is_satisfied"`      // Whether this IL was satisfied
	ForkId                     uint64    `db:"fork_id"`           // Fork ID for this IL
}

// InclusionListCommittee represents the IL committee assignments for a slot.
// Each slot has exactly 16 committee members.
type InclusionListCommittee struct {
	Slot           uint64 `db:"slot"`
	CommitteeRoot  []byte `db:"committee_root"`   // Root of the committee calculation
	ValidatorIndex uint64 `db:"validator_index"`  // Committee member validator index
	CommitteeIndex uint8  `db:"committee_index"`  // Position in committee (0-15)
	ForkId         uint64 `db:"fork_id"`
}

// InclusionListSatisfaction tracks which ILs were satisfied in each block.
// This is populated from the InclusionListBits field in Heze blocks.
type InclusionListSatisfaction struct {
	BlockRoot         []byte `db:"block_root"`
	Slot              uint64 `db:"slot"`
	InclusionListBits []byte `db:"inclusion_list_bits"` // BitVector[16] - 2 bytes
	SatisfiedCount    uint8  `db:"satisfied_count"`     // Number of satisfied ILs
	TotalReceived     uint8  `db:"total_received"`      // Total ILs received for previous slot
	ForkId            uint64 `db:"fork_id"`
}

// InclusionListStats holds aggregated statistics for inclusion lists.
type InclusionListStats struct {
	Slot            uint64  `db:"slot"`
	TotalILs        uint8   `db:"total_ils"`        // Total ILs received (0-16)
	SatisfiedILs    uint8   `db:"satisfied_ils"`    // ILs that were satisfied
	EquivocatedILs  uint8   `db:"equivocated_ils"`  // ILs that were equivocated
	SatisfactionRate float64 `db:"satisfaction_rate"` // SatisfiedILs / TotalILs
	TotalTxCount    uint32  `db:"total_tx_count"`   // Total transactions across all ILs
	TotalTxSize     uint64  `db:"total_tx_size"`    // Total size of all transactions
}