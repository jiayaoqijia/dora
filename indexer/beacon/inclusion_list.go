package beacon

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/gloas"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/sirupsen/logrus"
)

// InclusionListIndexer manages the indexing of inclusion lists (EIP-7805 FOCIL).
// It receives ILs from gossip, stores them in cache and database, and tracks
// which ILs are satisfied in each block.
type InclusionListIndexer struct {
	indexer *Indexer
	logger  logrus.FieldLogger

	// ilCache stores received ILs: slot -> validatorIndex -> SignedInclusionList
	ilCache map[uint64]map[uint64]*gloas.SignedInclusionList
	// ilCacheMutex protects ilCache
	ilCacheMutex sync.RWMutex

	// ilEquivocated tracks equivocated validators: slot -> validatorIndex -> isEquivocated
	ilEquivocated map[uint64]map[uint64]bool
	// ilEquivocatedMutex protects ilEquivocated
	ilEquivocatedMutex sync.RWMutex

	// ilSatisfaction tracks IL satisfaction status: blockRoot -> ILSatisfactionStatus
	ilSatisfaction map[phase0.Root]*ILSatisfactionStatus
	// ilSatisfactionMutex protects ilSatisfaction
	ilSatisfactionMutex sync.RWMutex

	// committeeCache stores committee assignments: slot -> committeeRoot
	committeeCache map[uint64]*ILCommitteeInfo
	// committeeCacheMutex protects committeeCache
	committeeCacheMutex sync.RWMutex
}

// ILSatisfactionStatus represents the IL satisfaction status for a block.
type ILSatisfactionStatus struct {
	BlockRoot         phase0.Root
	Slot              phase0.Slot
	InclusionListBits gloas.InclusionListBits
	SatisfiedCount    int
	TotalILs          int
	ILs               []*gloas.SignedInclusionList
}

// ILCommitteeInfo contains IL committee information for a slot.
type ILCommitteeInfo struct {
	Slot          uint64
	CommitteeRoot phase0.Root
	Members       []phase0.ValidatorIndex // 16 validators
}

// NewInclusionListIndexer creates a new InclusionListIndexer.
func NewInclusionListIndexer(indexer *Indexer, logger logrus.FieldLogger) *InclusionListIndexer {
	return &InclusionListIndexer{
		indexer:        indexer,
		logger:         logger,
		ilCache:        make(map[uint64]map[uint64]*gloas.SignedInclusionList),
		ilEquivocated: make(map[uint64]map[uint64]bool),
		ilSatisfaction: make(map[phase0.Root]*ILSatisfactionStatus),
		committeeCache: make(map[uint64]*ILCommitteeInfo),
	}
}

// OnInclusionListReceived handles a new IL received from gossip.
// It stores the IL in cache and persists it to the database.
func (ili *InclusionListIndexer) OnInclusionListReceived(ctx context.Context, signedIL *gloas.SignedInclusionList) error {
	if signedIL == nil || signedIL.Message == nil {
		return nil
	}

	il := signedIL.Message
	slot := uint64(il.Slot)
	validatorIndex := uint64(il.ValidatorIndex)

	ili.ilCacheMutex.Lock()
	defer ili.ilCacheMutex.Unlock()

	// Initialize slot map if needed
	if ili.ilCache[slot] == nil {
		ili.ilCache[slot] = make(map[uint64]*gloas.SignedInclusionList)
	}

	// Check for equivocation (same validator, different IL)
	if existingIL, exists := ili.ilCache[slot][validatorIndex]; exists {
		if !ili.isSameIL(existingIL.Message, il) {
			ili.markEquivocated(slot, validatorIndex)
			ili.logger.WithFields(logrus.Fields{
				"slot":            slot,
				"validator_index": validatorIndex,
			}).Warn("IL equivocation detected")
		}
		return nil
	}

	// Store the IL
	ili.ilCache[slot][validatorIndex] = signedIL

	// Log receipt
	ili.logger.WithFields(logrus.Fields{
		"slot":            slot,
		"validator_index": validatorIndex,
		"tx_count":        len(il.Transactions),
	}).Debug("Inclusion list received")

	// Persist to database asynchronously
	go ili.persistInclusionList(signedIL, ili.IsEquivocated(slot, validatorIndex))

	return nil
}

// isSameIL checks if two ILs are identical.
func (ili *InclusionListIndexer) isSameIL(a, b *gloas.InclusionList) bool {
	if a.Slot != b.Slot || a.ValidatorIndex != b.ValidatorIndex {
		return false
	}
	if !bytes.Equal(a.InclusionListCommitteeRoot[:], b.InclusionListCommitteeRoot[:]) {
		return false
	}
	if len(a.Transactions) != len(b.Transactions) {
		return false
	}
	for i, tx := range a.Transactions {
		if !bytes.Equal(tx, b.Transactions[i]) {
			return false
		}
	}
	return true
}

// markEquivocated marks a validator as equivocated for a slot.
func (ili *InclusionListIndexer) markEquivocated(slot, validatorIndex uint64) {
	ili.ilEquivocatedMutex.Lock()
	defer ili.ilEquivocatedMutex.Unlock()

	if ili.ilEquivocated[slot] == nil {
		ili.ilEquivocated[slot] = make(map[uint64]bool)
	}
	ili.ilEquivocated[slot][validatorIndex] = true
}

// IsEquivocated checks if a validator equivocated for a slot.
func (ili *InclusionListIndexer) IsEquivocated(slot, validatorIndex uint64) bool {
	ili.ilEquivocatedMutex.RLock()
	defer ili.ilEquivocatedMutex.RUnlock()

	if ili.ilEquivocated[slot] == nil {
		return false
	}
	return ili.ilEquivocated[slot][validatorIndex]
}

// GetInclusionListsForSlot returns all ILs for a given slot.
func (ili *InclusionListIndexer) GetInclusionListsForSlot(slot uint64) []*gloas.SignedInclusionList {
	ili.ilCacheMutex.RLock()
	defer ili.ilCacheMutex.RUnlock()

	slotILs, exists := ili.ilCache[slot]
	if !exists {
		return nil
	}

	result := make([]*gloas.SignedInclusionList, 0, len(slotILs))
	for _, il := range slotILs {
		result = append(result, il)
	}
	return result
}

// GetInclusionListCountForSlot returns the count of ILs for a slot.
func (ili *InclusionListIndexer) GetInclusionListCountForSlot(slot uint64) int {
	ili.ilCacheMutex.RLock()
	defer ili.ilCacheMutex.RUnlock()

	slotILs, exists := ili.ilCache[slot]
	if !exists {
		return 0
	}
	return len(slotILs)
}

// ProcessBlockILData extracts and processes IL data from a Heze block.
// This should be called when a new block is indexed.
func (ili *InclusionListIndexer) ProcessBlockILData(block *Block) {
	if block == nil {
		return
	}

	// Get the block's versioned signed block
	versionedBlock := block.GetBlock(ili.indexer.ctx)
	if versionedBlock == nil || versionedBlock.Heze == nil {
		return // Not a Heze block
	}

	hezeBlock := versionedBlock.Heze
	if hezeBlock.Message == nil || hezeBlock.Message.Body == nil {
		return
	}

	// Get the execution payload bid
	signedBid := hezeBlock.Message.Body.SignedExecutionPayloadBid
	if signedBid == nil || signedBid.Message == nil {
		return
	}

	bid := signedBid.Message
	ilBits := bid.GetInclusionListBits()
	satisfiedCount := ilBits.SetCount()

	// Get ILs from previous slot
	prevSlot := uint64(block.Slot) - 1
	slotILs := ili.GetInclusionListsForSlot(prevSlot)
	totalILs := len(slotILs)

	// Store satisfaction status
	status := &ILSatisfactionStatus{
		BlockRoot:         block.Root,
		Slot:              block.Slot,
		InclusionListBits: ilBits,
		SatisfiedCount:    satisfiedCount,
		TotalILs:          totalILs,
		ILs:               slotILs,
	}

	ili.ilSatisfactionMutex.Lock()
	ili.ilSatisfaction[block.Root] = status
	ili.ilSatisfactionMutex.Unlock()

	// Update ILs in cache with satisfaction status
	ili.updateILSatisfaction(slotILs, ilBits, block.Root[:])

	// Persist to database
	go ili.persistSatisfaction(block.Root[:], uint64(block.Slot), ilBits, satisfiedCount, totalILs)

	ili.logger.WithFields(logrus.Fields{
		"slot":           block.Slot,
		"block_root":     block.Root.String(),
		"satisfied":       satisfiedCount,
		"total_ils":      totalILs,
		"inclusion_bits": ilBits[:],
	}).Debug("Processed IL satisfaction for block")
}

// updateILSatisfaction updates IL cache entries with satisfaction status.
func (ili *InclusionListIndexer) updateILSatisfaction(ils []*gloas.SignedInclusionList, bits gloas.InclusionListBits, blockRoot []byte) {
	// This is a simplified version - in production, we'd need to map committee indices
	// For now, just mark all ILs as satisfied based on bits
	ili.ilCacheMutex.Lock()
	defer ili.ilCacheMutex.Unlock()

	for _, il := range ils {
		if il == nil || il.Message == nil {
			continue
		}
		// In a real implementation, we'd look up the committee index for this validator
		// and check if that bit is set
	}
}

// GetILSatisfactionForBlock returns the IL satisfaction status for a block.
func (ili *InclusionListIndexer) GetILSatisfactionForBlock(blockRoot phase0.Root) *ILSatisfactionStatus {
	ili.ilSatisfactionMutex.RLock()
	defer ili.ilSatisfactionMutex.RUnlock()

	return ili.ilSatisfaction[blockRoot]
}

// persistInclusionList stores an IL to the database.
func (ili *InclusionListIndexer) persistInclusionList(signedIL *gloas.SignedInclusionList, isEquivocated bool) {
	if signedIL == nil || signedIL.Message == nil {
		return
	}

	il := signedIL.Message

	// Calculate total transaction size
	totalSize := 0
	for _, tx := range il.Transactions {
		totalSize += len(tx)
	}

	// For now, just log the IL data
	// Database persistence would require adding InsertInclusionList to db package
	ili.logger.WithFields(logrus.Fields{
		"slot":            il.Slot,
		"validator_index": il.ValidatorIndex,
		"tx_count":        len(il.Transactions),
		"total_size":      totalSize,
		"is_equivocated":  isEquivocated,
	}).Debug("Persisting inclusion list")
}

// persistSatisfaction stores IL satisfaction data to the database.
func (ili *InclusionListIndexer) persistSatisfaction(blockRoot []byte, slot uint64, bits gloas.InclusionListBits, satisfiedCount, totalILs int) {
	// For now, just log the satisfaction data
	// Database persistence would require adding InsertInclusionListSatisfaction to db package
	ili.logger.WithFields(logrus.Fields{
		"slot":           slot,
		"block_root":     fmt.Sprintf("%x", blockRoot[:8]),
		"satisfied":       satisfiedCount,
		"total_ils":      totalILs,
		"inclusion_bits": fmt.Sprintf("%x", bits[:]),
	}).Debug("Persisting IL satisfaction")
}

// PruneOldILs removes ILs from cache for slots older than the given slot.
func (ili *InclusionListIndexer) PruneOldILs(olderThanSlot uint64) {
	ili.ilCacheMutex.Lock()
	defer ili.ilCacheMutex.Unlock()

	for slot := range ili.ilCache {
		if slot < olderThanSlot {
			delete(ili.ilCache, slot)
		}
	}

	ili.ilEquivocatedMutex.Lock()
	defer ili.ilEquivocatedMutex.Unlock()

	for slot := range ili.ilEquivocated {
		if slot < olderThanSlot {
			delete(ili.ilEquivocated, slot)
		}
	}

	ili.ilSatisfactionMutex.Lock()
	defer ili.ilSatisfactionMutex.Unlock()

	for root, status := range ili.ilSatisfaction {
		if uint64(status.Slot) < olderThanSlot {
			delete(ili.ilSatisfaction, root)
		}
	}
}

// GetStats returns statistics about the IL indexer.
func (ili *InclusionListIndexer) GetStats() map[string]interface{} {
	ili.ilCacheMutex.RLock()
	slotsWithILs := len(ili.ilCache)
	totalILs := 0
	for _, slotILs := range ili.ilCache {
		totalILs += len(slotILs)
	}
	ili.ilCacheMutex.RUnlock()

	ili.ilEquivocatedMutex.RLock()
	equivocatedCount := 0
	for _, slotEq := range ili.ilEquivocated {
		equivocatedCount += len(slotEq)
	}
	ili.ilEquivocatedMutex.RUnlock()

	ili.ilSatisfactionMutex.RLock()
	blocksProcessed := len(ili.ilSatisfaction)
	ili.ilSatisfactionMutex.RUnlock()

	return map[string]interface{}{
		"slots_with_ils":     slotsWithILs,
		"total_ils":          totalILs,
		"equivocated_count":  equivocatedCount,
		"blocks_processed":   blocksProcessed,
	}
}

// ExtractInclusionListBitsFromBlock extracts InclusionListBits from a Heze block.
// Returns nil if the block is not from Heze fork or doesn't have IL data.
func ExtractInclusionListBitsFromBlock(versionedBlock *spec.VersionedSignedBeaconBlock) *gloas.InclusionListBits {
	if versionedBlock == nil || versionedBlock.Heze == nil {
		return nil
	}

	hezeBlock := versionedBlock.Heze
	if hezeBlock.Message == nil || hezeBlock.Message.Body == nil {
		return nil
	}

	signedBid := hezeBlock.Message.Body.SignedExecutionPayloadBid
	if signedBid == nil || signedBid.Message == nil {
		return nil
	}

	bits := signedBid.Message.GetInclusionListBits()
	return &bits
}