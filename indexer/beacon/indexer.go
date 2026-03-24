package beacon

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	v1 "github.com/attestantio/go-eth2-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/jmoiron/sqlx"
	dynssz "github.com/pk910/dynamic-ssz"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"

	"github.com/ethpandaops/dora/clients/consensus"
	"github.com/ethpandaops/dora/clients/execution"
	"github.com/ethpandaops/dora/db"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/ethpandaops/dora/utils"
	"github.com/ethpandaops/ethwallclock"
)

const EtherGweiFactor = 1_000_000_000

// ExecutionClientProvider is an interface for getting execution clients
// This is used as a fallback when snooper is not configured
type ExecutionClientProvider interface {
	GetAllExecutionClients() []*execution.Client
}

// Indexer is responsible for indexing the ethereum beacon chain.
type Indexer struct {
	ctx                   context.Context
	logger                logrus.FieldLogger
	consensusPool         *consensus.Pool
	dynSsz                *dynssz.DynSsz
	synchronizer          *synchronizer
	executionTimeProvider ExecutionTimeProvider
	// executionClientProvider is used as fallback for Gloas/Heze blocks when snooper is not configured
	executionClientProvider ExecutionClientProvider

	// configuration
	disableSync           bool
	disableBlockDbWrite   bool
	blockCompression      bool
	inMemoryEpochs        uint16
	activityHistoryLength uint16
	maxParallelStateCalls uint16

	// caches
	blockCache        *blockCache
	epochCache        *epochCache
	forkCache         *forkCache
	pubkeyCache       *pubkeyCache
	validatorCache    *validatorCache
	validatorActivity *validatorActivityCache

	// indexer state
	clients               []*Client
	dbWriter              *dbWriter
	running               bool
	backfillCompleteMutex sync.Mutex
	backfillingCount      int
	backfillComplete      bool
	backfillCompleteChan  chan bool
	lastFinalizedEpoch    phase0.Epoch
	lastPrunedEpoch       phase0.Epoch
	lastPruneRunEpoch     phase0.Epoch
	lastPrecalcRunEpoch   phase0.Epoch
	finalitySubscription  *utils.Subscription[*v1.Finality]
	wallclockSubscription *utils.Subscription[*ethwallclock.Slot]

	// dispatchers
	blockDispatcher *utils.Dispatcher[*Block]

	// canonical head state
	canonicalHeadMutex   sync.Mutex
	canonicalHead        *Block
	canonicalComputation phase0.Root
	cachedChainHeads     []*ChainHead
	badChainRoots        []phase0.Root
}

// NewIndexer creates a new instance of the Indexer.
func NewIndexer(ctx context.Context, logger logrus.FieldLogger, consensusPool *consensus.Pool) *Indexer {
	// Initialize the indexer with default values from the configuration.
	inMemoryEpochs := utils.Config.Indexer.InMemoryEpochs
	if inMemoryEpochs < 2 {
		inMemoryEpochs = 2
	}
	activityHistoryLength := utils.Config.Indexer.ActivityHistoryLength
	if activityHistoryLength == 0 {
		activityHistoryLength = 6
	}
	maxParallelStateCalls := uint16(utils.Config.Indexer.MaxParallelValidatorSetRequests)
	if maxParallelStateCalls < 2 {
		maxParallelStateCalls = 2
	}
	blockCompression := true
	if utils.Config.KillSwitch.DisableBlockCompression {
		blockCompression = false
	}

	// Create the indexer instance.
	indexer := &Indexer{
		ctx:                   ctx,
		logger:                logger,
		consensusPool:         consensusPool,
		disableSync:           utils.Config.Indexer.DisableSynchronizer,
		disableBlockDbWrite:   utils.Config.Indexer.DisableBlockDB,
		blockCompression:      blockCompression,
		inMemoryEpochs:        inMemoryEpochs,
		activityHistoryLength: activityHistoryLength,
		maxParallelStateCalls: maxParallelStateCalls,

		clients:              make([]*Client, 0),
		backfillCompleteChan: make(chan bool),

		blockDispatcher: &utils.Dispatcher[*Block]{},
	}

	indexer.blockCache = newBlockCache(indexer)
	indexer.epochCache = newEpochCache(indexer)
	indexer.forkCache = newForkCache(indexer)
	indexer.pubkeyCache = newPubkeyCache(indexer, utils.Config.Indexer.PubkeyCachePath)
	indexer.validatorCache = newValidatorCache(indexer)
	indexer.validatorActivity = newValidatorActivityCache(indexer)
	indexer.dbWriter = newDbWriter(indexer)

	badChainRoots := utils.Config.Indexer.BadChainRoots
	if len(badChainRoots) > 0 {
		for _, root := range badChainRoots {
			indexer.badChainRoots = append(indexer.badChainRoots, phase0.Root(utils.MustParseHex(root)))
		}
	}

	return indexer
}

func (indexer *Indexer) SetExecutionTimeProvider(executionTimeProvider ExecutionTimeProvider) {
	indexer.executionTimeProvider = executionTimeProvider
}

// SetExecutionClientProvider sets the execution client provider for fallback EL queries
// This is used for Gloas/Heze blocks when snooper is not configured
func (indexer *Indexer) SetExecutionClientProvider(provider ExecutionClientProvider) {
	indexer.executionClientProvider = provider
}

func (indexer *Indexer) GetActivityHistoryLength() uint16 {
	return indexer.activityHistoryLength
}

func (indexer *Indexer) SubscribeBlockEvent(capacity int, blocking bool) *utils.Subscription[*Block] {
	return indexer.blockDispatcher.Subscribe(capacity, blocking)
}

func (indexer *Indexer) getMinInMemoryEpoch() phase0.Epoch {
	minInMemoryEpoch := phase0.Epoch(0)
	if indexer.lastFinalizedEpoch > 0 {
		minInMemoryEpoch = indexer.lastFinalizedEpoch - 1
	}
	if indexer.lastPrunedEpoch > 0 && indexer.lastPrunedEpoch > minInMemoryEpoch {
		minInMemoryEpoch = indexer.lastPrunedEpoch - 1
	}

	return minInMemoryEpoch
}

func (indexer *Indexer) getAbsoluteMinInMemoryEpoch() phase0.Epoch {
	minInMemoryEpoch := phase0.Epoch(0)
	currentEpoch := indexer.consensusPool.GetChainState().CurrentEpoch()
	if currentEpoch > phase0.Epoch(indexer.inMemoryEpochs) {
		minInMemoryEpoch = currentEpoch - phase0.Epoch(indexer.inMemoryEpochs)
	} else {
		minInMemoryEpoch = 0
	}
	return minInMemoryEpoch
}

// getMinInMemorySlot returns the minimum in-memory slot based on the indexer's configuration.
func (indexer *Indexer) getMinInMemorySlot() phase0.Slot {
	chainState := indexer.consensusPool.GetChainState()
	minInMemoryEpoch := indexer.getMinInMemoryEpoch()
	if minInMemoryEpoch == 0 {
		return 0
	}
	return chainState.EpochToSlot(indexer.getMinInMemoryEpoch() + 1)
}

func (indexer *Indexer) withBackfillTracker(backfillCb func() error) error {
	indexer.backfillCompleteMutex.Lock()
	indexer.backfillingCount++
	indexer.backfillCompleteMutex.Unlock()

	var resError error

	defer func() {
		indexer.backfillCompleteMutex.Lock()
		indexer.backfillingCount--
		if indexer.backfillingCount == 0 && !indexer.backfillComplete && resError == nil {
			indexer.backfillComplete = true
			close(indexer.backfillCompleteChan)
		}
		indexer.backfillCompleteMutex.Unlock()
	}()

	resError = backfillCb()
	return resError
}

func (indexer *Indexer) awaitBackfillComplete(ctx context.Context, timeout time.Duration) error {
	select {
	case <-indexer.backfillCompleteChan:
	case <-time.After(timeout):
		return fmt.Errorf("timeout waiting for backfill to complete")
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// AddClient adds a new consensus pool client to the indexer.
func (indexer *Indexer) AddClient(index uint16, client *consensus.Client, priority int, archive bool, skipValidators bool) *Client {
	logger := indexer.logger.WithField("client", client.GetName())
	indexerClient := newClient(index, client, priority, archive, skipValidators, indexer, logger)
	indexer.clients = append(indexer.clients, indexerClient)

	return indexerClient
}

// StartIndexer starts the indexing process.
func (indexer *Indexer) StartIndexer() {
	if indexer.running {
		return
	}

	indexer.running = true
	chainState := indexer.consensusPool.GetChainState()

	// initialize dynamic SSZ encoder
	staticSpec := map[string]any{}
	specYaml, err := yaml.Marshal(chainState.GetSpecs())
	if err == nil {
		yaml.Unmarshal(specYaml, &staticSpec)
	}
	// Add Gloas/Heze-specific constants that may not be in the spec response
	addGloasSpecConstants(staticSpec)
	indexer.dynSsz = dynssz.NewDynSsz(staticSpec)

	// initialize synchronizer & restore state
	indexer.synchronizer = newSynchronizer(indexer, indexer.logger.WithField("service", "synchronizer"))
	finalizedSlot := chainState.GetFinalizedSlot()
	finalizedEpoch, _ := chainState.GetFinalizedCheckpoint()
	indexer.lastFinalizedEpoch = finalizedEpoch
	indexer.lastPrecalcRunEpoch = chainState.CurrentEpoch()

	pruneState := dbtypes.IndexerPruneState{}
	db.GetExplorerState(indexer.ctx, "indexer.prunestate", &pruneState)
	indexer.lastPrunedEpoch = phase0.Epoch(pruneState.Epoch)

	if indexer.lastPrunedEpoch < finalizedEpoch {
		indexer.lastPrunedEpoch = finalizedEpoch
		err := db.RunDBTransaction(func(tx *sqlx.Tx) error {
			return indexer.updatePruningState(tx, indexer.lastPrunedEpoch)
		})
		if err != nil {
			indexer.logger.WithError(err).Errorf("error while updating prune state")
		}
	}

	indexer.lastPruneRunEpoch = chainState.CurrentEpoch()

	// restore unfinalized forks from db
	for _, dbFork := range db.GetUnfinalizedForks(indexer.ctx, uint64(finalizedSlot)) {
		fork := newForkFromDb(dbFork)
		indexer.forkCache.addFork(fork)
	}

	if err := indexer.forkCache.loadForkState(); err != nil {
		indexer.logger.WithError(err).Errorf("failed loading fork state")
	}

	// restore finalized validator set from db
	t1 := time.Now()
	if validatorCount, err := indexer.validatorCache.prepopulateFromDB(); err != nil {
		indexer.logger.WithError(err).Errorf("failed loading validator set")
	} else {
		indexer.logger.Infof("restored %v validators from DB (%.3f sec)", validatorCount, time.Since(t1).Seconds())
	}

	// restore unfinalized epoch stats from db
	restoredEpochStats := 0
	t1 = time.Now()
	processingLimiter := make(chan bool, 10)
	processingWaitGroup := sync.WaitGroup{}
	err = db.StreamUnfinalizedDuties(indexer.ctx, uint64(finalizedEpoch), func(dbDuty *dbtypes.UnfinalizedDuty) {
		// restoring epoch stats can be slow as all duties are recomputed
		// parallelize the processing to speed up the restore
		processingWaitGroup.Add(1)
		processingLimiter <- true

		go func() {
			defer func() {
				<-processingLimiter
				processingWaitGroup.Done()
			}()

			epochStats := indexer.epochCache.createOrGetEpochStats(phase0.Epoch(dbDuty.Epoch), phase0.Root(dbDuty.DependentRoot), false)
			pruneStats := dbDuty.Epoch < uint64(indexer.lastPrunedEpoch)

			err := epochStats.restoreFromDb(dbDuty, chainState, !pruneStats)
			if err != nil {
				indexer.logger.WithError(err).Errorf("failed restoring epoch stats for epoch %v (%x) from db", dbDuty.Epoch, dbDuty.DependentRoot)
				return
			}

			epochStats.isInDb = true

			restoredEpochStats++
			if pruneStats {
				epochStats.pruneValues()
			}
		}()
	})
	processingWaitGroup.Wait()
	if err != nil {
		indexer.logger.WithError(err).Errorf("failed restoring unfinalized epoch stats from DB")
	} else {
		indexer.logger.Infof("restored %v unfinalized epoch stats from DB (%.3f sec)", restoredEpochStats, time.Since(t1).Seconds())
	}

	// restore unfinalized epoch aggregations from db
	restoredEpochAggregations := 0
	t1 = time.Now()
	err = db.StreamUnfinalizedEpochs(indexer.ctx, uint64(finalizedEpoch), func(unfinalizedEpoch *dbtypes.UnfinalizedEpoch) {
		epochStats := indexer.epochCache.getEpochStats(phase0.Epoch(unfinalizedEpoch.Epoch), phase0.Root(unfinalizedEpoch.DependentRoot))
		if epochStats == nil {
			indexer.logger.Debugf("failed restoring epoch aggregations for epoch %v [%x] from db: epoch stats not found", unfinalizedEpoch.Epoch, unfinalizedEpoch.DependentRoot)
			return
		}

		if epochStats.prunedEpochAggregations == nil {
			epochStats.prunedEpochAggregations = []*dbtypes.UnfinalizedEpoch{}
		}
		epochStats.prunedEpochAggregations = append(epochStats.prunedEpochAggregations, unfinalizedEpoch)
	})
	if err != nil {
		indexer.logger.WithError(err).Errorf("failed restoring unfinalized epoch aggregations from DB")
	} else {
		indexer.logger.Infof("restored %v unfinalized epoch aggregations from DB (%.3f sec)", restoredEpochAggregations, time.Since(t1).Seconds())
	}

	// restore unfinalized blocks from db
	restoredBlockCount := 0
	restoredBodyCount := 0
	t1 = time.Now()
	err = db.StreamUnfinalizedBlocks(indexer.ctx, uint64(finalizedSlot), func(dbBlock *dbtypes.UnfinalizedBlock) {
		block, _ := indexer.blockCache.createOrGetBlock(phase0.Root(dbBlock.Root), phase0.Slot(dbBlock.Slot))
		block.BlockUID = dbBlock.BlockUid
		block.forkId = ForkKey(dbBlock.ForkId)
		block.forkChecked = true
		block.processingStatus = dbBlock.Status
		block.isInUnfinalizedDb = true
		block.recvDelay = dbBlock.RecvDelay

		err := block.restoreExecutionTimes(uint16(dbBlock.MinExecTime), uint16(dbBlock.MaxExecTime), dbBlock.ExecTimes)
		if err != nil {
			indexer.logger.Warnf("failed restoring execution times for block %v [%x] from db: %v", dbBlock.Slot, dbBlock.Root, err)
		}

		if dbBlock.HeaderVer != 1 {
			indexer.logger.Warnf("failed unmarshal unfinalized block header %v [%x] from db: unsupported header version", dbBlock.Slot, dbBlock.Root)
			return
		}

		header := &phase0.SignedBeaconBlockHeader{}
		err = header.UnmarshalSSZ(dbBlock.HeaderSSZ)
		if err != nil {
			indexer.logger.Warnf("failed unmarshal unfinalized block header %v [%x] from db: %v", dbBlock.Slot, dbBlock.Root, err)
			return
		}

		block.SetHeader(header)
		indexer.blockCache.addBlockToParentMap(block)

		blockBody, err := UnmarshalVersionedSignedBeaconBlockSSZ(indexer.dynSsz, dbBlock.BlockVer, dbBlock.BlockSSZ)
		if err != nil {
			indexer.logger.Warnf("could not restore unfinalized block body %v [%x] from db: %v", dbBlock.Slot, dbBlock.Root, err)
		} else if block.processingStatus == 0 {
			block.SetBlock(blockBody)
			restoredBodyCount++
		} else {
			block.setBlockIndex(blockBody)
			block.isInFinalizedDb = true
		}

		indexer.blockCache.addBlockToExecBlockMap(block)

		blockFork := indexer.forkCache.getForkById(block.forkId)
		if blockFork != nil {
			if blockFork.headBlock == nil || blockFork.headBlock.Slot < block.Slot {
				blockFork.headBlock = block
			}
		}

		indexer.blockCache.latestBlock = block
		restoredBlockCount++

		if time.Since(t1) > 5*time.Second {
			indexer.logger.Infof("restoring unfinalized blocks from DB... (%v done)", restoredBlockCount)
			t1 = time.Now()
		}
	})
	if err != nil {
		indexer.logger.WithError(err).Errorf("failed restoring unfinalized blocks from DB")
	} else {
		indexer.logger.Infof("restored %v unfinalized blocks from DB (%v with bodies, %.3f sec)", restoredBlockCount, restoredBodyCount, time.Since(t1).Seconds())
	}

	// start indexing for all clients
	for _, client := range indexer.clients {
		client.startIndexing()
	}

	// add indexer event handlers
	indexer.finalitySubscription = indexer.consensusPool.SubscribeFinalizedEvent(10)
	indexer.wallclockSubscription = indexer.consensusPool.SubscribeWallclockSlotEvent(1)

	go func() {
		// start processing a bit delayed to allow clients to complete initial block backfill
		if chainState.CurrentEpoch() > 0 {
			indexer.awaitBackfillComplete(indexer.ctx, 5*time.Minute)
			time.Sleep(10 * time.Second)
		} else {
			// load initial state if launched in/pre epoch 0
			genesisBlock := indexer.blockCache.getBlocksBySlot(0)
			if len(genesisBlock) == 0 {
				indexer.logger.Warnf("genesis block not found in cache")
			} else {
				indexer.epochCache.createOrGetEpochStats(0, genesisBlock[0].Root, true)
			}
		}

		indexer.logger.Infof("starting indexer processing (finalization, pruning & synchronization)")

		go indexer.runIndexerLoop()

		// start synchronizer
		indexer.startSynchronizer(indexer.lastFinalizedEpoch)
	}()
}

func (indexer *Indexer) StopIndexer() {
	indexer.pubkeyCache.Close()
}

func (indexer *Indexer) runIndexerLoop() {
	defer func() {
		if err := recover(); err != nil {
			indexer.logger.WithError(err.(error)).Errorf("uncaught panic in indexer.beacon.Indexer.runIndexerLoop subroutine: %v, stack: %v", err, string(debug.Stack()))
			time.Sleep(10 * time.Second)

			go indexer.runIndexerLoop()
		}
	}()

	chainState := indexer.consensusPool.GetChainState()

	for {
		select {
		case finalityEvent := <-indexer.finalitySubscription.Channel():
			err := indexer.processFinalityEvent(finalityEvent)
			if err != nil {
				indexer.logger.WithError(err).Errorf("error processing finality event (epoch: %v, root: %v)", finalityEvent.Finalized.Epoch, finalityEvent.Finalized.Root.String())
			}

			if indexer.lastFinalizedEpoch > indexer.lastPrunedEpoch {
				indexer.lastPrunedEpoch = indexer.lastFinalizedEpoch
				err := db.RunDBTransaction(func(tx *sqlx.Tx) error {
					return indexer.updatePruningState(tx, indexer.lastPrunedEpoch)
				})
				if err != nil {
					indexer.logger.WithError(err).Errorf("error while updating prune state")
				}
			}

			err = indexer.runCachePruning()
			if err != nil {
				indexer.logger.WithError(err).Errorf("failed pruning cache")
			}

			indexer.lastPruneRunEpoch = chainState.CurrentEpoch()

		case slotEvent := <-indexer.wallclockSubscription.Channel():
			genesis := chainState.GetGenesis()
			if time.Since(genesis.GenesisTime) < 0 {
				// genesis time is in the future, skip
				continue
			}

			epoch := chainState.EpochOfSlot(phase0.Slot(slotEvent.Number()))
			slotIndex := chainState.SlotToSlotIndex(phase0.Slot(slotEvent.Number()))
			slotProgress := uint8(100 / chainState.GetSpecs().SlotsPerEpoch * uint64(slotIndex))

			// precalc next canonical duties on epoch start
			if epoch >= indexer.lastPrecalcRunEpoch {
				err := indexer.precalcNextEpochStats(epoch)
				if err != nil {
					indexer.logger.WithError(err).Errorf("failed precalculating epoch %v stats", epoch)
				}

				indexer.lastPrecalcRunEpoch = epoch + 1
			}

			// prune cache if last pruning epoch is outdated and we are at least 50% into the current
			if epoch > indexer.lastPruneRunEpoch && slotProgress >= 50 {
				err := indexer.runCachePruning()
				if err != nil {
					indexer.logger.WithError(err).Errorf("failed pruning cache")
				}

				indexer.lastPruneRunEpoch = epoch
			}

		}
	}
}

// addGloasSpecConstants adds Gloas/Heze-specific constants to the specs map.
// These constants are required for proper SSZ decoding but may not be exposed
// by the CL client's /eth/v1/config/spec API.
func addGloasSpecConstants(specs map[string]any) {
	// Builder registry limit (2^40)
	if _, ok := specs["BUILDER_REGISTRY_LIMIT"]; !ok {
		specs["BUILDER_REGISTRY_LIMIT"] = uint64(1099511627776)
	}
	// Builder pending payments limit (64 for mainnet, derived from 2 * SLOTS_PER_EPOCH)
	if _, ok := specs["BUILDER_PENDING_PAYMENTS_LIMIT"]; !ok {
		if slotsPerEpoch, ok := specs["SLOTS_PER_EPOCH"].(string); ok {
			if slots, err := strconv.ParseUint(slotsPerEpoch, 10, 64); err == nil {
				specs["BUILDER_PENDING_PAYMENTS_LIMIT"] = 2 * slots
			} else {
				specs["BUILDER_PENDING_PAYMENTS_LIMIT"] = uint64(64)
			}
		} else if slotsPerEpoch, ok := specs["SLOTS_PER_EPOCH"].(uint64); ok {
			specs["BUILDER_PENDING_PAYMENTS_LIMIT"] = 2 * slotsPerEpoch
		} else {
			specs["BUILDER_PENDING_PAYMENTS_LIMIT"] = uint64(64)
		}
	}
	// Builder pending withdrawals limit
	if _, ok := specs["BUILDER_PENDING_WITHDRAWALS_LIMIT"]; !ok {
		specs["BUILDER_PENDING_WITHDRAWALS_LIMIT"] = uint64(1048576)
	}
	// Proposer lookahead slots (derived from (MIN_SEED_LOOKAHEAD + 1) * SLOTS_PER_EPOCH)
	if _, ok := specs["PROPOSER_LOOKAHEAD_SLOTS"]; !ok {
		if slotsPerEpoch, ok := specs["SLOTS_PER_EPOCH"].(string); ok {
			if slots, err := strconv.ParseUint(slotsPerEpoch, 10, 64); err == nil {
				specs["PROPOSER_LOOKAHEAD_SLOTS"] = 2 * slots // MIN_SEED_LOOKAHEAD=1, so (1+1)*SLOTS_PER_EPOCH
			} else {
				specs["PROPOSER_LOOKAHEAD_SLOTS"] = uint64(64)
			}
		} else if slotsPerEpoch, ok := specs["SLOTS_PER_EPOCH"].(uint64); ok {
			specs["PROPOSER_LOOKAHEAD_SLOTS"] = 2 * slotsPerEpoch
		} else {
			specs["PROPOSER_LOOKAHEAD_SLOTS"] = uint64(64)
		}
	}
}
