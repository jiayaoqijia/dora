-- +goose Up
-- +goose StatementBegin

-- Inclusion Lists table (EIP-7805 FOCIL)
-- Stores inclusion lists received from IL committee members
CREATE TABLE IF NOT EXISTS inclusion_lists (
    slot INTEGER NOT NULL,
    validator_index INTEGER NOT NULL,
    inclusion_list_committee_root BLOB NOT NULL,
    transactions BLOB,
    transactions_count INTEGER DEFAULT 0,
    total_size INTEGER DEFAULT 0,
    signature BLOB NOT NULL,
    is_equivocated BOOLEAN DEFAULT FALSE,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    block_root BLOB,
    is_satisfied BOOLEAN DEFAULT FALSE,
    fork_id INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (slot, validator_index)
);

CREATE INDEX IF NOT EXISTS idx_inclusion_lists_slot ON inclusion_lists(slot);
CREATE INDEX IF NOT EXISTS idx_inclusion_lists_validator ON inclusion_lists(validator_index);
CREATE INDEX IF NOT EXISTS idx_inclusion_lists_block_root ON inclusion_lists(block_root);
CREATE INDEX IF NOT EXISTS idx_inclusion_lists_fork_id ON inclusion_lists(fork_id);

-- IL Committee assignments
-- Tracks which validators are assigned to the IL committee for each slot
CREATE TABLE IF NOT EXISTS inclusion_list_committees (
    slot INTEGER NOT NULL,
    committee_root BLOB NOT NULL,
    validator_index INTEGER NOT NULL,
    committee_index INTEGER NOT NULL,
    fork_id INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (slot, committee_index)
);

CREATE INDEX IF NOT EXISTS idx_il_committees_slot ON inclusion_list_committees(slot);
CREATE INDEX IF NOT EXISTS idx_il_committees_validator ON inclusion_list_committees(validator_index);

-- IL Satisfaction tracking
-- Tracks which ILs were satisfied in each block (from InclusionListBits)
CREATE TABLE IF NOT EXISTS inclusion_list_satisfactions (
    block_root BLOB NOT NULL PRIMARY KEY,
    slot INTEGER NOT NULL,
    inclusion_list_bits BLOB NOT NULL,
    satisfied_count INTEGER DEFAULT 0,
    total_received INTEGER DEFAULT 0,
    fork_id INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_il_satisfactions_slot ON inclusion_list_satisfactions(slot);

-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS inclusion_list_satisfactions;
DROP TABLE IF EXISTS inclusion_list_committees;
DROP TABLE IF EXISTS inclusion_lists;
-- +goose StatementEnd