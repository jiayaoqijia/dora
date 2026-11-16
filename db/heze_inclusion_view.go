package db

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/jmoiron/sqlx"
)

func LoadHezeInclusionView(ctx context.Context, root phase0.Root) (*dbtypes.HezeInclusionView, error) {
	var payload []byte
	err := ReaderDb.GetContext(ctx, &payload, "SELECT view_data FROM heze_inclusion_views WHERE block_root = $1", root[:])
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var view dbtypes.HezeInclusionView
	if err := json.Unmarshal(payload, &view); err != nil {
		return nil, err
	}
	return &view, nil
}

func StoreHezeInclusionView(ctx context.Context, view *dbtypes.HezeInclusionView, blockSlot phase0.Slot) (*dbtypes.HezeInclusionView, error) {
	if err := view.Validate(view.BlockRoot, blockSlot); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(view)
	if err != nil {
		return nil, err
	}
	var merged dbtypes.HezeInclusionView
	err = RunDBTransaction(func(tx *sqlx.Tx) error {
		// Inserting or updating first serializes concurrent changes to this
		// row in both databases before we merge the saved observations.
		row := tx.QueryRowxContext(ctx, `
			INSERT INTO heze_inclusion_views (block_root, view_data) VALUES ($1, $2)
			ON CONFLICT (block_root) DO UPDATE SET view_data = heze_inclusion_views.view_data
			RETURNING view_data`, view.BlockRoot[:], payload)
		var saved []byte
		if err := row.Scan(&saved); err != nil {
			return err
		}
		if err := json.Unmarshal(saved, &merged); err != nil {
			return err
		}
		if err := merged.Validate(view.BlockRoot, blockSlot); err != nil {
			return err
		}
		if err := merged.Merge(view); err != nil {
			return err
		}
		encoded, err := json.Marshal(&merged)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE heze_inclusion_views SET view_data = $1 WHERE block_root = $2",
			encoded, view.BlockRoot[:])
		return err
	})
	if err != nil {
		return nil, err
	}
	return &merged, nil
}
