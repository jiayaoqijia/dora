package db

import (
	"bytes"
	"path/filepath"
	"slices"
	"testing"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethpandaops/dora/dbtypes"
	"github.com/jmoiron/sqlx"
)

func TestHezeInclusionViewSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dora.sqlite")
	oldReader, oldWriter, oldEngine := ReaderDb, writerDb, DbEngine
	DbEngine = dbtypes.DBEngineSqlite
	t.Cleanup(func() { ReaderDb, writerDb, DbEngine = oldReader, oldWriter, oldEngine })
	open := func() *sqlx.DB {
		t.Helper()
		conn, err := sqlx.Connect("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		ReaderDb, writerDb = conn, conn
		return conn
	}
	conn := open()
	if err := ApplyEmbeddedDbSchema(-2); err != nil {
		t.Fatal(err)
	}
	const slot = phase0.Slot(67)
	newView := func(root, dependent byte, tx byte) *dbtypes.HezeInclusionView {
		dr := phase0.Root{dependent}
		return &dbtypes.HezeInclusionView{
			BlockRoot: phase0.Root{root}, DependentRoot: dr, Slot: 66,
			Committee: []string{"2", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "42", "14", "15"},
			Lists: []dbtypes.HezeInclusionObservation{{SignedList: dbtypes.HezeSignedInclusionList{
				Message: dbtypes.HezeInclusionMessage{
					Slot: 66, DependentRoot: dr, ValidatorIndex: 42,
					Transactions: []hexutil.Bytes{{tx}},
				},
				Signature: phase0.BLSSignature{1},
			}}},
		}
	}
	canonical := newView(1, 2, 0xab)
	alternate := newView(3, 4, 0xcd)
	for _, view := range []*dbtypes.HezeInclusionView{canonical, alternate, newView(1, 2, 0xef)} {
		if _, err := StoreHezeInclusionView(t.Context(), view, slot); err != nil {
			t.Fatal(err)
		}
	}
	empty := *canonical
	empty.Lists = nil
	if _, err := StoreHezeInclusionView(t.Context(), &empty, slot); err != nil {
		t.Fatal(err)
	}
	bad := newView(1, 9, 0xab)
	if _, err := StoreHezeInclusionView(t.Context(), bad, slot); err == nil {
		t.Fatal("accepted another dependent root for the same block")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	open()
	for i, want := range []*dbtypes.HezeInclusionView{canonical, alternate} {
		got, err := LoadHezeInclusionView(t.Context(), want.BlockRoot)
		if err != nil || got == nil {
			t.Fatalf("load failed: %v", err)
		}
		if err := got.Validate(want.BlockRoot, slot); err != nil {
			t.Fatal(err)
		}
		if len(got.Lists) != 1 || !slices.Equal(got.Positions(42), []int{13}) ||
			!slices.Equal(got.Positions(2), []int{0, 1}) || got.Lists[0].Equivocated != (i == 0) ||
			!bytes.Equal(got.Lists[0].SignedList.Message.Transactions[0], want.Lists[0].SignedList.Message.Transactions[0]) {
			t.Fatalf("wrong view after reopening: %+v", got)
		}
	}
}
