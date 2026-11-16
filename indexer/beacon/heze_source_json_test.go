package beacon

import (
	"bytes"
	"os"
	"testing"

	"github.com/attestantio/go-eth2-client/spec"
)

func TestGloasSourceJSONRetainedInHistory(t *testing.T) {
	source, err := os.ReadFile("testdata/gloas-lighthouse-block.json")
	if err != nil {
		t.Fatal(err)
	}

	block, err := unmarshalVersionedSignedBeaconBlockJson(uint64(spec.DataVersionGloas)|jsonVersionFlag, source)
	if err != nil {
		t.Fatal(err)
	}
	block.SourceJSON = source

	version, stored, err := MarshalVersionedSignedBeaconBlockSSZ(nil, block, true, false)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := UnmarshalVersionedSignedBeaconBlockSSZ(nil, version, stored)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(source, restored.SourceJSON) || restored.Gloas.Message.Slot != 18 ||
		restored.Gloas.Message.Body.PayloadAttestations[0].Data.Slot != 17 {
		t.Fatal("archived block lost its source JSON or changed its decoded fields")
	}
	if _, _, err := MarshalVersionedSignedBeaconBlockSSZ(nil, restored, false, true); err == nil {
		t.Fatal("generated SSZ using the incompatible draft schema")
	}
}
