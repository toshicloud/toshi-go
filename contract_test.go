package toshi_test

import (
	"testing"
	"time"

	"go.toshicloud.io/api/contract"
)

// TestContractConstants reads a bound of the API module's contract package,
// the limits and defaults generated from the protos' bound options, so the
// module this client pins provides it.
func TestContractConstants(t *testing.T) {
	if got, want := contract.WorkspaceTokenLifetimeMax, 90*24*time.Hour; got != want {
		t.Errorf("contract.WorkspaceTokenLifetimeMax = %v, want %v", got, want)
	}
}
