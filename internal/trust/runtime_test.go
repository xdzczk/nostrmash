package trust

import (
	"fmt"
	"testing"
	"time"
)

type fakeScanRow struct {
	values []any
	err    error
}

func (r fakeScanRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("scan destination mismatch: got %d want %d", len(dest), len(r.values))
	}
	for i := range dest {
		switch d := dest[i].(type) {
		case *int64:
			*d = r.values[i].(int64)
		case **int64:
			if r.values[i] == nil {
				*d = nil
			} else {
				v := r.values[i].(int64)
				*d = &v
			}
		case *int:
			*d = r.values[i].(int)
		case *string:
			*d = r.values[i].(string)
		case **string:
			if r.values[i] == nil {
				*d = nil
			} else {
				v := r.values[i].(string)
				*d = &v
			}
		case *time.Time:
			*d = r.values[i].(time.Time)
		case **time.Time:
			if r.values[i] == nil {
				*d = nil
			} else {
				v := r.values[i].(time.Time)
				*d = &v
			}
		default:
			return fmt.Errorf("unsupported scan destination type %T", dest[i])
		}
	}
	return nil
}

func TestScanRunRow_ScansPhaseFields(t *testing.T) {
	now := time.Now()
	started := now.Add(-time.Minute)
	finished := now
	phase := RunPhaseCompute

	row := fakeScanRow{
		values: []any{
			int64(1), "trust_scores_global", 3, RunStatusRunning, int64(11), 2,
			int64(10), int64(4), "snap-1",
			phase, int64(21), int64(22), int64(23),
			started, finished, "phase error",
			started, finished, "run error", now, now,
		},
	}
	run, err := scanRunRow(row)
	if err != nil {
		t.Fatalf("scan run row: %v", err)
	}
	if run.CurrentPhase == nil || *run.CurrentPhase != RunPhaseCompute {
		t.Fatalf("unexpected current phase: %#v", run.CurrentPhase)
	}
	if run.SyncJobID == nil || run.ComputeJobID == nil || run.PromoteJobID == nil {
		t.Fatalf("expected phase job ids to be set: %+v", run)
	}
	if run.PhaseLastError == nil || *run.PhaseLastError != "phase error" {
		t.Fatalf("expected phase error to be set: %+v", run.PhaseLastError)
	}
}
