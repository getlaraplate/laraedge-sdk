package conformance

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestSecondProcessProbeOfAnInterruptedRun: when the run's context ends during the probe of a
// second Process stream, the probe reports the interruption, not a plugin that did not refuse.
func TestSecondProcessProbeOfAnInterruptedRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	s := &suite{ctx: ctx, timeout: time.Second}
	got, probed := s.refusedSecondProcess(&session{conn: conn}, "instance", nil)
	if !probed || !strings.Contains(got, "the run was interrupted") {
		t.Errorf("probe of an interrupted run = %q (probed %t), want it to report the interruption", got, probed)
	}
}
