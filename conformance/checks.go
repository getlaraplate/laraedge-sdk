package conformance

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/getlaraplate/laraedge-sdk/handshake"
	"github.com/getlaraplate/laraedge-sdk/manifest"
	pb "github.com/getlaraplate/laraedge-sdk/pluginv1"
	"github.com/getlaraplate/laraedge-sdk/sdk"
)

// protocols are the protocol versions the suite offers, as the agent does.
var protocols = []string{"v1"}

// session is a launched and connected plugin.
type session struct {
	proc *handshake.Process
	conn *grpc.ClientConn
}

// launch starts the plugin with its socket in socketDir and its working directory in a fresh
// directory, and connects to it. The process is killed and reaped when the run ends, if it has
// not exited before.
func (s *suite) launch(socketDir string) (*session, error) {
	workDir, err := s.tempDir()
	if err != nil {
		return nil, err
	}
	proc, err := handshake.Launch(s.ctx, handshake.LaunchConfig{
		Path:      s.binary,
		Dir:       workDir,
		SocketDir: socketDir,
		Protocols: protocols,
		Env:       s.env(),
		Stdout:    s.output,
		Stderr:    s.output,
	})
	if err != nil {
		return nil, err
	}
	s.onCleanup(func() {
		_ = proc.Kill()
		_ = proc.Wait()
	})
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	conn, err := proc.Dial(ctx)
	if err != nil {
		return nil, err
	}
	s.onCleanup(func() { _ = conn.Close() })
	return &session{proc: proc, conn: conn}, nil
}

// checkNoCookie starts the binary directly, with the protocols and a socket directory but
// without the cookie: it must exit at once with a non-zero code and print no handshake line.
func (s *suite) checkNoCookie() {
	const name = "no-cookie"
	workDir, err := s.tempDir()
	if err != nil {
		s.fail(name, "%v", err)
		return
	}
	socketDir, err := s.tempDir()
	if err != nil {
		s.fail(name, "%v", err)
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	stdout := &headBuffer{limit: 4096}
	cmd := exec.CommandContext(ctx, s.binary)
	cmd.Dir = workDir
	cmd.Env = append(s.env(), handshake.ProtocolsKey+"="+strings.Join(protocols, ","), handshake.SocketDirKey+"="+socketDir)
	cmd.Stdout = stdout
	cmd.Stderr = s.output
	cmd.WaitDelay = time.Second
	started := time.Now()
	err = cmd.Run()
	elapsed := time.Since(started).Round(time.Millisecond)

	var exitErr *exec.ExitError
	switch {
	case s.ctx.Err() != nil:
		s.fail(name, "the run was cancelled: %v", s.ctx.Err())
		return
	case ctx.Err() != nil:
		s.fail(name, "started without the cookie, the plugin was still running after %v; it must exit at once", s.timeout)
		return
	case err == nil:
		s.fail(name, "started without the cookie, the plugin exited with code 0; it must exit with an error")
		return
	case !errors.As(err, &exitErr):
		s.fail(name, "running the plugin: %v", err)
		return
	}
	for line := range strings.SplitSeq(stdout.String(), "\n") {
		if strings.HasPrefix(line, "1|") {
			s.fail(name, "started without the cookie, the plugin printed the handshake line %q", line)
			return
		}
	}
	s.pass(name, "exited with code %d after %v, no handshake line", exitErr.ExitCode(), elapsed)
}

// checkHandshake launches the plugin for the main session.
func (s *suite) checkHandshake() (*session, bool) {
	const name = "handshake"
	socketDir, err := s.tempDir()
	if err != nil {
		s.fail(name, "%v", err)
		return nil, false
	}
	sess, err := s.launch(socketDir)
	if err != nil {
		s.fail(name, "%v", err)
		return nil, false
	}
	s.pass(name, "1|%s|%s|%s", sess.proc.Protocol, sess.proc.Network, sess.proc.Address)
	return sess, true
}

func (s *suite) checkPermissions(sess *session) {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	detail, err := transportPermissions(ctx, sess.proc.Address)
	if err != nil {
		s.fail("permissions", "%v", err)
		return
	}
	s.pass("permissions", "%s", detail)
}

// checkInfo compares Info with plugin.json.
func (s *suite) checkInfo(sess *session) {
	const name = "info"
	ctx, cancel := context.WithTimeout(s.ctx, infoDeadline)
	defer cancel()
	info, err := pb.NewPluginClient(sess.conn).Info(ctx, &pb.InfoRequest{})
	if err != nil {
		s.fail(name, "Info: %v", err)
		return
	}
	var problems []string
	if info.GetName() != s.manifest.Name {
		problems = append(problems, fmt.Sprintf("name %q, plugin.json says %q", info.GetName(), s.manifest.Name))
	}
	if info.GetVersion() != s.manifest.Version {
		problems = append(problems, fmt.Sprintf("version %q, plugin.json says %q", info.GetVersion(), s.manifest.Version))
	}
	if !slices.Equal(info.GetProtocolVersions(), s.manifest.ProtocolVersions) {
		problems = append(problems, fmt.Sprintf("protocol versions %q, plugin.json says %q", info.GetProtocolVersions(),
			s.manifest.ProtocolVersions))
	}
	if !slices.Contains(info.GetProtocolVersions(), sess.proc.Protocol) {
		problems = append(problems, fmt.Sprintf("the negotiated protocol %q is not among the protocol versions", sess.proc.Protocol))
	}
	reported, err := manifest.Canonical(info.GetManifest())
	if err != nil {
		problems = append(problems, fmt.Sprintf("the manifest of Info is not valid JSON: %v", err))
	} else if committed, err := manifest.Canonical(s.manifestRaw); err != nil {
		problems = append(problems, fmt.Sprintf("plugin.json: %v", err))
	} else if !bytes.Equal(reported, committed) {
		problems = append(problems, fmt.Sprintf("the manifest of Info differs from plugin.json in canonical form from byte %d",
			commonPrefix(reported, committed)))
	}
	if len(problems) > 0 {
		s.fail(name, "%s", strings.Join(problems, "; "))
		return
	}
	s.pass(name, "%s %s, protocols %s, manifest equal to plugin.json", info.GetName(), info.GetVersion(),
		strings.Join(info.GetProtocolVersions(), ","))
}

// commonPrefix returns the length of the common prefix of a and b.
func commonPrefix(a, b []byte) int {
	n := 0
	for n < len(a) && n < len(b) && a[n] == b[n] {
		n++
	}
	return n
}

func (s *suite) checkHealth(sess *session) {
	ctx, cancel := context.WithTimeout(s.ctx, healthDeadline)
	defer cancel()
	resp, err := healthpb.NewHealthClient(sess.conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		s.fail("health", "Check: %v", err)
		return
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		s.fail("health", "status %s, want SERVING", resp.GetStatus())
		return
	}
	s.pass("health", "SERVING")
}

// configureFunc is the Configure call of every kind's client.
type configureFunc func(context.Context, *pb.ConfigureRequest, ...grpc.CallOption) (*pb.ConfigureResponse, error)

// instanceID names the instance the suite configures with a fixture.
func instanceID(kind, group string, i int) string {
	return fmt.Sprintf("conformance-%s-%s-%d", kind, group, i)
}

// checkConfigure configures every fixture of kind on its own instance. It returns the instance of
// the first valid fixture and whether it was accepted.
func (s *suite) checkConfigure(sess *session, kind string) (string, bool) {
	name := kind + ".configure"
	var configure configureFunc
	switch kind {
	case "input":
		configure = pb.NewInputClient(sess.conn).Configure
	case "processor":
		configure = pb.NewProcessorClient(sess.conn).Configure
	default:
		configure = pb.NewOutputClient(sess.conn).Configure
	}
	call := func(id string, section json.RawMessage) ([]*pb.ConfigError, error) {
		ctx, cancel := context.WithTimeout(s.ctx, configureDeadline)
		defer cancel()
		resp, err := configure(ctx, &pb.ConfigureRequest{InstanceId: id, Config: section})
		return resp.GetErrors(), err
	}
	fx := s.fixturesOf(kind)
	var problems []string
	firstAccepted := false
	for i, section := range fx.Valid {
		errs, err := call(instanceID(kind, "valid", i), section)
		switch {
		case err != nil:
			problems = append(problems, fmt.Sprintf("valid fixture %d: Configure failed: %v", i, err))
		case len(errs) > 0:
			problems = append(problems, fmt.Sprintf("valid fixture %d was refused: %s", i, describeConfigErrors(errs)))
		case i == 0:
			firstAccepted = true
		}
	}
	for i, section := range fx.Invalid {
		errs, err := call(instanceID(kind, "invalid", i), section)
		switch {
		case err != nil:
			problems = append(problems, fmt.Sprintf("invalid fixture %d: Configure failed instead of answering configuration errors: %v", i, err))
		case len(errs) == 0:
			problems = append(problems, fmt.Sprintf("invalid fixture %d was accepted", i))
		}
	}
	if len(problems) > 0 {
		s.fail(name, "%s", strings.Join(problems, "; "))
	} else {
		s.pass(name, "%d valid accepted, %d invalid refused", len(fx.Valid), len(fx.Invalid))
	}
	return instanceID(kind, "valid", 0), firstAccepted
}

func describeConfigErrors(errs []*pb.ConfigError) string {
	parts := make([]string, 0, len(errs))
	for _, e := range errs {
		parts = append(parts, e.GetPath()+": "+e.GetMessage())
	}
	return strings.Join(parts, ", ")
}

// Results of firstEvent other than an event or the stream's error.
var (
	errNoEvent       = errors.New("no event")
	errEndedNoEvents = errors.New("the stream ended without an error and without an event")
)

// firstEvent opens a stream of the instance and waits for its first event, at most wait. The
// stream stays open until cancel is called. Without an event the error is errNoEvent (wrapped),
// errEndedNoEvents, or the stream's own error.
func (s *suite) firstEvent(client pb.InputClient, instance string, wait time.Duration) (cancel context.CancelFunc, err error) {
	ctx, cancel := context.WithCancel(s.ctx)
	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{InstanceId: instance})
	if err != nil {
		cancel()
		return nil, err
	}
	received := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		received <- err
	}()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case err := <-received:
		if err != nil {
			cancel()
			if errors.Is(err, io.EOF) {
				return nil, errEndedNoEvents
			}
			return nil, err
		}
		return cancel, nil
	case <-timer.C:
		cancel()
		return nil, fmt.Errorf("%w within %v", errNoEvent, wait)
	}
}

// checkInputStream opens the stream of the instance and waits for an event. While it is open, a
// second stream of the instance must be refused (one stream per instance, ALREADY_EXISTS in the
// protocol). Then the first stream is cancelled: it has ended once a new stream of the instance
// is served, since until then the plugin refuses it.
func (s *suite) checkInputStream(sess *session, instance string) {
	const name = "input.stream"
	client := pb.NewInputClient(sess.conn)
	cancel, err := s.firstEvent(client, instance, s.timeout)
	if err != nil {
		s.fail(name, "the stream did not start: %v", err)
		return
	}
	cancelSecond, err := s.firstEvent(client, instance, s.timeout)
	switch {
	case err == nil:
		cancelSecond()
		cancel()
		s.fail(name, "a second stream of the instance was served while the first was open; the protocol allows one "+
			"stream per instance and refuses another with ALREADY_EXISTS")
		return
	case errors.Is(err, errNoEvent) || errors.Is(err, errEndedNoEvents):
		cancel()
		s.fail(name, "a second stream of the instance, opened while the first was open, was not refused (%v); want "+
			"ALREADY_EXISTS", err)
		return
	}
	refusal := status.Code(err)
	cancel()
	cancelled := time.Now()

	deadline := time.NewTimer(s.timeout)
	defer deadline.Stop()
	retry := time.NewTicker(50 * time.Millisecond)
	defer retry.Stop()
	for {
		cancelAgain, err := s.firstEvent(client, instance, max(time.Until(cancelled.Add(s.timeout)), time.Millisecond))
		if err == nil {
			cancelAgain()
			s.pass(name, "started, a second stream refused with %s, served again %v after the cancellation", refusal,
				time.Since(cancelled).Round(time.Millisecond))
			return
		}
		select {
		case <-deadline.C:
			s.fail(name, "the stream did not end on cancellation: a new stream of the instance was not served within %v (last: %v)",
				s.timeout, err)
			return
		case <-s.ctx.Done():
			s.fail(name, "the run was cancelled: %v", s.ctx.Err())
			return
		case <-retry.C:
		}
	}
}

// checkInputProbe probes the instance and expects one sample per point of the first valid fixture.
func (s *suite) checkInputProbe(sess *session, instance string) {
	const name = "input.probe"
	cfg, err := sdk.DecodeInput(s.fixtures.Input.Valid[0])
	if err != nil {
		s.fail(name, "the first valid input fixture cannot be decoded: %v", err)
		return
	}
	want := map[[2]string]bool{}
	for _, device := range cfg.Devices {
		for _, point := range device.Points {
			want[[2]string{device.Device, point.Signal}] = true
		}
	}
	ctx, cancel := context.WithTimeout(s.ctx, probeDeadline)
	defer cancel()
	started := time.Now()
	resp, err := pb.NewInputClient(sess.conn).Probe(ctx, &pb.ProbeRequest{InstanceId: instance})
	elapsed := time.Since(started).Round(time.Millisecond)
	if status.Code(err) == codes.DeadlineExceeded {
		s.fail(name, "Probe did not answer within its %v deadline", probeDeadline)
		return
	}
	if err != nil {
		s.fail(name, "Probe: %v", err)
		return
	}
	got := map[[2]string]int{}
	var problems []string
	for _, sample := range resp.GetSamples() {
		key := [2]string{sample.GetDevice(), sample.GetSignal()}
		if !want[key] {
			problems = append(problems, fmt.Sprintf("a sample of %s/%s, which is not a configured point", key[0], key[1]))
			continue
		}
		got[key]++
	}
	for key := range want {
		if got[key] != 1 {
			problems = append(problems, fmt.Sprintf("%d samples of point %s/%s, want 1", got[key], key[0], key[1]))
		}
	}
	if len(problems) > 0 {
		slices.Sort(problems)
		s.fail(name, "%s", strings.Join(problems, "; "))
		return
	}
	s.pass(name, "%d samples for %d points in %v", len(resp.GetSamples()), len(want), elapsed)
}

// checkProcessorSignals feeds the processor the records of the first valid fixture, or synthetic
// records when it has none (see syntheticRecords), ends its side of the stream and collects the
// samples until the stream ends or the timeout passes. The suite opens one Process stream only.
func (s *suite) checkProcessorSignals(sess *session, instance string) {
	const name = "processor.signals"
	cfg, err := sdk.DecodeProcessor(s.fixtures.Processor.Valid[0])
	if err != nil {
		s.fail(name, "the first valid processor fixture cannot be decoded: %v", err)
		return
	}
	declared := map[[2]string]bool{}
	var signals []string
	for _, device := range cfg.Devices {
		for _, signal := range device.Signals {
			declared[[2]string{device.Device, signal.Signal}] = true
			if !slices.Contains(signals, signal.Signal) {
				signals = append(signals, signal.Signal)
			}
		}
	}
	if len(cfg.Observe) == 0 || len(declared) == 0 {
		s.fail(name, "the first valid processor fixture observes no device or declares no signal")
		return
	}
	base := time.Now().UTC()
	records := syntheticRecords(base, cfg.Observe, signals)
	source := "synthetic records (births, then integer samples 1, 2, 3 of the declared signal names on the observed devices)"
	if s.processorRecords != nil {
		records = make([]*pb.Record, 0, len(s.processorRecords))
		for i, r := range s.processorRecords {
			record, err := r.toProto(base, i)
			if err != nil {
				s.fail(name, "records[%d]: %v", i, err)
				return
			}
			records = append(records, record)
		}
		source = "the records of conformance.json"
	}

	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	stream, err := pb.NewProcessorClient(sess.conn).Process(ctx)
	if err != nil {
		s.fail(name, "Process: %v", err)
		return
	}
	type result struct {
		samples []*pb.Sample
		err     error
	}
	collected := make(chan result, 1)
	go func() {
		var r result
		for {
			out, err := stream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					r.err = err
				}
				collected <- r
				return
			}
			r.samples = append(r.samples, out.GetSample())
		}
	}()

	sendErr := sendRecords(stream, instance, records)
	r := <-collected
	var problems []string
	for _, sample := range r.samples {
		key := [2]string{sample.GetDevice(), sample.GetSignal()}
		if !declared[key] {
			problems = append(problems, fmt.Sprintf("%s/%s is not declared", key[0], key[1]))
		}
	}
	slices.Sort(problems)
	problems = slices.Compact(problems)
	nothing := "so nothing could be checked"
	if s.processorRecords == nil {
		nothing += `; add a "records" list to the first valid processor fixture of conformance.json with records ` +
			"the processor answers"
	}
	switch {
	case len(problems) > 0:
		s.fail(name, "the processor emitted undeclared signals: %s", strings.Join(problems, "; "))
	case r.err != nil && status.Code(r.err) != codes.DeadlineExceeded:
		s.fail(name, "the stream failed: %v", r.err)
	case errors.Is(sendErr, io.EOF):
		s.fail(name, "the processor ended the stream before the records were sent")
	case sendErr != nil:
		s.fail(name, "sending the records: %v", sendErr)
	case len(r.samples) == 0 && r.err == nil:
		s.fail(name, "the stream ended without a sample for %s, %s", source, nothing)
	case len(r.samples) == 0:
		s.fail(name, "no sample within %v for %s, %s", s.timeout, source, nothing)
	default:
		s.pass(name, "%d samples, all of declared signals, for %s", len(r.samples), source)
	}
}

// sendRecords starts the stream on the instance, sends the records and ends its side.
func sendRecords(stream pb.Processor_ProcessClient, instance string, records []*pb.Record) error {
	err := stream.Send(&pb.ProcessorIn{Msg: &pb.ProcessorIn_Start{Start: &pb.ProcessStart{InstanceId: instance}}})
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := stream.Send(&pb.ProcessorIn{Msg: &pb.ProcessorIn_Record{Record: record}}); err != nil {
			return err
		}
	}
	return stream.CloseSend()
}

// checkOutputSend sends one small valid batch twice under the same message_id: the first answer
// is checked by output.send, both by output.duplicate, which is not run when the first Send
// failed with an error.
func (s *suite) checkOutputSend(sess *session, instance string) {
	now := time.Now().UTC()
	req := &pb.SendRequest{InstanceId: instance, Batch: &pb.Batch{
		MessageId: newMessageID(now),
		SourceSeq: 1,
		Records: []*pb.Record{{Item: &pb.Record_Sample{Sample: &pb.Sample{
			Device: "conformance-device", Signal: "conformance-signal", Ts: timestamppb.New(now),
			Value:   &pb.Value{Kind: &pb.Value_IntValue{IntValue: 1}},
			Quality: pb.Quality_QUALITY_GOOD, Priority: pb.Priority_PRIORITY_CRITICAL,
		}}}},
	}}
	client := pb.NewOutputClient(sess.conn)
	send := func() (*pb.SendResult, error) {
		ctx, cancel := context.WithTimeout(s.ctx, sendDeadline)
		defer cancel()
		return client.Send(ctx, req)
	}
	first, firstErr := send()
	switch {
	case firstErr != nil:
		s.fail("output.send", "Send: %v", firstErr)
		s.notRun("the first Send failed", "output.duplicate")
		return
	case first.GetOutcome() == pb.Outcome_OUTCOME_UNSPECIFIED || pb.Outcome_name[int32(first.GetOutcome())] == "":
		s.fail("output.send", "Send answered the outcome %d, which is not one of the six", first.GetOutcome())
	default:
		s.pass("output.send", "%s", outcomeName(first.GetOutcome()))
	}

	second, secondErr := send()
	confirmed := func(r *pb.SendResult, err error) bool {
		return err == nil && (r.GetOutcome() == pb.Outcome_OUTCOME_ACCEPTED || r.GetOutcome() == pb.Outcome_OUTCOME_DUPLICATE)
	}
	describe := func(r *pb.SendResult, err error) string {
		if err != nil {
			return "the error " + err.Error()
		}
		return outcomeName(r.GetOutcome())
	}
	if !confirmed(first, firstErr) || !confirmed(second, secondErr) {
		s.fail("output.duplicate", "the same message_id twice was answered %s, then %s; want ACCEPTED or DUPLICATE both times",
			describe(first, firstErr), describe(second, secondErr))
		return
	}
	s.pass("output.duplicate", "%s, then %s", outcomeName(first.GetOutcome()), outcomeName(second.GetOutcome()))
}

// outcomeName is the name of an outcome without the enum prefix, as the design spec writes it.
func outcomeName(o pb.Outcome) string {
	return strings.TrimPrefix(o.String(), "OUTCOME_")
}

// newMessageID returns a UUIDv7 for now.
func newMessageID(now time.Time) string {
	var id [16]byte
	var millis [8]byte
	binary.BigEndian.PutUint64(millis[:], uint64(now.UnixMilli()))
	copy(id[:6], millis[2:])
	_, _ = rand.Read(id[6:]) // crypto/rand.Read never returns an error
	id[6] = 0x70 | id[6]&0x0f
	id[8] = 0x80 | id[8]&0x3f
	h := hex.EncodeToString(id[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// checkShutdown asks the plugin to stop within shutdownDeadline and waits for its exit. A health
// check right before proves the plugin was still running, so one that exited on its own does not
// pass. The plugin may exit before its answer is flushed, so an UNAVAILABLE answer or an EOF
// counts as success.
func (s *suite) checkShutdown(sess *session) {
	const name = "shutdown"
	healthCtx, healthCancel := context.WithTimeout(s.ctx, healthDeadline)
	_, err := healthpb.NewHealthClient(sess.conn).Check(healthCtx, &healthpb.HealthCheckRequest{})
	healthCancel()
	if err != nil {
		s.fail(name, "before Shutdown the plugin did not answer a health check: %v", err)
		return
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(s.ctx, shutdownDeadline+exitGrace)
	defer cancel()
	_, err = pb.NewPluginClient(sess.conn).Shutdown(ctx, &pb.ShutdownRequest{Deadline: durationpb.New(shutdownDeadline)})
	if err != nil && status.Code(err) != codes.Unavailable && !errors.Is(err, io.EOF) {
		s.fail(name, "Shutdown: %v", err)
		return
	}
	exited := make(chan error, 1)
	go func() { exited <- sess.proc.Wait() }()
	timer := time.NewTimer(time.Until(started.Add(shutdownDeadline + exitGrace)))
	defer timer.Stop()
	// The connection to an exited plugin is useless; on Windows it would also hold the old pipe.
	defer func() { _ = sess.conn.Close() }()
	select {
	case err := <-exited:
		elapsed := time.Since(started).Round(time.Millisecond)
		if err != nil {
			s.fail(name, "the plugin exited after Shutdown with %v; want exit code 0", err)
			return
		}
		s.pass(name, "exited 0 after %v", elapsed)
	case <-timer.C:
		_ = sess.proc.Kill()
		s.fail(name, "the plugin was still running %v after Shutdown with a %v deadline", shutdownDeadline+exitGrace,
			shutdownDeadline)
	case <-s.ctx.Done():
		s.fail(name, "the run was cancelled: %v", s.ctx.Err())
	}
}

// checkRestart launches the plugin, connects, kills it as kill -9 does and launches it again with
// the same socket directory. The killed plugin cannot remove its socket: the host removes the
// leftover *.sock files of the directory before the new launch (S4), which works when the plugin
// names its socket as S4 says.
func (s *suite) checkRestart() {
	const name = "restart"
	socketDir, err := s.tempDir()
	if err != nil {
		s.fail(name, "%v", err)
		return
	}
	first, err := s.launch(socketDir)
	if err != nil {
		s.fail(name, "the first launch: %v", err)
		return
	}
	oldAddress := first.proc.Address
	_ = first.conn.Close()
	if err := first.proc.Kill(); err != nil {
		s.fail(name, "killing the plugin: %v", err)
		return
	}
	_ = first.proc.Wait()

	second, err := s.launch(socketDir)
	if err != nil {
		s.fail(name, "the launch after the kill: %v", err)
		return
	}
	defer func() {
		_ = second.conn.Close()
		_ = second.proc.Kill()
		_ = second.proc.Wait()
	}()
	if second.proc.Address == oldAddress {
		s.fail(name, "after the kill the plugin listens again on %s; want a new address", oldAddress)
		return
	}
	if second.proc.Network == "unix" {
		if _, err := os.Lstat(oldAddress); !errors.Is(err, fs.ErrNotExist) {
			s.fail(name, "the old socket %s survived the relaunch: the host removes the leftover *.sock files of the "+
				"socket directory (S4), so the plugin must name its socket p<8 hex>.sock", oldAddress)
			return
		}
		s.pass(name, "new handshake on %s after the kill; the host removed the old socket", second.proc.Address)
		return
	}
	s.pass(name, "new handshake on %s after the kill", second.proc.Address)
}

// headBuffer keeps the first limit bytes written to it.
type headBuffer struct {
	limit int
	buf   bytes.Buffer
}

func (h *headBuffer) Write(p []byte) (int, error) {
	if room := h.limit - h.buf.Len(); room > 0 {
		h.buf.Write(p[:min(room, len(p))])
	}
	return len(p), nil
}

func (h *headBuffer) String() string { return h.buf.String() }
