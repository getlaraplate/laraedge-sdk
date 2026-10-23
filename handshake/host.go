package handshake

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// DefaultTimeout is the handshake timeout when [LaunchConfig.Timeout] is not set.
const DefaultTimeout = 10 * time.Second

const (
	// stderrTailLines and stderrTailBytes bound the stderr lines kept for an [ExitError].
	stderrTailLines = 20
	stderrTailBytes = 4096
	// maxLineBytes bounds the handshake line the host reads.
	maxLineBytes = 4096
	// quoteLimit is how many characters of an unexpected line an error quotes.
	quoteLimit = 80
	// outputDrainTimeout bounds how long Wait waits, after the exit, for the plugin's output
	// pipes to drain; a descendant may keep them open.
	outputDrainTimeout = 2 * time.Second
)

var (
	// ErrBadHandshakeLine reports a first stdout line that is not a valid handshake line for
	// this host: another text, a protocol not offered, the other platform's transport, or an
	// address outside the socket directory.
	ErrBadHandshakeLine = errors.New("bad handshake line")
	// ErrHandshakeTimeout reports a plugin that printed no handshake line within the timeout.
	ErrHandshakeTimeout = errors.New("no handshake line within the timeout")
	// ErrExited reports that the plugin exited while [Process.Dial] waited for the connection;
	// the error also wraps the plugin's exit status when it is not a clean exit.
	ErrExited = errors.New("the plugin exited")
)

// ExitError reports a plugin that exited before completing the handshake. It matches
// [ErrNoProtocol] when the exit code is [ExitCodeNoProtocol].
type ExitError struct {
	// ExitCode is the plugin's exit code, -1 when it was ended by a signal.
	ExitCode int
	// Stderr holds the plugin's last stderr lines, oldest first.
	Stderr []string
}

func (e *ExitError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "handshake: the plugin exited before the handshake with exit code %d", e.ExitCode)
	if e.ExitCode == ExitCodeNoProtocol {
		b.WriteString(" (no protocol version in common with the host)")
	}
	if len(e.Stderr) > 0 {
		b.WriteString("; last stderr lines: ")
		for i, line := range e.Stderr {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(strconv.Quote(line))
		}
	}
	return b.String()
}

// Is reports whether target is [ErrNoProtocol] and the plugin exited with
// [ExitCodeNoProtocol].
func (e *ExitError) Is(target error) bool {
	return target == ErrNoProtocol && e.ExitCode == ExitCodeNoProtocol
}

// LaunchConfig describes how to start a plugin.
type LaunchConfig struct {
	// Path is the plugin binary. It runs directly, never through a shell; a relative path is
	// resolved against the host's working directory, never looked up in PATH.
	Path string
	// Dir is the plugin's working directory; empty means the directory of Path.
	Dir string
	// SocketDir is the directory of the plugin's Unix socket (Unix only). It is created 0700
	// when missing, narrowed to 0700 when wider, and refused when it is a symbolic link or
	// belongs to another user. Leftover *.sock files in it are removed before the start.
	SocketDir string
	// Protocols are the protocol versions the host offers, preferred first.
	Protocols []string
	// Env is the plugin's whole environment besides the handshake variables, such as PATH,
	// TZ and, on Windows, SystemRoot. The host's own environment is never passed.
	Env []string
	// Stdout receives the plugin's stdout after the handshake line, Stderr all its stderr.
	// Nil discards. Each is written from its own goroutine; a writer shared by both must be
	// safe for concurrent use. A writer that fails is replaced by a discard.
	Stdout, Stderr io.Writer
	// Timeout bounds the wait for the handshake line; zero means [DefaultTimeout].
	Timeout time.Duration
}

func (c LaunchConfig) validate() error {
	if c.Path == "" {
		return errors.New("handshake: LaunchConfig.Path is empty")
	}
	if len(c.Protocols) == 0 {
		return errors.New("handshake: LaunchConfig.Protocols is empty")
	}
	for _, protocol := range c.Protocols {
		if protocol == "" || strings.ContainsAny(protocol, ",| \t\r\n") {
			return fmt.Errorf("handshake: invalid protocol %q in LaunchConfig.Protocols", protocol)
		}
	}
	return nil
}

// Process is a plugin that completed the handshake.
type Process struct {
	// Protocol is the negotiated protocol version.
	Protocol string
	// Network is "unix" or "npipe"; Address is the socket path or the pipe name.
	Network, Address string
	// Pid is the plugin's process id.
	Pid int

	cmd  *exec.Cmd
	tree *processTree

	mu     sync.Mutex // guards reaped against Signal and Kill
	reaped bool

	output     sync.WaitGroup // the stdout and stderr copiers
	stdoutRead *os.File
	stderrRead *os.File

	done    chan struct{} // closed once the process is reaped and its output drained
	waitErr error
}

// Launch starts the plugin and completes the host side of the handshake. ctx bounds the
// handshake only, not the life of the plugin. On any failure the plugin is killed and reaped
// before Launch returns: a plugin that exits first yields an [ExitError], one that prints
// another first line [ErrBadHandshakeLine], one that prints nothing in time
// [ErrHandshakeTimeout].
func Launch(ctx context.Context, cfg LaunchConfig) (*Process, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("handshake: %w", err)
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	path, err := filepath.Abs(cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("handshake: plugin path: %w", err)
	}
	dir := cfg.Dir
	if dir == "" {
		dir = filepath.Dir(path)
	}
	socketDir, err := prepareSocketDir(cfg.SocketDir)
	if err != nil {
		return nil, err
	}

	// A nil Env would make exec pass the host's whole environment; this list is never nil.
	env := append(slices.Clone(cfg.Env),
		CookieKey+"="+CookieValue,
		ProtocolsKey+"="+strings.Join(cfg.Protocols, ","),
	)
	if socketDir != "" {
		env = append(env, SocketDirKey+"="+socketDir)
	}

	p, err := start(path, dir, env)
	if err != nil {
		return nil, err
	}
	tail := &tailBuffer{}
	p.output.Add(2)
	go p.copyStderr(cfg.Stderr, tail)
	lines := make(chan stdoutLine, 1)
	go p.copyStdout(cfg.Stdout, lines)
	go p.await()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case first := <-lines:
		if first.err != nil {
			// Stdout ended without a line: the plugin is exiting, or it closed stdout.
			select {
			case <-p.done:
				return nil, p.exitError(tail)
			case <-timer.C:
				p.abort()
				return nil, fmt.Errorf("handshake: %w (%v): the plugin closed stdout", ErrHandshakeTimeout, timeout)
			case <-ctx.Done():
				p.abort()
				return nil, fmt.Errorf("handshake: %w", ctx.Err())
			}
		}
		if err := p.accept(first.text, cfg.Protocols, socketDir); err != nil {
			p.abort()
			return nil, err
		}
		return p, nil
	case <-p.done:
		return nil, p.exitError(tail)
	case <-timer.C:
		p.abort()
		return nil, fmt.Errorf("handshake: %w (%v)", ErrHandshakeTimeout, timeout)
	case <-ctx.Done():
		p.abort()
		return nil, fmt.Errorf("handshake: %w", ctx.Err())
	}
}

// start runs the binary in its process tree with fresh output pipes.
func start(path, dir string, env []string) (*Process, error) {
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("handshake: stdout pipe: %w", err)
	}
	stderrRead, stderrWrite, err := os.Pipe()
	if err != nil {
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		return nil, fmt.Errorf("handshake: stderr pipe: %w", err)
	}
	closeReads := func() {
		_ = stdoutRead.Close()
		_ = stderrRead.Close()
	}
	cmd := &exec.Cmd{Path: path, Args: []string{path}, Dir: dir, Env: env, Stdout: stdoutWrite, Stderr: stderrWrite}
	tree, err := newProcessTree(cmd)
	if err != nil {
		closeReads()
		_ = stdoutWrite.Close()
		_ = stderrWrite.Close()
		return nil, fmt.Errorf("handshake: process tree: %w", err)
	}
	err = startCommand(cmd)
	// The child holds its own copies; the host keeps only the read ends.
	_ = stdoutWrite.Close()
	_ = stderrWrite.Close()
	if err != nil {
		closeReads()
		tree.release()
		return nil, fmt.Errorf("handshake: starting the plugin: %w", err)
	}
	if err := tree.attach(cmd.Process); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		closeReads()
		tree.release()
		return nil, fmt.Errorf("handshake: process tree: %w", err)
	}
	return &Process{
		Pid:        cmd.Process.Pid,
		cmd:        cmd,
		tree:       tree,
		stdoutRead: stdoutRead,
		stderrRead: stderrRead,
		done:       make(chan struct{}),
	}, nil
}

// stdoutLine is the first stdout line, or why there is none.
type stdoutLine struct {
	text string
	err  error
}

// copyStdout delivers the first stdout line and copies the rest to w.
func (p *Process) copyStdout(w io.Writer, first chan<- stdoutLine) {
	defer p.output.Done()
	reader := bufio.NewReaderSize(p.stdoutRead, maxLineBytes)
	line, err := reader.ReadSlice('\n')
	switch {
	case err == nil, errors.Is(err, bufio.ErrBufferFull):
		first <- stdoutLine{text: string(line)}
	default:
		first <- stdoutLine{err: err}
	}
	_, _ = io.Copy(&tolerantWriter{w: w}, reader)
}

// copyStderr copies stderr to w and keeps its tail.
func (p *Process) copyStderr(w io.Writer, tail *tailBuffer) {
	defer p.output.Done()
	_, _ = io.Copy(io.MultiWriter(&tolerantWriter{w: w}, tail), p.stderrRead)
}

// accept checks the handshake line and fills the process's transport fields.
func (p *Process) accept(line string, offered []string, socketDir string) error {
	text := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	fields := strings.SplitN(text, "|", 4)
	if len(fields) != 4 || fields[0] != lineVersion || !strings.HasSuffix(line, "\n") {
		return fmt.Errorf("handshake: %w: the plugin's first stdout line is %s", ErrBadHandshakeLine, quoteStart(text))
	}
	protocol, network, address := fields[1], fields[2], fields[3]
	if !slices.Contains(offered, protocol) {
		return fmt.Errorf("handshake: %w: protocol %s was not offered by the host", ErrBadHandshakeLine, quoteStart(protocol))
	}
	if network != transportNetwork {
		return fmt.Errorf("handshake: %w: transport %s, this platform uses %q", ErrBadHandshakeLine, quoteStart(network),
			transportNetwork)
	}
	address, err := verifyAddress(socketDir, address)
	if err != nil {
		return fmt.Errorf("handshake: %w: %v", ErrBadHandshakeLine, err)
	}
	p.Protocol, p.Network, p.Address = protocol, network, address
	return nil
}

// await reaps the plugin, ends its tree, drains its output and closes done.
func (p *Process) await() {
	var err error
	if awaitExit(p.Pid) {
		// The exited process is not reaped yet, so its pid cannot be reused: reaping under the
		// lock keeps Kill from signalling a reused process group. Meanwhile the zombie still
		// reserves the group id, so the descendants the plugin left behind are killed safely,
		// as closing the job does on Windows.
		p.mu.Lock()
		_ = p.tree.kill(p.Pid)
		err = p.cmd.Wait()
		p.reaped = true
		p.tree.release()
		p.mu.Unlock()
	} else {
		err = p.cmd.Wait()
		p.mu.Lock()
		p.reaped = true
		p.tree.release()
		p.mu.Unlock()
	}
	p.waitErr = err
	p.drainOutput()
	close(p.done)
}

// drainOutput waits for the output copiers, closes the pipes, and waits for the copiers again
// so that no write to the caller's writers follows Wait. A descendant holding the pipes open
// only delays it: closing the read ends ends the copiers' reads. Each wait is bounded by
// outputDrainTimeout, as a last resort where a close cannot end a blocked read.
func (p *Process) drainOutput() {
	drained := make(chan struct{})
	go func() {
		p.output.Wait()
		close(drained)
	}()
	timer := time.NewTimer(outputDrainTimeout)
	defer timer.Stop()
	select {
	case <-drained:
	case <-timer.C:
	}
	_ = p.stdoutRead.Close()
	_ = p.stderrRead.Close()
	timer.Reset(outputDrainTimeout)
	select {
	case <-drained:
	case <-timer.C:
	}
}

// abort kills the plugin during a failed handshake and waits until it is reaped.
func (p *Process) abort() {
	_ = p.Kill()
	<-p.done
}

// exitError describes a plugin that exited before the handshake; done must be closed.
func (p *Process) exitError(tail *tailBuffer) error {
	code := -1
	if p.cmd.ProcessState != nil {
		code = p.cmd.ProcessState.ExitCode()
	}
	return &ExitError{ExitCode: code, Stderr: tail.snapshot()}
}

// Dial opens a gRPC connection to the plugin over its local transport, with message limits of
// [MaxMessageSize], and returns once the connection is ready, ctx ends, or the plugin exits
// ([ErrExited]).
func (p *Process) Dial(ctx context.Context) (*grpc.ClientConn, error) {
	address := p.Address
	if err := p.exitedError(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		select {
		case <-p.done:
			cancel(ErrExited)
		case <-ctx.Done():
		}
	}()
	conn, err := grpc.NewClient("passthrough:///localhost",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return dialTransport(ctx, address)
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(MaxMessageSize),
			grpc.MaxCallSendMsgSize(MaxMessageSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("handshake: gRPC client: %w", err)
	}
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn, nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			_ = conn.Close()
			if err := p.exitedError(); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("handshake: connecting to the plugin at %s (state %s): %w", address, state, ctx.Err())
		}
	}
}

// exitedError returns an error wrapping [ErrExited] and the exit status once the plugin has
// exited, nil before.
func (p *Process) exitedError() error {
	select {
	case <-p.done:
	default:
		return nil
	}
	if p.waitErr != nil {
		return fmt.Errorf("handshake: %w before the connection was ready: %w", ErrExited, p.waitErr)
	}
	return fmt.Errorf("handshake: %w before the connection was ready", ErrExited)
}

// Signal sends sig to the plugin process. On Windows only [os.Kill] is supported. After the
// process is reaped it returns [os.ErrProcessDone].
func (p *Process) Signal(sig os.Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reaped {
		return os.ErrProcessDone
	}
	return p.cmd.Process.Signal(sig)
}

// Kill ends the plugin and its descendants: its process group on Unix, its job object on
// Windows. After the process is reaped it returns [os.ErrProcessDone].
func (p *Process) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reaped {
		return os.ErrProcessDone
	}
	return p.tree.kill(p.Pid)
}

// Wait waits for the plugin to exit and its output to drain, and returns its exit status as
// [exec.Cmd.Wait] does. Every call returns the same result.
func (p *Process) Wait() error {
	<-p.done
	return p.waitErr
}

// quoteStart quotes at most the first quoteLimit characters of s.
func quoteStart(s string) string {
	if utf8.RuneCountInString(s) <= quoteLimit {
		return strconv.Quote(s)
	}
	runes := []rune(s)
	return strconv.Quote(string(runes[:quoteLimit])) + " (truncated)"
}

// tolerantWriter forwards to w until w fails, then discards: a failing caller's writer never
// blocks the plugin's output pipe.
type tolerantWriter struct {
	w      io.Writer
	failed bool
}

func (t *tolerantWriter) Write(b []byte) (int, error) {
	if t.w != nil && !t.failed {
		if _, err := t.w.Write(b); err != nil {
			t.failed = true
		}
	}
	return len(b), nil
}

// tailBuffer keeps the last stderrTailLines lines of stderr, stderrTailBytes at most.
type tailBuffer struct {
	mu      sync.Mutex
	lines   []string
	size    int
	partial []byte
}

func (t *tailBuffer) Write(b []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := len(b)
	for len(b) > 0 {
		end := bytes.IndexByte(b, '\n')
		if end < 0 {
			t.appendPartial(b)
			break
		}
		t.appendPartial(b[:end])
		t.push(strings.TrimSuffix(string(t.partial), "\r"))
		t.partial = t.partial[:0]
		b = b[end+1:]
	}
	return n, nil
}

func (t *tailBuffer) appendPartial(b []byte) {
	room := stderrTailBytes - len(t.partial)
	if len(b) > room {
		b = b[:room]
	}
	t.partial = append(t.partial, b...)
}

func (t *tailBuffer) push(line string) {
	t.lines = append(t.lines, line)
	t.size += len(line)
	for len(t.lines) > stderrTailLines || (t.size > stderrTailBytes && len(t.lines) > 1) {
		t.size -= len(t.lines[0])
		t.lines = t.lines[1:]
	}
}

// snapshot returns the kept lines, an unterminated last line included.
func (t *tailBuffer) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	lines := slices.Clone(t.lines)
	if len(t.partial) > 0 {
		lines = append(lines, strings.TrimSuffix(string(t.partial), "\r"))
	}
	return lines
}
