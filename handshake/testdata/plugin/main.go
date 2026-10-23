// Command plugin is the test helper of package handshake: a plugin whose behaviour around the
// handshake is chosen through environment variables, since the host passes no arguments.
//
//	HELPER_SUPPORTED     comma-separated protocols the helper supports (default "v1")
//	HELPER_SLEEP=1       block before the handshake (also used for the helper's own child)
//	HELPER_CRASH=1       write 30 numbered stderr lines and exit 7 before the handshake
//	HELPER_STRAY=<text>  print <text> on stdout before the handshake
//	HELPER_LINE=<text>   print <text> instead of a real handshake, then block
//	HELPER_AFTER_STDOUT  a line printed on stdout after the handshake
//	HELPER_AFTER_STDERR  a line printed on stderr after the handshake
//	HELPER_ECHO_ENV=1    after the handshake print env=<KEY> on stdout for every variable
//	HELPER_CHILD=1       after the handshake start a blocked child of this helper, print child=<pid>
//	HELPER_HOLD_OUTPUT=1 after the handshake start a child outside the helper's process group
//	                     (Unix) that shares its stdout and floods it, print child=<pid>
//	HELPER_FLOOD=1       write lines on stdout until stdout breaks (the HELPER_HOLD_OUTPUT child)
//	HELPER_NO_SERVE=1    after the handshake block without serving
//	HELPER_EXIT_ON_CALL=1 exit 0 on the first gRPC call instead of answering it
//
// After the handshake the helper serves grpc.health.v1.Health (SERVING) until it is killed.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/getlaraplate/laraedge-sdk/handshake"
)

func main() {
	if os.Getenv("HELPER_SLEEP") == "1" {
		block()
	}
	if os.Getenv("HELPER_FLOOD") == "1" {
		flood()
	}
	if os.Getenv("HELPER_CRASH") == "1" {
		for i := 1; i <= 30; i++ {
			fmt.Fprintf(os.Stderr, "crash line %d\n", i)
		}
		os.Exit(7)
	}
	if line := os.Getenv("HELPER_LINE"); line != "" {
		fmt.Println(line)
		block()
	}
	if stray := os.Getenv("HELPER_STRAY"); stray != "" {
		fmt.Println(stray)
	}

	supported := "v1"
	if value := os.Getenv("HELPER_SUPPORTED"); value != "" {
		supported = value
	}
	listener, _, err := handshake.Listen(strings.Split(supported, ","))
	if errors.Is(err, handshake.ErrNoProtocol) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(handshake.ExitCodeNoProtocol)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if text := os.Getenv("HELPER_AFTER_STDOUT"); text != "" {
		fmt.Println(text)
	}
	if text := os.Getenv("HELPER_AFTER_STDERR"); text != "" {
		fmt.Fprintln(os.Stderr, text)
	}
	if os.Getenv("HELPER_ECHO_ENV") == "1" {
		for _, entry := range os.Environ() {
			key, _, _ := strings.Cut(entry, "=")
			fmt.Printf("env=%s\n", key)
		}
	}
	if os.Getenv("HELPER_CHILD") == "1" {
		startChild()
	}
	if os.Getenv("HELPER_HOLD_OUTPUT") == "1" {
		startOutputHolder()
	}
	if os.Getenv("HELPER_NO_SERVE") == "1" {
		block()
	}

	var options []grpc.ServerOption
	if os.Getenv("HELPER_EXIT_ON_CALL") == "1" {
		options = append(options, grpc.UnaryInterceptor(
			func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
				os.Exit(0)
				return nil, nil
			}))
	}
	server := grpc.NewServer(options...)
	healthpb.RegisterHealthServer(server, health.NewServer())
	if err := server.Serve(listener); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// startChild starts this helper again, blocked before its handshake, and prints its pid.
func startChild() {
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	child := exec.Command(self)
	child.Env = append(os.Environ(), "HELPER_SLEEP=1")
	if err := child.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("child=%d\n", child.Process.Pid)
}

// startOutputHolder starts this helper again as a flooding child that shares the helper's
// stdout and, on Unix, leaves the helper's process group, and prints its pid.
func startOutputHolder() {
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	child := exec.Command(self)
	child.Env = append(os.Environ(), "HELPER_FLOOD=1")
	child.Stdout = os.Stdout
	detach(child)
	if err := child.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("child=%d\n", child.Process.Pid)
}

// flood writes stdout lines until a write fails.
func flood() {
	for {
		if _, err := fmt.Println("flood"); err != nil {
			os.Exit(0)
		}
	}
}

// block keeps the helper alive until it is killed.
func block() {
	for {
		time.Sleep(time.Hour)
	}
}
