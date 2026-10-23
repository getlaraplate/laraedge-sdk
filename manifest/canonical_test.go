package manifest_test

import (
	"testing"

	"github.com/getlaraplate/laraedge-sdk/manifest"
)

func TestCanonical(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"sorts keys at every depth", `{"b":1,"a":{"z":[{"y":1,"x":2}],"c":null}}`, `{"a":{"c":null,"z":[{"x":2,"y":1}]},"b":1}`},
		{"drops whitespace", "{ \"a\" :\n [ 1 , 2 ] }\n", `{"a":[1,2]}`},
		{"keeps numbers as written", `{"a":1.0,"b":1e3,"c":-0.50,"d":12345678901234567890}`, `{"a":1.0,"b":1e3,"c":-0.50,"d":12345678901234567890}`},
		{"keeps UTF-8 unescaped", `{"k":"café € <&>"}`, `{"k":"café € <&>"}`},
		{"escapes what JSON requires", `{"k":"a\"b\\c\n\u0001"}`, `{"k":"a\"b\\c\n\u0001"}`},
		{"orders keys by UTF-8 bytes", `{"b":1,"B":2,"é":3,"a":4}`, `{"B":2,"a":4,"b":1,"é":3}`},
		{"scalars", `true`, `true`},
		{"empty containers", `{"a":{},"b":[]}`, `{"a":{},"b":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := manifest.Canonical([]byte(tt.in))
			if err != nil {
				t.Fatalf("Canonical: %v", err)
			}
			if string(got) != tt.want {
				t.Fatalf("got  %s\nwant %s", got, tt.want)
			}
			again, err := manifest.Canonical(got)
			if err != nil || string(again) != string(got) {
				t.Fatalf("not idempotent: %s, %v", again, err)
			}
		})
	}
}

func TestCanonicalRefuses(t *testing.T) {
	tests := []struct{ name, in string }{
		{"empty", ``},
		{"truncated", `{"a":`},
		{"not json", `nope`},
		{"duplicate key", `{"a":1,"a":2}`},
		{"trailing data", `{"a":1} {"b":2}`},
		{"trailing garbage", `{"a":1} x`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := manifest.Canonical([]byte(tt.in)); err == nil {
				t.Fatalf("want an error, got %s", got)
			}
		})
	}
}

func TestCanonicalRefusesLossyText(t *testing.T) {
	tests := []struct{ name, in string }{
		{"invalid UTF-8 byte", "{\"a\":\"\xff\"}"},
		{"invalid UTF-8 in a key", "{\"\xc3\":1}"},
		{"lone high surrogate", `{"a":"\ud800"}`},
		{"lone low surrogate", `{"a":"\udc00"}`},
		{"high surrogate then other escape", `{"a":"\ud800A"}`},
		{"high surrogate then text", `{"a":"\ud83dx"}`},
		{"unpaired in a key", `{"\udfff":1}`},
		{"surrogate after an escaped backslash", `{"a":"\\\ud800"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := manifest.Canonical([]byte(tt.in)); err == nil {
				t.Fatalf("want an error, got %s", got)
			}
		})
	}
}

func TestCanonicalKeepsValidSurrogatesAndLookalikes(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"valid pair", `{"a":"😀"}`, `{"a":"😀"}`},
		{"escaped backslash before u", `{"a":"\\ud800"}`, `{"a":"\\ud800"}`},
		{"surrogate text outside strings is not scanned", `{"a":"x"}`, `{"a":"x"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := manifest.Canonical([]byte(tt.in))
			if err != nil || string(got) != tt.want {
				t.Fatalf("got %s, %v; want %s", got, err, tt.want)
			}
		})
	}
}
