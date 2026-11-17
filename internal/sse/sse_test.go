package sse

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func readAll(t *testing.T, r *Reader) ([]Event, error) {
	t.Helper()
	var evs []Event
	for {
		ev, err := r.Next()
		if err != nil {
			return evs, err
		}
		evs = append(evs, ev)
	}
}

func TestReader(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []Event
	}{
		{
			name: "openai style",
			in:   "data: {\"a\":1}\n\ndata: [DONE]\n\n",
			want: []Event{{Data: `{"a":1}`}, {Data: "[DONE]"}},
		},
		{
			name: "named events anthropic style",
			in:   "event: message_start\ndata: {}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
			want: []Event{{Name: "message_start", Data: "{}"}, {Name: "message_stop", Data: `{"type":"message_stop"}`}},
		},
		{
			name: "comments and keepalives ignored",
			in:   ": ping\n\n: OPENROUTER PROCESSING\ndata: x\n\n",
			want: []Event{{Data: "x"}},
		},
		{
			name: "CRLF line endings",
			in:   "data: x\r\n\r\ndata: y\r\n\r\n",
			want: []Event{{Data: "x"}, {Data: "y"}},
		},
		{
			name: "multi-line data joined with newline",
			in:   "data: a\ndata: b\n\n",
			want: []Event{{Data: "a\nb"}},
		},
		{
			name: "no space after colon",
			in:   "data:x\n\n",
			want: []Event{{Data: "x"}},
		},
		{
			name: "id and retry fields ignored, event without data dropped",
			in:   "id: 1\nretry: 10\n\nevent: ping\n\ndata: z\n\n",
			want: []Event{{Data: "z"}},
		},
		{
			name: "final event without trailing blank line is dispatched",
			in:   "data: x\n\ndata: [DONE]",
			want: []Event{{Data: "x"}, {Data: "[DONE]"}},
		},
		{
			name: "empty stream",
			in:   "",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readAll(t, NewReader(strings.NewReader(tt.in)))
			if !errors.Is(err, io.EOF) {
				t.Fatalf("err = %v, want io.EOF", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d events %+v, want %d %+v", len(got), got, len(tt.want), tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("event %d = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestReaderLargeLineWithinLimit(t *testing.T) {
	big := strings.Repeat("a", 512*1024)
	got, err := readAll(t, NewReader(strings.NewReader("data: "+big+"\n\n")))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	if len(got) != 1 || len(got[0].Data) != len(big) {
		t.Fatalf("large event not read intact")
	}
}

func TestReaderLineTooLongIsError(t *testing.T) {
	big := strings.Repeat("a", MaxLineSize+1)
	_, err := readAll(t, NewReader(strings.NewReader("data: "+big+"\n\n")))
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want non-EOF error", err)
	}
}

type failingReader struct {
	data []byte
	err  error
}

func (f *failingReader) Read(p []byte) (int, error) {
	if len(f.data) == 0 {
		return 0, f.err
	}
	n := copy(p, f.data)
	f.data = f.data[n:]
	return n, nil
}

func TestReaderAbortedConnectionIsError(t *testing.T) {
	boom := errors.New("connection reset by peer")
	r := NewReader(&failingReader{data: []byte("data: x\n\ndata: partial"), err: boom})
	ev, err := r.Next()
	if err != nil || ev.Data != "x" {
		t.Fatalf("first event = %+v, %v", ev, err)
	}
	_, err = r.Next()
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want wrapped %v", err, boom)
	}
}

func TestWrite(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteData(&buf, []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := WriteDone(&buf); err != nil {
		t.Fatal(err)
	}
	want := "data: {\"a\":1}\n\ndata: [DONE]\n\n"
	if buf.String() != want {
		t.Fatalf("got %q, want %q", buf.String(), want)
	}
}
