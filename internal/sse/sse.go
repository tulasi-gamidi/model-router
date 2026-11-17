// Package sse reads and writes Server-Sent Events without buffering whole streams.
package sse

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// MaxLineSize is the largest single SSE line accepted from an upstream.
const MaxLineSize = 1 << 20

// Event is one dispatched SSE event.
type Event struct {
	Name string // value of the "event:" field, empty if absent
	Data string // "data:" lines joined with "\n"
}

// Reader parses an SSE byte stream one event at a time. Not safe for concurrent use.
type Reader struct {
	sc *bufio.Scanner
}

// NewReader returns a Reader over r with the line buffer raised to MaxLineSize.
func NewReader(r io.Reader) *Reader {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), MaxLineSize)
	return &Reader{sc: sc}
}

// Next returns the next event. It returns io.EOF when the stream ends cleanly; any other error
// (including a line over MaxLineSize or an aborted connection) means the stream was cut off.
// A final event not followed by a blank line is still dispatched at EOF: a truncated payload
// fails JSON decoding or terminator matching in the adapter, so leniency here is safe.
func (r *Reader) Next() (Event, error) {
	var (
		ev      Event
		data    strings.Builder
		hasData bool
	)
	for r.sc.Scan() {
		line := r.sc.Text() // ScanLines strips "\n" and a trailing "\r"
		if line == "" {
			if hasData {
				ev.Data = data.String()
				return ev, nil
			}
			ev = Event{} // event with no data: ignore per spec
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue // comment / keep-alive
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.WriteString(value)
			hasData = true
		case "event":
			ev.Name = value
		default:
			// "id", "retry" and unknown fields are irrelevant to us.
		}
	}
	if err := r.sc.Err(); err != nil {
		return Event{}, fmt.Errorf("reading sse stream: %w", err)
	}
	if hasData {
		ev.Data = data.String()
		return ev, nil
	}
	return Event{}, io.EOF
}

// DoneData is the OpenAI stream terminator payload.
const DoneData = "[DONE]"

// WriteData writes one "data: <payload>\n\n" event. payload must not contain newlines
// (json.Marshal output never does).
func WriteData(w io.Writer, payload []byte) error {
	buf := make([]byte, 0, len(payload)+8)
	buf = append(buf, "data: "...)
	buf = append(buf, payload...)
	buf = append(buf, '\n', '\n')
	_, err := w.Write(buf)
	return err
}

// WriteDone writes the "data: [DONE]" terminator.
func WriteDone(w io.Writer) error {
	return WriteData(w, []byte(DoneData))
}
