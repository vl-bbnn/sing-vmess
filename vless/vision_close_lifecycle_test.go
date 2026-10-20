package vless

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
)

func lifetimeText(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}
func lifetimeRecord(row map[string]any) {
	b, _ := json.Marshal(row)
	fmt.Println("VLESS_LIFETIME_CASE " + string(b))
}
func lifetimeWait(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("original 5s worker join exceeded")
		return nil
	}
}

// This transport records protocol error propagation, independently of the real
// socket/deadline controls below. Injected errors are never treated as EOF.
type lifetimeFaultConn struct {
	net.Conn
	writeErr, closeErr error
	writes             [][]byte
	closes             int
}

func (c *lifetimeFaultConn) Write(p []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), p...))
	return 0, c.writeErr
}
func (c *lifetimeFaultConn) Close() error { c.closes++; return c.closeErr }

func TestVLESSServerAbortAndGracefulErrors(t *testing.T) {
	for _, abort := range []bool{true, false} {
		t.Run(fmt.Sprint(abort), func(t *testing.T) {
			we, ce := errors.New("injected response-header write failure"), errors.New("injected underlying close failure")
			raw := &lifetimeFaultConn{writeErr: we, closeErr: ce}
			c := &serverConn{ExtendedConn: bufio.NewExtendedConn(raw)}
			var e error
			if abort {
				e = c.Close()
			} else {
				e = c.CloseWrite()
			}
			if !errors.Is(e, ce) || errors.Is(e, we) == abort || raw.closes != 1 {
				t.Fatalf("error propagation abort=%v writes=%d closes=%d err=%v", abort, len(raw.writes), raw.closes, e)
			}
			if abort {
				if len(raw.writes) != 0 || c.responseWritten {
					t.Fatal("abort Close emitted header")
				}
			} else {
				if len(raw.writes) != 1 || !bytes.Equal(raw.writes[0], []byte{Version, 0}) {
					t.Fatal("graceful CloseWrite omitted exact response header")
				}
			}
			lifetimeRecord(map[string]any{"gate": "abort-or-graceful-errors", "abort": abort, "writes": raw.writes, "close_calls": raw.closes, "write_error_identity_retained": errors.Is(e, we), "close_error_identity_retained": errors.Is(e, ce), "returned_error": lifetimeText(e)})
		})
	}
}

type lifetimeEnteredConn struct {
	net.Conn
	once    sync.Once
	entered chan struct{}
}

func (c *lifetimeEnteredConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(p)
}

func TestVisionConcurrentAbortUnblocksWriteAndCloseWrite(t *testing.T) {
	for _, mode := range []string{"none-write", "none-buffer", "vision-padded", "vision-direct"} {
		t.Run(mode, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			if e := a.SetDeadline(time.Now().Add(5 * time.Second)); e != nil {
				t.Fatal(e)
			}
			tapped := &lifetimeEnteredConn{Conn: a, entered: make(chan struct{})}
			server := &serverConn{ExtendedConn: bufio.NewExtendedConn(tapped)}
			var writer net.Conn = server
			var half interface{ CloseWrite() error } = server
			if mode == "vision-padded" || mode == "vision-direct" {
				v := &VisionConn{Conn: server, netConn: tapped, directWrite: mode == "vision-direct"}
				v.closeRawOnDirectWrite.Store(mode == "vision-direct")
				writer = v
				half = v
			}
			writeDone, halfDone, closeDone := make(chan error, 1), make(chan error, 1), make(chan error, 1)
			go func() {
				if mode == "none-buffer" {
					buffer := buf.NewSize(64)
					buffer.Resize(2, 0)
					buffer.Write([]byte("blocked-payload"))
					writeDone <- server.WriteBuffer(buffer)
				} else {
					_, e := writer.Write([]byte("blocked-payload"))
					writeDone <- e
				}
			}()
			select {
			case <-tapped.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Write never reached transport")
			}
			ready := make(chan struct{})
			go func() { close(ready); halfDone <- half.CloseWrite() }()
			<-ready
			// Full Close must bypass both write mutexes and release the blocked writer.
			go func() { closeDone <- writer.Close() }()
			closeErr := lifetimeWait(t, closeDone)
			writeErr := lifetimeWait(t, writeDone)
			halfErr := lifetimeWait(t, halfDone)
			if closeErr != nil || !errors.Is(writeErr, io.ErrClosedPipe) {
				t.Fatalf("abort not retained close=%v write=%v half=%v", closeErr, writeErr, halfErr)
			}
			if mode != "vision-direct" && !errors.Is(halfErr, io.ErrClosedPipe) {
				t.Fatalf("blocked graceful writer error lost: %v", halfErr)
			}
			if mode == "vision-direct" && halfErr != nil {
				t.Fatalf("no-capability full close return changed: %v", halfErr)
			}
			// Read-only optimizations must also remain safe after the write mutex retires.
			if server.FrontHeadroom() != 0 && mode != "vision-direct" {
				t.Fatal("response header state missing after blocked write")
			}
			_ = server.WriterReplaceable()
			lifetimeRecord(map[string]any{"gate": "concurrent-abort", "mode": mode, "write_error": lifetimeText(writeErr), "CloseWrite_error": lifetimeText(halfErr), "Close_error": lifetimeText(closeErr), "all_workers_joined": true, "manually_constructed_Vision_state": mode == "vision-padded" || mode == "vision-direct", "actual_Reality_transition_not_claimed": true})
		})
	}
}

type lifetimeHalfFaultConn struct {
	*lifetimeFaultConn
	halfErr error
	halves  int
}

func (c *lifetimeHalfFaultConn) CloseWrite() error { c.halves++; return c.halfErr }
func TestVisionDirectCloseErrorRouting(t *testing.T) {
	for _, capable := range []bool{true, false} {
		t.Run(fmt.Sprint(capable), func(t *testing.T) {
			ce, he := errors.New("injected raw Close failure"), errors.New("injected raw CloseWrite failure")
			outer := &lifetimeFaultConn{closeErr: errors.New("unexpected outer Close")}
			raw := &lifetimeFaultConn{closeErr: ce}
			half := &lifetimeHalfFaultConn{lifetimeFaultConn: raw, halfErr: he}
			var transport net.Conn = raw
			if capable {
				transport = half
			}
			c := &VisionConn{Conn: outer, netConn: transport, directWrite: true}
			c.closeRawOnDirectWrite.Store(true)
			e := c.CloseWrite()
			expected := ce
			if capable {
				expected = he
			}
			if !errors.Is(e, expected) {
				t.Fatalf("raw error lost: %v", e)
			}
			closeErr := c.Close()
			if !errors.Is(closeErr, ce) || outer.closes != 0 || len(outer.writes) != 0 {
				t.Fatalf("direct Close used outer TLS: %v", closeErr)
			}
			expectedCloses := 1
			if !capable {
				expectedCloses = 2
			}
			if raw.closes != expectedCloses || half.halves != map[bool]int{true: 1, false: 0}[capable] {
				t.Fatal("direct close capability routing changed")
			}
			lifetimeRecord(map[string]any{"gate": "direct-close-error-routing", "CloseWrite_capability": capable, "CloseWrite_error": lifetimeText(e), "Close_error": lifetimeText(closeErr), "raw_close_calls": raw.closes, "raw_half_close_calls": half.halves, "outer_close_calls": outer.closes, "outer_writes": len(outer.writes), "error_identities_retained": true})
		})
	}
}

func TestVLESSServerRealFiveSecondHeaderDeadline(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	start := time.Now()
	if e := a.SetWriteDeadline(start.Add(5 * time.Second)); e != nil {
		t.Fatal(e)
	}
	c := &serverConn{ExtendedConn: bufio.NewExtendedConn(a)}
	e := c.CloseWrite()
	elapsed := time.Since(start)
	var ne net.Error
	if !errors.Is(e, os.ErrDeadlineExceeded) || !errors.As(e, &ne) || !ne.Timeout() {
		t.Fatalf("real deadline error was lost: %v", e)
	}
	if elapsed < 5*time.Second {
		t.Fatalf("5s deadline shortened: %v", elapsed)
	}
	one := make([]byte, 1)
	n, readErr := b.Read(one)
	if n != 0 || readErr != io.EOF {
		t.Fatalf("underlying Close did not retire peer after timeout: %d %v", n, readErr)
	}
	lifetimeRecord(map[string]any{"gate": "real-5s-deadline", "error": lifetimeText(e), "Timeout": ne.Timeout(), "deadline_error_identity_retained": true, "elapsed_ns": elapsed.Nanoseconds(), "peer_read_error": lifetimeText(readErr), "timeout_not_reclassified_as_EOF": true})
}
