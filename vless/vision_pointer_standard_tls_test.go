package vless

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"github.com/sagernet/sing/common/logger"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// This executes Vision over real standard TLS, exercising the other pointer
// registry without modelling field offsets or disabling runtime checks.
func TestVisionStandardTLSOriginalBidirectionalBytes(t *testing.T) {
	cert := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer cert.Close()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	client := tls.Client(a, &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}) // ephemeral local fixture
	server := tls.Server(b, &tls.Config{Certificates: cert.TLS.Certificates, MinVersion: tls.VersionTLS13})
	deadline := time.Now().Add(3 * time.Second)
	_ = client.SetDeadline(deadline)
	_ = server.SetDeadline(deadline)
	ready := make(chan error, 1)
	go func() { ready <- server.Handshake() }()
	if e := client.Handshake(); e != nil {
		t.Fatal(e)
	}
	if e := <-ready; e != nil {
		t.Fatal(e)
	}
	var user [16]byte
	copy(user[:], []byte("original-vision!"))
	left, e := NewVisionConn(client, client, user, logger.NOP())
	if e != nil {
		t.Fatal(e)
	}
	right, e := NewVisionConn(server, server, user, logger.NOP())
	if e != nil {
		t.Fatal(e)
	}
	for i, pair := range [][2]net.Conn{{left, right}, {right, left}} {
		payload := bytes.Repeat([]byte(fmt.Sprintf("original-direction-%d-", i)), 256)
		want := sha256.Sum256(payload)
		received := make(chan error, 1)
		go func() {
			got := make([]byte, len(payload))
			_, e := io.ReadFull(pair[1], got)
			if e == nil && sha256.Sum256(got) != want {
				e = fmt.Errorf("original bytes changed")
			}
			received <- e
		}()
		if n, e := pair[0].Write(payload); e != nil || n != len(payload) {
			t.Fatal("original write", n, e)
		}
		if e := <-received; e != nil {
			t.Fatal("original receive", e)
		}
		t.Logf("STD_TLS original_direction=%d bytes=%d sha256=%x", i, len(payload), want)
	}
}
func TestVisionStandardTLSUnsupportedUnderlayStillRejected(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if c, e := NewVisionConn(a, a, [16]byte{}, logger.NOP()); e == nil || c != nil {
		t.Fatal("non-TLS underlay accepted")
	}
}
