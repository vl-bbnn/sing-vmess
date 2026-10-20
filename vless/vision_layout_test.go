package vless

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"testing"
	"unsafe"

	"github.com/sagernet/sing/common/logger"
)

func TestVisionUnsupportedTLSBufferLayoutsFailClosed(t *testing.T) {
	anchor := new(struct{ Value int })
	ptr := unsafe.Pointer(anchor)
	cases := []struct {
		name string
		typ  reflect.Type
		ptr  unsafe.Pointer
	}{
		{"nil-pointer", reflect.TypeOf(struct {
			input    bytes.Reader
			rawInput bytes.Buffer
		}{}), nil},
		{"nil-type", nil, ptr},
		{"non-struct-type", reflect.TypeOf(0), ptr},
		{"missing-input", reflect.TypeOf(struct{ rawInput bytes.Buffer }{}), ptr},
		{"missing-rawInput", reflect.TypeOf(struct{ input bytes.Reader }{}), ptr},
		{"wrong-input-type", reflect.TypeOf(struct {
			input    int
			rawInput bytes.Buffer
		}{}), ptr},
		{"wrong-rawInput-type", reflect.TypeOf(struct {
			input    bytes.Reader
			rawInput int
		}{}), ptr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			original := tlsRegistry
			tlsRegistry = append(append([]func(net.Conn) (bool, net.Conn, reflect.Type, unsafe.Pointer){}, original...), func(c net.Conn) (bool, net.Conn, reflect.Type, unsafe.Pointer) { return true, c, tc.typ, tc.ptr })
			defer func() { tlsRegistry = original }()
			conn, e := NewVisionConn(a, a, [16]byte{}, logger.NOP())
			if e == nil || conn != nil {
				t.Fatal("unsupported TLS layout accepted")
			}
			row, _ := json.Marshal(map[string]any{"case": tc.name, "unsupported_layout_rejected": true, "error": e.Error(), "unsafe_pointer_not_dereferenced": true})
			fmt.Println("VISION_LAYOUT_CASE " + string(row))
		})
	}
}
