package server

import (
	"bufio"
	"bytes"
	"net"
	"testing"
	"time"

	"lsm/internal/engine"
	"lsm/internal/protocol"
)

func startTestServer(t *testing.T) (net.Listener, *engine.DB) {
	t.Helper()
	db, err := engine.Open(engine.Options{DataDir: t.TempDir(), SyncMode: engine.SyncModeSync})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	go func() { _ = New(db, Config{IdleTimeout: time.Second}).Serve(listener) }()
	t.Cleanup(func() {
		_ = listener.Close()
		_ = db.Close()
	})
	return listener, db
}

func command(name string, args ...string) []byte {
	var out []byte
	out = append(out, '*')
	out = append(out, '0'+byte(len(args)+1), '\r', '\n')
	out = protocol.Bulk(out, []byte(name))
	for _, arg := range args {
		out = protocol.Bulk(out, []byte(arg))
	}
	return out
}

func TestRESPCommandsAndPipeline(t *testing.T) {
	listener, _ := startTestServer(t)
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var input []byte
	input = append(input, command("SET", "foo", "bar")...)
	input = append(input, command("GET", "foo")...)
	input = append(input, command("GET", "missing")...)
	input = append(input, command("DEL", "foo", "missing")...)
	if _, err := conn.Write(input); err != nil {
		t.Fatal(err)
	}

	reader := bufio.NewReader(conn)
	for _, want := range []string{"+OK\r\n", "$3\r\n", "bar\r\n", "$-1\r\n", ":1\r\n"} {
		got, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("reply = %q, want %q", got, want)
		}
	}
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	listener, _ := startTestServer(t)
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = conn.Write([]byte("*1\r\n:1\r\n"))
	reader := bufio.NewReader(conn)
	got, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix([]byte(got), []byte("-ERR Protocol error:")) {
		t.Fatalf("unexpected protocol error: %q", got)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("connection remained open")
	}
}
