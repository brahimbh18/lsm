package server

import (
	"errors"
	"fmt"
	"net"
	"time"

	"lsm/internal/engine"
	"lsm/internal/protocol"
)

type Store interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool)
	Delete(key []byte) error
}

type Config struct {
	Limits         protocol.Limits
	IdleTimeout    time.Duration
	MaxConnections int
	ReadBufferSize int
}

var DefaultConfig = Config{
	Limits: protocol.DefaultLimits, IdleTimeout: 30 * time.Second,
	MaxConnections: 1000, ReadBufferSize: 4096,
}

type Server struct {
	cfg   Config
	store Store
	sem   chan struct{}
}

func New(store Store, cfg Config) *Server {
	if cfg.Limits.MaxArrayLength <= 0 {
		cfg.Limits = protocol.DefaultLimits
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultConfig.IdleTimeout
	}
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = DefaultConfig.MaxConnections
	}
	if cfg.ReadBufferSize <= 0 {
		cfg.ReadBufferSize = DefaultConfig.ReadBufferSize
	}
	return &Server{cfg: cfg, store: store, sem: make(chan struct{}, cfg.MaxConnections)}
}

func (s *Server) Serve(ln net.Listener) error {
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetNoDelay(true)
		}
		select {
		case s.sem <- struct{}{}:
			go func() {
				defer func() { <-s.sem }()
				s.serveConn(conn)
			}()
		default:
			_, _ = conn.Write(protocol.Error(nil, []byte("ERR max connections reached")))
			_ = conn.Close()
		}
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 0, s.cfg.ReadBufferSize)
	readBuf := make([]byte, s.cfg.ReadBufferSize)
	out := make([]byte, 0, s.cfg.ReadBufferSize)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(s.cfg.IdleTimeout))
		n, err := conn.Read(readBuf)
		if n > 0 {
			buf = append(buf, readBuf[:n]...)
			for len(buf) > 0 {
				result := protocol.ParseRequest(buf, s.cfg.Limits)
				if result.Status == protocol.Incomplete {
					break
				}
				if result.Status == protocol.ProtocolError {
					out = protocol.Error(out, []byte("ERR Protocol error: "+result.Err.Error()))
					_, _ = conn.Write(out)
					return
				}
				var closeAfter bool
				out, closeAfter = s.dispatch(result.Command, out)
				buf = buf[result.Consumed:]
				if closeAfter {
					_, _ = conn.Write(out)
					return
				}
			}
			if len(out) > 0 {
				if _, err := conn.Write(out); err != nil {
					return
				}
				out = out[:0]
			}
		}
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			return
		}
	}
}

func (s *Server) dispatch(cmd protocol.Command, out []byte) ([]byte, bool) {
	name := string(cmd.Name)
	switch name {
	case "PING":
		switch len(cmd.Args) {
		case 0:
			return protocol.SimpleString(out, []byte("PONG")), false
		case 1:
			return protocol.Bulk(out, cmd.Args[0]), false
		default:
			return wrongArgs(out, name), false
		}
	case "QUIT":
		if len(cmd.Args) != 0 {
			return wrongArgs(out, name), false
		}
		return protocol.SimpleString(out, []byte("OK")), true
	case "SET":
		if len(cmd.Args) != 2 {
			return wrongArgs(out, name), false
		}
		if err := s.store.Put(cmd.Args[0], cmd.Args[1]); err != nil {
			return storeError(out, err), false
		}
		return protocol.SimpleString(out, []byte("OK")), false
	case "GET":
		if len(cmd.Args) != 1 {
			return wrongArgs(out, name), false
		}
		value, ok := s.store.Get(cmd.Args[0])
		if !ok {
			return protocol.NullBulk(out), false
		}
		return protocol.Bulk(out, value), false
	case "DEL":
		if len(cmd.Args) < 1 {
			return wrongArgs(out, name), false
		}
		var count int64
		for _, key := range cmd.Args {
			if _, ok := s.store.Get(key); !ok {
				continue
			}
			if err := s.store.Delete(key); err != nil {
				return storeError(out, err), false
			}
			count++
		}
		return protocol.Integer(out, count), false
	case "EXISTS":
		if len(cmd.Args) < 1 {
			return wrongArgs(out, name), false
		}
		var count int64
		for _, key := range cmd.Args {
			if _, ok := s.store.Get(key); ok {
				count++
			}
		}
		return protocol.Integer(out, count), false
	case "MGET":
		if len(cmd.Args) < 1 {
			return wrongArgs(out, name), false
		}
		elements := make([][]byte, 0, len(cmd.Args))
		for _, key := range cmd.Args {
			value, ok := s.store.Get(key)
			if ok {
				elements = append(elements, protocol.Bulk(nil, value))
			} else {
				elements = append(elements, protocol.NullBulk(nil))
			}
		}
		return protocol.Array(out, elements...), false
	case "MSET":
		if len(cmd.Args) == 0 || len(cmd.Args)%2 != 0 {
			return wrongArgs(out, name), false
		}
		for i := 0; i < len(cmd.Args); i += 2 {
			if err := s.store.Put(cmd.Args[i], cmd.Args[i+1]); err != nil {
				return storeError(out, err), false
			}
		}
		return protocol.SimpleString(out, []byte("OK")), false
	case "COMMAND", "CONFIG":
		return protocol.Array(out), false
	case "RANGE":
		if len(cmd.Args) < 2 {
			return wrongArgs(out, name), false
		}
		// TODO: replace with the engine's ordered LSM scan.
		return protocol.Array(out), false
	default:
		return protocol.Error(out, []byte(fmt.Sprintf("ERR unknown command '%s'", name))), false
	}
}

func wrongArgs(out []byte, name string) []byte {
	return protocol.Error(out, []byte(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name)))
}

func storeError(out []byte, err error) []byte {
	if errors.Is(err, engine.ErrClosed) {
		return protocol.Error(out, []byte("ERR database is closed"))
	}
	return protocol.Error(out, []byte("ERR internal storage error"))
}
