package protocol

import (
	"errors"
	"fmt"
	"strconv"
)

var ErrIncomplete = errors.New("incomplete RESP frame")

type ParseStatus int

const (
	Incomplete ParseStatus = iota
	Parsed
	ProtocolError
)

type Command struct {
	Name []byte
	Args [][]byte
}

type Limits struct {
	MaxArrayLength int
	MaxBulkLength  int
	MaxLineLength  int
}

var DefaultLimits = Limits{MaxArrayLength: 1024, MaxBulkLength: 1 << 20, MaxLineLength: 64 << 10}

type Result struct {
	Status   ParseStatus
	Command  Command
	Consumed int
	Err      error
}

func protocolError(format string, args ...any) Result {
	return Result{Status: ProtocolError, Err: fmt.Errorf(format, args...)}
}

func numberLine(buf []byte, limit int) (int64, int, ParseStatus, error) {
	end := indexCRLF(buf)
	if end < 0 {
		if len(buf) > limit {
			return 0, 0, ProtocolError, fmt.Errorf("header exceeds %d bytes", limit)
		}
		return 0, 0, Incomplete, nil
	}
	if end > limit {
		return 0, 0, ProtocolError, fmt.Errorf("header exceeds %d bytes", limit)
	}
	if end == 0 {
		return 0, 0, ProtocolError, errors.New("empty length")
	}
	field := buf[:end]
	start := 0
	if field[0] == '-' {
		if len(field) == 1 {
			return 0, 0, ProtocolError, errors.New("invalid length")
		}
		start = 1
	}
	for _, b := range field[start:] {
		if b < '0' || b > '9' {
			return 0, 0, ProtocolError, errors.New("invalid length")
		}
	}
	n, err := strconv.ParseInt(string(field), 10, 64)
	if err != nil {
		return 0, 0, ProtocolError, errors.New("invalid length")
	}
	return n, end + 2, Parsed, nil
}

func indexCRLF(buf []byte) int {
	for i := 0; i+1 < len(buf); i++ {
		if buf[i] == '\r' && buf[i+1] == '\n' {
			return i
		}
	}
	return -1
}
