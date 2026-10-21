package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

func ParseRequest(buf []byte, limits Limits) Result {
	if limits.MaxArrayLength <= 0 {
		limits.MaxArrayLength = DefaultLimits.MaxArrayLength
	}
	if limits.MaxBulkLength <= 0 {
		limits.MaxBulkLength = DefaultLimits.MaxBulkLength
	}
	if limits.MaxLineLength <= 0 {
		limits.MaxLineLength = DefaultLimits.MaxLineLength
	}
	if len(buf) == 0 {
		return Result{Status: Incomplete}
	}
	if buf[0] != '*' {
		return parseInline(buf, limits.MaxLineLength)
	}
	count, header, status, err := numberLine(buf[1:], limits.MaxLineLength)
	if status != Parsed {
		return Result{Status: status, Err: err}
	}
	if count < 0 {
		return protocolError("negative array length")
	}
	if count > int64(limits.MaxArrayLength) {
		return protocolError("array length exceeds %d", limits.MaxArrayLength)
	}
	pos := header + 1
	args := make([][]byte, 0, int(count))
	for i := 0; i < int(count); i++ {
		if pos >= len(buf) {
			return Result{Status: Incomplete}
		}
		if buf[pos] != '$' {
			return protocolError("request element %d is not a bulk string", i)
		}
		value, consumed, status, err := parseBulk(buf[pos:], limits)
		if status != Parsed {
			return Result{Status: status, Err: err}
		}
		if value == nil {
			return protocolError("null bulk command element")
		}
		args = append(args, value)
		pos += consumed
	}
	if len(args) == 0 {
		return protocolError("empty command")
	}
	return Result{
		Status: Parsed, Consumed: pos,
		Command: Command{Name: bytes.ToUpper(args[0]), Args: args[1:]},
	}
}

func parseBulk(buf []byte, limits Limits) ([]byte, int, ParseStatus, error) {
	length, header, status, err := numberLine(buf[1:], limits.MaxLineLength)
	if status != Parsed {
		return nil, 0, status, err
	}
	if length < 0 {
		if length == -1 {
			return nil, header + 1, Parsed, nil
		}
		return nil, 0, ProtocolError, fmt.Errorf("invalid bulk length %d", length)
	}
	if length > int64(limits.MaxBulkLength) {
		return nil, 0, ProtocolError, fmt.Errorf("bulk length exceeds %d", limits.MaxBulkLength)
	}
	end := header + 1 + int(length)
	if end+2 > len(buf) {
		return nil, 0, Incomplete, nil
	}
	if buf[end] != '\r' || buf[end+1] != '\n' {
		return nil, 0, ProtocolError, errors.New("bulk payload missing trailing CRLF")
	}
	return buf[header+1 : end], end + 2, Parsed, nil
}

func parseInline(buf []byte, max int) Result {
	end := indexCRLF(buf)
	if end < 0 {
		if len(buf) > max {
			return protocolError("inline command exceeds %d bytes", max)
		}
		return Result{Status: Incomplete}
	}
	if end > max {
		return protocolError("inline command exceeds %d bytes", max)
	}
	fields := strings.Fields(string(buf[:end]))
	if len(fields) == 0 {
		return protocolError("empty command")
	}
	args := make([][]byte, len(fields)-1)
	for i := range args {
		args[i] = []byte(fields[i+1])
	}
	return Result{
		Status: Parsed, Consumed: end + 2,
		Command: Command{Name: bytes.ToUpper([]byte(fields[0])), Args: args},
	}
}
