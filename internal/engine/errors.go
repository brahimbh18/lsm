package engine

import "errors"

var ErrSequenceOverflow = errors.New("sequence number overflow")
var ErrWALIDOverflow = errors.New("WAL id overflow")
