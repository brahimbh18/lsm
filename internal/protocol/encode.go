package protocol

import "strconv"

func SimpleString(dst []byte, s []byte) []byte {
	return appendLine(dst, '+', sanitize(s))
}

func Error(dst []byte, s []byte) []byte {
	return appendLine(dst, '-', sanitize(s))
}

func Integer(dst []byte, n int64) []byte {
	dst = append(dst, ':')
	dst = strconv.AppendInt(dst, n, 10)
	return append(dst, '\r', '\n')
}

func Bulk(dst []byte, b []byte) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(b)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, b...)
	return append(dst, '\r', '\n')
}

func NullBulk(dst []byte) []byte { return append(dst, '$', '-', '1', '\r', '\n') }

func Array(dst []byte, elements ...[]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(elements)), 10)
	dst = append(dst, '\r', '\n')
	for _, element := range elements {
		dst = append(dst, element...)
	}
	return dst
}

func appendLine(dst []byte, prefix byte, s []byte) []byte {
	dst = append(dst, prefix)
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

func sanitize(s []byte) []byte {
	out := make([]byte, 0, len(s))
	for _, b := range s {
		if b != '\r' && b != '\n' {
			out = append(out, b)
		}
	}
	return out
}
