package main

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"testing"
)

type testStore struct {
	values    map[string][]byte
	deleteErr error
}

func (s *testStore) Put(key, value []byte) error {
	if s.values == nil {
		s.values = make(map[string][]byte)
	}
	s.values[string(key)] = append([]byte(nil), value...)
	return nil
}

func (s *testStore) Get(key []byte) ([]byte, bool) {
	value, ok := s.values[string(key)]
	return append([]byte(nil), value...), ok
}

func (s *testStore) Delete(key []byte) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	delete(s.values, string(key))
	return nil
}

func TestCLICommands(t *testing.T) {
	db := &testStore{}
	var output bytes.Buffer

	runCLI(db, bufio.NewScanner(strings.NewReader("put user:1 Alice\nget user:1\ndel user:1\nget user:1\nexit\n")), &output)

	result := output.String()
	for _, want := range []string{"Commands: put <key> <value>, get <key>, del <key>, exit", "OK", "Alice", "Deleted", "Key not found"} {
		if !strings.Contains(result, want) {
			t.Errorf("CLI output does not contain %q:\n%s", want, result)
		}
	}
	if _, ok := db.values["user:1"]; ok {
		t.Fatal("del did not remove user:1")
	}
}

func TestCLIDeleteError(t *testing.T) {
	db := &testStore{deleteErr: errors.New("delete failed")}
	var output bytes.Buffer

	runCLI(db, bufio.NewScanner(strings.NewReader("del user:1\nexit\n")), &output)

	if !strings.Contains(output.String(), "Error: delete failed") {
		t.Fatalf("CLI output = %q, want delete error", output.String())
	}
	if strings.Contains(output.String(), "Deleted") {
		t.Fatalf("CLI reported successful deletion after error: %q", output.String())
	}
}

func TestCLIDelUsage(t *testing.T) {
	var output bytes.Buffer
	runCLI(&testStore{}, bufio.NewScanner(strings.NewReader("del\nexit\n")), &output)

	if !strings.Contains(output.String(), "Usage: del <key>") {
		t.Fatalf("CLI output = %q, want del usage", output.String())
	}
}
