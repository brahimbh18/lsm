package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"lsm/internal/engine"
)

func main() {
	db, err := engine.Open(engine.Options{
		DataDir:  envOrDefault("DATA_DIR", "data"),
		SyncMode: engine.SyncModeSync,
	})

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	runCLI(db, bufio.NewScanner(os.Stdin), os.Stdout)
}

type store interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, bool)
	Delete(key []byte) error
}

func runCLI(db store, scanner *bufio.Scanner, output io.Writer) {
	fmt.Fprintln(output, "LSM Database CLI")
	fmt.Fprintln(output, "Commands: put <key> <value>, get <key>, del <key>, exit")
	for {
		fmt.Fprint(output, "lsm> ")

		if !scanner.Scan() {
			break
		}

		input := strings.Fields(scanner.Text())

		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "put":
			if len(input) < 3 {
				fmt.Fprintln(output, "Usage: put <key> <value>")
				continue
			}

			key := []byte(input[1])
			value := []byte(strings.Join(input[2:], " "))

			if err := db.Put(key, value); err != nil {
				fmt.Fprintln(output, "Error:", err)
				continue
			}

			fmt.Fprintln(output, "OK")

		case "get":
			if len(input) != 2 {
				fmt.Fprintln(output, "Usage: get <key>")
				continue
			}

			value, ok := db.Get([]byte(input[1]))

			if ok {
				fmt.Fprintf(output, "%s\n", value)
			} else {
				fmt.Fprintln(output, "Key not found")
			}

		case "del":
			if len(input) != 2 {
				fmt.Fprintln(output, "Usage: del <key>")
				continue
			}

			if err := db.Delete([]byte(input[1])); err != nil {
				fmt.Fprintln(output, "Error:", err)
				continue
			}

			fmt.Fprintln(output, "Deleted")

		case "exit":
			fmt.Fprintln(output, "Closing database...")
			return

		default:
			fmt.Fprintln(output, "Unknown command:", input[0])
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(output, "Input error:", err)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
