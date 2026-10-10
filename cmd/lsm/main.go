package main

import (
	"bufio"
	"fmt"
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

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("LSM Database CLI")
	fmt.Println("Commands: put <key> <value>, get <key>, exit")

	for {
		fmt.Print("lsm> ")

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
				fmt.Println("Usage: put <key> <value>")
				continue
			}

			key := []byte(input[1])
			value := []byte(strings.Join(input[2:], " "))

			if err := db.Put(key, value); err != nil {
				fmt.Println("Error:", err)
				continue
			}

			fmt.Println("OK")

		case "get":
			if len(input) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}

			value, ok := db.Get([]byte(input[1]))

			if ok {
				fmt.Printf("%s\n", value)
			} else {
				fmt.Println("(nil)")
			}

		case "exit":
			fmt.Println("Closing database...")
			return

		default:
			fmt.Println("Unknown command:", input[0])
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Input error:", err)
	}
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
