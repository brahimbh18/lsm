package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type putRequest struct {
	Value string `json:"value"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func main() {
	addr := flag.String("addr", "http://localhost:8080", "LSM server base URL")
	flag.Parse()

	baseURL := strings.TrimRight(*addr, "/")
	client := &http.Client{}

	// 1. PUT foo=hello
	if err := put(client, baseURL, "foo", "hello"); err != nil {
		fmt.Fprintf(os.Stderr, "Error putting 'foo': %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PUT foo=hello")

	// 2. GET foo
	val, status, err := get(client, baseURL, "foo")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting 'foo': %v\n", err)
		os.Exit(1)
	}
	if status == http.StatusOK {
		fmt.Printf("GET foo → %s\n\n", val)
	} else {
		fmt.Printf("GET foo → %d\n\n", status)
	}

	// 3. PUT bar=world
	if err := put(client, baseURL, "bar", "world"); err != nil {
		fmt.Fprintf(os.Stderr, "Error putting 'bar': %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PUT bar=world")

	// 4. GET bar
	val, status, err = get(client, baseURL, "bar")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting 'bar': %v\n", err)
		os.Exit(1)
	}
	if status == http.StatusOK {
		fmt.Printf("GET bar → %s\n\n", val)
	} else {
		fmt.Printf("GET bar → %d\n\n", status)
	}

	// 5. GET missing key
	val, status, err = get(client, baseURL, "missing")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting 'missing': %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("GET missing → %d\n", status)
}

func put(client *http.Client, baseURL, key, value string) error {
	payload, err := json.Marshal(putRequest{Value: value})
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/kv/%s", baseURL, key)
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func get(client *http.Client, baseURL, key string) (string, int, error) {
	url := fmt.Sprintf("%s/kv/%s", baseURL, key)
	resp, err := client.Get(url)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var r getResponse
		if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
			return "", resp.StatusCode, err
		}
		return r.Value, http.StatusOK, nil
	}

	return "", resp.StatusCode, nil
}
