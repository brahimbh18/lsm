package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type TableMeta struct {
	ID       uint64 `json:"id"`
	Level    int    `json:"level"`
	Size     uint64 `json:"size"`
	Smallest []byte `json:"smallest"`
	Largest  []byte `json:"largest"`
}

type Manifest struct {
	Tables       []TableMeta `json:"tables"`
	MinLiveWALID uint64      `json:"min_live_wal_id,omitempty"`
}

func ReadManifest(directory string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(directory, "MANIFEST"))
	if os.IsNotExist(err) {
		return Manifest{}, nil
	}
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return manifest, nil
}

func WriteManifest(directory string, manifest Manifest) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	tempPath := filepath.Join(directory, "MANIFEST.tmp")
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	file, err := os.OpenFile(tempPath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tempPath, filepath.Join(directory, "MANIFEST"))
}
