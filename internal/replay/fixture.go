package replay

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FixtureEntry is one deterministic replay input payload.
type FixtureEntry struct {
	RelayURL string          `json:"relay_url"`
	Payload  json.RawMessage `json:"payload"`
}

// Fixture is an ordered collection of relay payload entries.
type Fixture struct {
	Entries []FixtureEntry `json:"entries"`
}

// LoadFixture loads a replay fixture from a .ndjson file or a directory of .ndjson files.
// Directory mode is deterministic: files are loaded in lexicographic path order.
func LoadFixture(path string) (Fixture, error) {
	trimmedPath := strings.TrimSpace(path)
	if trimmedPath == "" {
		return Fixture{}, fmt.Errorf("fixture path is required")
	}

	info, err := os.Stat(trimmedPath)
	if err != nil {
		return Fixture{}, fmt.Errorf("stat fixture path: %w", err)
	}

	if info.IsDir() {
		return loadFixtureDir(trimmedPath)
	}
	return loadFixtureFile(trimmedPath)
}

func loadFixtureDir(dir string) (Fixture, error) {
	entries := make([]FixtureEntry, 0)
	paths, err := collectNDJSONFiles(dir)
	if err != nil {
		return Fixture{}, err
	}
	if len(paths) == 0 {
		return Fixture{}, fmt.Errorf("no .ndjson files found in %s", dir)
	}
	for _, path := range paths {
		fixture, err := loadFixtureFile(path)
		if err != nil {
			return Fixture{}, err
		}
		entries = append(entries, fixture.Entries...)
	}
	return Fixture{Entries: entries}, nil
}

func collectNDJSONFiles(root string) ([]string, error) {
	out := make([]string, 0)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if strings.EqualFold(filepath.Ext(path), ".ndjson") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk fixture directory: %w", err)
	}
	sort.Strings(out)
	return out, nil
}

func loadFixtureFile(path string) (Fixture, error) {
	file, err := os.Open(path)
	if err != nil {
		return Fixture{}, fmt.Errorf("open fixture file %s: %w", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Allow sizable payloads while keeping bounded memory per line.
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	entries := make([]FixtureEntry, 0)
	line := 0
	for scanner.Scan() {
		line++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}
		var entry FixtureEntry
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			return Fixture{}, fmt.Errorf("decode fixture %s line %d: %w", path, line, err)
		}
		entry.RelayURL = strings.TrimSpace(entry.RelayURL)
		if entry.RelayURL == "" {
			return Fixture{}, fmt.Errorf("fixture %s line %d: relay_url is required", path, line)
		}
		entry.Payload = normalizePayload(entry.Payload)
		if len(entry.Payload) == 0 {
			return Fixture{}, fmt.Errorf("fixture %s line %d: payload is required", path, line)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		if err == io.EOF {
			return Fixture{Entries: entries}, nil
		}
		return Fixture{}, fmt.Errorf("scan fixture file %s: %w", path, err)
	}
	if len(entries) == 0 {
		return Fixture{}, fmt.Errorf("fixture file %s has no replay entries", path)
	}
	return Fixture{Entries: entries}, nil
}

func normalizePayload(payload json.RawMessage) json.RawMessage {
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" {
		return nil
	}
	return json.RawMessage(trimmed)
}
