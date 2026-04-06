package replay

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkLoadFixtureFile(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "fixture.ndjson")

	lines := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"relay_url":"wss://relay-%d.example","payload":["EVENT","sub-%d",{"id":"evt_%d","kind":1,"content":"bench"}]}`,
			i%5,
			i%7,
			i,
		))
	}
	payload := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		b.Fatalf("write fixture: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fixture, err := loadFixtureFile(path)
		if err != nil {
			b.Fatalf("loadFixtureFile: %v", err)
		}
		if len(fixture.Entries) != 500 {
			b.Fatalf("unexpected entries count: %d", len(fixture.Entries))
		}
	}
}
