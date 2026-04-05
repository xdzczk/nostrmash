package archtest

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type importRule struct {
	name       string
	appliesTo  func(path string) bool
	forbidden  []string
	exceptions []string
}

func TestImportBoundaries(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current file path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))

	rules := []importRule{
		{
			name: "ingestor executables stay API-independent",
			appliesTo: func(path string) bool {
				return strings.HasPrefix(path, filepath.ToSlash("cmd/ingestor/")) ||
					strings.HasPrefix(path, filepath.ToSlash("cmd/worker/"))
			},
			forbidden: []string{
				"github.com/xdzczk/nostrmash/internal/api",
				"github.com/xdzczk/nostrmash/internal/api_primal",
			},
		},
		{
			name: "ingestor internals do not depend on API layers",
			appliesTo: func(path string) bool {
				return strings.HasPrefix(path, filepath.ToSlash("internal/ingestor/"))
			},
			forbidden: []string{
				"github.com/xdzczk/nostrmash/internal/api",
				"github.com/xdzczk/nostrmash/internal/api_primal",
			},
		},
		{
			name: "primal adapter stays isolated to API entrypoint",
			appliesTo: func(path string) bool {
				if strings.HasPrefix(path, filepath.ToSlash("cmd/api/")) {
					return false
				}
				if strings.HasPrefix(path, filepath.ToSlash("internal/api_primal/")) {
					return false
				}
				return true
			},
			forbidden: []string{
				"github.com/xdzczk/nostrmash/internal/api_primal",
			},
		},
	}

	var violations []string
	fset := token.NewFileSet()
	files := make([]string, 0, 128)
	for _, dir := range []string{"cmd", "internal"} {
		dirPath := filepath.Join(root, dir)
		err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			if strings.HasSuffix(path, ".go") {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		rel, err := filepath.Rel(root, file)
		if err != nil {
			t.Fatalf("relative path for %s: %v", file, err)
		}
		rel = filepath.ToSlash(rel)
		parsed, err := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports in %s: %v", rel, err)
		}

		for _, spec := range parsed.Imports {
			importPath := strings.Trim(spec.Path.Value, "\"")
			for _, rule := range rules {
				if !rule.appliesTo(rel) {
					continue
				}
				for _, forbidden := range rule.forbidden {
					if importPath != forbidden {
						continue
					}
					if isException(rel, rule.exceptions) {
						continue
					}
					violations = append(violations, rel+" imports "+importPath+" (rule: "+rule.name+")")
				}
			}
		}
	}

	if len(violations) > 0 {
		t.Fatalf("architecture import boundary violations:\n- %s", strings.Join(violations, "\n- "))
	}
}

func isException(path string, exceptions []string) bool {
	for _, exception := range exceptions {
		if strings.HasPrefix(path, exception) {
			return true
		}
	}
	return false
}
