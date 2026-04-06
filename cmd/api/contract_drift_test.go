package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestOpenAPIContainsAllContractOwnedRoutes_OneWayPolicy(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	openapiFile := filepath.Join(root, "docs", "openapi.yaml")
	openapiSource, err := os.ReadFile(openapiFile)
	if err != nil {
		t.Fatalf("read openapi source: %v", err)
	}

	openapiMethods := parseOpenAPIPathMethods(string(openapiSource))
	defs := contractOwnedRoutes()
	seen := map[string]struct{}{}
	for _, def := range defs {
		if !def.OwnsContract {
			continue
		}
		pattern := strings.TrimSpace(def.Pattern)
		if _, ok := seen[pattern]; ok {
			t.Fatalf("duplicate route definition: %s", pattern)
		}
		seen[pattern] = struct{}{}

		method, path, ok := strings.Cut(pattern, " ")
		if !ok || strings.TrimSpace(path) == "" {
			t.Fatalf("invalid route pattern %q", pattern)
		}
		if !openAPIHasPathMethod(openapiMethods, path, method) {
			t.Fatalf("openapi missing %s %s", method, path)
		}
	}
	if len(seen) == 0 {
		t.Fatal("no contract-owned routes declared")
	}
}

func TestParseOpenAPIPathMethods_ExtractsPathMethods(t *testing.T) {
	source := `openapi: 3.0.3
paths:
  /alpha:
    get:
      summary: alpha
  /beta:
    post:
      summary: beta
components:
  schemas: {}
`
	got := parseOpenAPIPathMethods(source)
	if !openAPIHasPathMethod(got, "/alpha", "GET") {
		t.Fatal("expected /alpha GET to be present")
	}
	if !openAPIHasPathMethod(got, "/beta", "POST") {
		t.Fatal("expected /beta POST to be present")
	}
	if openAPIHasPathMethod(got, "/beta", "GET") {
		t.Fatal("did not expect /beta GET")
	}
}

func openAPIHasPathMethod(methodsByPath map[string]map[string]struct{}, path, method string) bool {
	methods, ok := methodsByPath[path]
	if !ok {
		return false
	}
	_, ok = methods[strings.ToLower(strings.TrimSpace(method))]
	return ok
}

func parseOpenAPIPathMethods(source string) map[string]map[string]struct{} {
	lines := strings.Split(source, "\n")
	methodsByPath := make(map[string]map[string]struct{})

	inPaths := false
	currentPath := ""
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		if !inPaths {
			if trimmed == "paths:" {
				inPaths = true
			}
			continue
		}

		if !strings.HasPrefix(line, "  ") {
			break
		}

		if strings.HasPrefix(line, "  /") && strings.HasSuffix(trimmed, ":") {
			currentPath = strings.TrimSuffix(trimmed, ":")
			if _, ok := methodsByPath[currentPath]; !ok {
				methodsByPath[currentPath] = make(map[string]struct{})
			}
			continue
		}

		if currentPath == "" || !strings.HasPrefix(line, "    ") || !strings.HasSuffix(trimmed, ":") {
			continue
		}

		maybeMethod := strings.ToLower(strings.TrimSuffix(trimmed, ":"))
		if isHTTPMethod(maybeMethod) {
			methodsByPath[currentPath][maybeMethod] = struct{}{}
		}
	}
	return methodsByPath
}

func isHTTPMethod(value string) bool {
	switch value {
	case "get", "post", "put", "patch", "delete", "head", "options", "trace":
		return true
	default:
		return false
	}
}
