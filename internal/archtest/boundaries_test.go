package archtest

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"strconv"
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
	root := repoRoot(t)

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
		{
			name: "store layer does not depend on derivation package",
			appliesTo: func(path string) bool {
				return strings.HasPrefix(path, filepath.ToSlash("internal/store/"))
			},
			forbidden: []string{
				"github.com/xdzczk/nostrmash/internal/derivation",
			},
		},
		{
			name: "jobs package remains store-independent",
			appliesTo: func(path string) bool {
				return strings.HasPrefix(path, filepath.ToSlash("internal/jobs/"))
			},
			forbidden: []string{
				"github.com/xdzczk/nostrmash/internal/store",
			},
		},
		{
			name: "query application layer stays transport-agnostic",
			appliesTo: func(path string) bool {
				return strings.HasPrefix(path, filepath.ToSlash("internal/query/"))
			},
			// Query is the shared app-level orchestration surface and must not depend
			// on concrete transport adapters.
			forbidden: []string{
				"github.com/xdzczk/nostrmash/internal/api",
				"github.com/xdzczk/nostrmash/internal/api_primal",
				"github.com/xdzczk/nostrmash/internal/transport",
			},
		},
		{
			name: "native and primal HTTP transports stay decoupled",
			appliesTo: func(path string) bool {
				return strings.HasPrefix(path, filepath.ToSlash("internal/api/"))
			},
			// Prevent cross-surface helper drift between native and primal handlers.
			forbidden: []string{
				"github.com/xdzczk/nostrmash/internal/api_primal",
			},
		},
		{
			name: "primal transport does not depend on native API transport",
			appliesTo: func(path string) bool {
				return strings.HasPrefix(path, filepath.ToSlash("internal/api_primal/"))
			},
			forbidden: []string{
				"github.com/xdzczk/nostrmash/internal/api",
			},
		},
		{
			name: "shared transport helpers remain infra-only",
			appliesTo: func(path string) bool {
				return strings.HasPrefix(path, filepath.ToSlash("internal/transport/"))
			},
			// Shared transport utilities should stay thin and avoid application
			// orchestration dependencies.
			forbidden: []string{
				"github.com/xdzczk/nostrmash/internal/query",
				"github.com/xdzczk/nostrmash/internal/store",
				"github.com/xdzczk/nostrmash/internal/derivation",
				"github.com/xdzczk/nostrmash/internal/api",
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
					if !matchesImportPath(importPath, forbidden) {
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

func TestMigratedThreadPathsStayQueryOrchestrated(t *testing.T) {
	root := repoRoot(t)
	fset := token.NewFileSet()

	// These handlers were migrated to query.Service orchestration and should not
	// drift back to direct store method calls.
	for _, tc := range []struct {
		file     string
		funcName string
		receiver string
	}{
		{file: "internal/api/handlers_threads.go", funcName: "GetThread", receiver: "h"},
		{file: "internal/api_primal/handlers.go", funcName: "GetThreadView", receiver: "h"},
	} {
		parsed := parseFile(t, fset, filepath.Join(root, tc.file))
		fn := mustFindFunc(t, parsed, tc.funcName)
		if hasFieldMethodCall(fn.Body, tc.receiver, "store", "") {
			t.Fatalf("%s must not call %s.store methods directly; keep thread orchestration in internal/query", tc.file, tc.receiver)
		}
		if !hasFieldMethodCall(fn.Body, tc.receiver, "service", "GetThread") {
			t.Fatalf("%s must delegate thread orchestration through %s.service.GetThread", tc.file, tc.receiver)
		}
	}

	wsFile := parseFile(t, fset, filepath.Join(root, "internal/api_primal/primal_cache_dispatch.go"))
	dispatchCacheCall := mustFindFunc(t, wsFile, "dispatchCacheCall")
	for _, wsCase := range []struct {
		name               string
		queryMethod        string
		helperMethod       string
		failStoreMsg       string
		failQueryMsg       string
		failHelperRouteMsg string
		failHelperStoreMsg string
		failHelperQueryMsg string
	}{
		{
			name:               "thread_view",
			queryMethod:        "GetThreadWindow",
			helperMethod:       "cacheDispatchThreadView",
			failStoreMsg:       "internal/api_primal/primal_cache_dispatch.go thread_view case must not call g.store methods directly; keep migrated thread assembly in internal/query",
			failQueryMsg:       "internal/api_primal/primal_cache_dispatch.go thread_view case must call g.query.GetThreadWindow or route to cacheDispatchThreadView",
			failHelperRouteMsg: "internal/api_primal/primal_cache_dispatch.go thread_view case must route to cacheDispatchThreadView when query call is delegated",
			failHelperStoreMsg: "internal/api_primal/cacheDispatchThreadView must not call g.store methods directly; keep migrated thread assembly in internal/query",
			failHelperQueryMsg: "internal/api_primal/cacheDispatchThreadView must call g.query.GetThreadWindow",
		},
		{
			name:               "user_infos",
			queryMethod:        "GetUserInfos",
			helperMethod:       "cacheDispatchUserInfos",
			failStoreMsg:       "internal/api_primal/primal_cache_dispatch.go user_infos case must not call g.store methods directly; keep profile batch orchestration in internal/query",
			failQueryMsg:       "internal/api_primal/primal_cache_dispatch.go user_infos case must call g.query.GetUserInfos or route to cacheDispatchUserInfos",
			failHelperRouteMsg: "internal/api_primal/primal_cache_dispatch.go user_infos case must route to cacheDispatchUserInfos when query call is delegated",
			failHelperStoreMsg: "internal/api_primal/cacheDispatchUserInfos must not call g.store methods directly; keep profile batch orchestration in internal/query",
			failHelperQueryMsg: "internal/api_primal/cacheDispatchUserInfos must call g.query.GetUserInfos",
		},
	} {
		caseBody := mustFindSwitchCaseBody(t, dispatchCacheCall, wsCase.name)
		if hasFieldMethodCallInStmts(caseBody, "g", "store", "") {
			t.Fatal(wsCase.failStoreMsg)
		}
		if hasFieldMethodCallInStmts(caseBody, "g", "query", wsCase.queryMethod) {
			continue
		}
		if !hasReceiverMethodCallInStmts(caseBody, "g", wsCase.helperMethod) {
			t.Fatal(wsCase.failHelperRouteMsg)
		}
		helperFn := mustFindFuncInDir(t, root, filepath.ToSlash("internal/api_primal"), wsCase.helperMethod)
		if hasFieldMethodCall(helperFn.Body, "g", "store", "") {
			t.Fatal(wsCase.failHelperStoreMsg)
		}
		if !hasFieldMethodCall(helperFn.Body, "g", "query", wsCase.queryMethod) {
			t.Fatal(wsCase.failHelperQueryMsg)
		}
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

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current file path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func matchesImportPath(importPath, forbiddenRoot string) bool {
	return importPath == forbiddenRoot || strings.HasPrefix(importPath, forbiddenRoot+"/")
}

func parseFile(t *testing.T, fset *token.FileSet, filePath string) *ast.File {
	t.Helper()
	parsed, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", filePath, err)
	}
	return parsed
}

func mustFindFunc(t *testing.T, file *ast.File, funcName string) *ast.FuncDecl {
	t.Helper()
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name == nil {
			continue
		}
		if fn.Name.Name == funcName {
			return fn
		}
	}
	t.Fatalf("function %s not found", funcName)
	return nil
}

func mustFindFuncInDir(t *testing.T, root, relDir, funcName string) *ast.FuncDecl {
	t.Helper()
	fset := token.NewFileSet()
	dir := filepath.Join(root, filepath.FromSlash(relDir))
	var found *ast.FuncDecl
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file := parseFile(t, fset, path)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name == nil {
				continue
			}
			if fn.Name.Name == funcName {
				found = fn
				return fs.SkipAll
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, fs.SkipAll) {
		t.Fatalf("walk %s: %v", relDir, err)
	}
	if found == nil {
		t.Fatalf("function %s not found in %s", funcName, relDir)
	}
	return found
}

func mustFindSwitchCaseBody(t *testing.T, fn *ast.FuncDecl, caseValue string) []ast.Stmt {
	t.Helper()
	for _, stmt := range fn.Body.List {
		switchStmt, ok := stmt.(*ast.SwitchStmt)
		if !ok {
			continue
		}
		for _, clause := range switchStmt.Body.List {
			caseClause, ok := clause.(*ast.CaseClause)
			if !ok {
				continue
			}
			for _, expr := range caseClause.List {
				lit, ok := expr.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					continue
				}
				if value == caseValue {
					return caseClause.Body
				}
			}
		}
	}
	t.Fatalf("switch case %q not found in %s", caseValue, fn.Name.Name)
	return nil
}

func hasFieldMethodCall(node ast.Node, receiverName, fieldName, methodName string) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		fieldSelector, ok := sel.X.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		receiver, ok := fieldSelector.X.(*ast.Ident)
		if !ok {
			return true
		}
		if receiver.Name != receiverName || fieldSelector.Sel.Name != fieldName {
			return true
		}
		if methodName != "" && sel.Sel.Name != methodName {
			return true
		}
		found = true
		return false
	})
	return found
}

func hasFieldMethodCallInStmts(stmts []ast.Stmt, receiverName, fieldName, methodName string) bool {
	for _, stmt := range stmts {
		if hasFieldMethodCall(stmt, receiverName, fieldName, methodName) {
			return true
		}
	}
	return false
}

func hasReceiverMethodCall(node ast.Node, receiverName, methodName string) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		receiver, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		if receiver.Name == receiverName && sel.Sel.Name == methodName {
			found = true
			return false
		}
		return true
	})
	return found
}

func hasReceiverMethodCallInStmts(stmts []ast.Stmt, receiverName, methodName string) bool {
	for _, stmt := range stmts {
		if hasReceiverMethodCall(stmt, receiverName, methodName) {
			return true
		}
	}
	return false
}
