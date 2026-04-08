package api_primal

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestCacheCallRoutesUseExpectedHandlerFamily(t *testing.T) {
	if len(cacheCallRoutes) == 0 {
		t.Fatal("cacheCallRoutes must not be empty")
	}
	for method, route := range cacheCallRoutes {
		if strings.TrimSpace(method) != method || strings.ToLower(method) != method {
			t.Fatalf("cache route key must be normalized lower-case without surrounding spaces: %q", method)
		}
		handlerName := methodNameFromFunc(route.handler)
		switch route.family {
		case cacheCallFamilyDispatch:
			if !strings.HasPrefix(handlerName, "cacheDispatch") {
				t.Fatalf("cache method %q must use a cacheDispatch* handler for %q family (got %s)", method, route.family, handlerName)
			}
		case cacheCallFamilyResolve:
			if !strings.HasPrefix(handlerName, "resolve") {
				t.Fatalf("cache method %q must use a resolve* handler for %q family (got %s)", method, route.family, handlerName)
			}
		case cacheCallFamilyBuild:
			if !strings.HasPrefix(handlerName, "build") {
				t.Fatalf("cache method %q must use a build* handler for %q family (got %s)", method, route.family, handlerName)
			}
		default:
			t.Fatalf("cache method %q has unsupported handler family %q", method, route.family)
		}
	}
}

func TestAllCacheDispatchMethodsAreRegistered(t *testing.T) {
	registered := map[string]struct{}{}
	for _, route := range cacheCallRoutes {
		registered[methodNameFromFunc(route.handler)] = struct{}{}
	}
	for _, methodName := range listCacheDispatchMethods(t) {
		if _, ok := registered[methodName]; !ok {
			t.Fatalf("%s is not registered in cacheCallRoutes; add explicit method registration", methodName)
		}
	}
}

func TestWSFilterRoutesStayExplicit(t *testing.T) {
	want := map[string]string{
		"cache":  "wsCacheFilterHandler",
		"ids":    "wsIDsFilterHandler",
		"search": "wsSearchFilterHandler",
	}
	if len(wsFilterRoutes) != len(want) {
		t.Fatalf("unexpected wsFilterRoutes size: got=%d want=%d", len(wsFilterRoutes), len(want))
	}
	for _, route := range wsFilterRoutes {
		wantHandler, ok := want[route.key]
		if !ok {
			t.Fatalf("unexpected ws filter route key %q", route.key)
		}
		if got := methodNameFromFunc(route.handler); got != wantHandler {
			t.Fatalf("route key %q must use %s (got %s)", route.key, wantHandler, got)
		}
	}
}

func TestWSFrameHandlersStayExplicit(t *testing.T) {
	want := map[string]string{
		wsFrameREQ:   "handleWSFrameREQ",
		wsFrameClose: "handleWSFrameClose",
	}
	if len(wsFrameHandlers) != len(want) {
		t.Fatalf("unexpected wsFrameHandlers size: got=%d want=%d", len(wsFrameHandlers), len(want))
	}
	for kind, handler := range wsFrameHandlers {
		wantHandler, ok := want[kind]
		if !ok {
			t.Fatalf("unexpected ws frame kind registration %q", kind)
		}
		if got := methodNameFromFunc(handler); got != wantHandler {
			t.Fatalf("frame kind %q must use %s (got %s)", kind, wantHandler, got)
		}
	}
}

func listCacheDispatchMethods(t *testing.T) []string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current file path")
	}
	dir := filepath.Dir(thisFile)
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(info fs.FileInfo) bool {
		name := info.Name()
		return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse api_primal package: %v", err)
	}
	pkg, ok := pkgs["api_primal"]
	if !ok {
		t.Fatal("api_primal package not found")
	}

	seen := map[string]struct{}{}
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 || fn.Name == nil {
				continue
			}
			if !strings.HasPrefix(fn.Name.Name, "cacheDispatch") {
				continue
			}
			recvType, ok := fn.Recv.List[0].Type.(*ast.Ident)
			if !ok || recvType.Name != "WSGateway" {
				continue
			}
			seen[fn.Name.Name] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func methodNameFromFunc(fn any) string {
	value := reflect.ValueOf(fn)
	if !value.IsValid() || value.Kind() != reflect.Func {
		return ""
	}
	full := runtime.FuncForPC(value.Pointer()).Name()
	if idx := strings.LastIndex(full, "/"); idx >= 0 {
		full = full[idx+1:]
	}
	if idx := strings.LastIndex(full, "."); idx >= 0 {
		full = full[idx+1:]
	}
	return strings.TrimSuffix(full, "-fm")
}
