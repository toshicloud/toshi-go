package toshi_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// readmeMarker names the Example whose body the next Go block of README.md
// copies: a bare ExampleName is in the module's example_test.go, and a
// <dir>.ExampleName is in <dir>/example_test.go.
var readmeMarker = regexp.MustCompile(`^<!-- (?:(\w+)\.)?(Example\w*) -->$`)

// TestReadmeMatchesExamples proves each Go block of README.md and of the pages
// of docs/ is the Example its marker names: its import declaration lists
// exactly the packages the Example's body uses, and the rest is that body,
// ignoring indentation. A block is then a program's imports and the body of
// its main, and compiles as it stands.
func TestReadmeMatchesExamples(t *testing.T) {
	pages, err := filepath.Glob("docs/*.md")
	if err != nil {
		t.Fatal(err)
	}
	bodies := map[string]map[string]example{}
	total := 0
	for _, page := range append([]string{"README.md"}, pages...) {
		blocks := readmeBlocks(t, page)
		total += len(blocks)
		for _, b := range blocks {
			if b.example == "" {
				t.Errorf("%s:%d: Go block without a <!-- ExampleName --> marker on the line before it", page, b.line)
				continue
			}
			if _, ok := bodies[b.dir]; !ok {
				bodies[b.dir] = exampleBodies(t, b.dir)
			}
			ex, ok := bodies[b.dir][b.example]
			if !ok {
				t.Errorf("%s:%d: the marker names %s, which example_test.go does not define", page, b.line, b.example)
				continue
			}
			imports, code := splitImports(t, page, b)
			if !slices.Equal(imports, ex.imports) {
				t.Errorf("%s:%d: the block imports %q; the body of %s uses %q", page, b.line, imports, b.example, ex.imports)
			}
			if diff := lineDiff(normalize(code), normalize(ex.body)); diff != "" {
				t.Errorf("%s:%d: the block differs from the body of %s: %s", page, b.line, b.example, diff)
			}
		}
	}
	if total == 0 {
		t.Fatal("README.md and docs/ have no Go block")
	}
}

// readmeBlock is one Go block of README.md: the line of its opening fence,
// the directory of the Example its marker names, that Example, and its lines.
type readmeBlock struct {
	line    int
	dir     string
	example string
	code    []string
}

func readmeBlocks(t *testing.T, path string) []readmeBlock {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var blocks []readmeBlock
	var current *readmeBlock
	previous := ""
	for i, line := range strings.Split(string(src), "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case current != nil && trimmed == "```":
			blocks = append(blocks, *current)
			current = nil
		case current != nil:
			current.code = append(current.code, line)
		case trimmed == "```go":
			current = &readmeBlock{line: i + 1}
			if m := readmeMarker.FindStringSubmatch(previous); m != nil {
				current.dir = m[1]
				current.example = m[2]
			}
		}
		if trimmed != "" {
			previous = trimmed
		}
	}
	if current != nil {
		t.Fatalf("%s:%d: Go block is not closed", path, current.line)
	}
	return blocks
}

// example is one Example function: the lines of its body, and the imports of
// its file that the body uses, sorted, each as an import declaration writes
// it (a path, or a name and a path).
type example struct {
	body    []string
	imports []string
}

// exampleBodies returns each Example function of <dir>/example_test.go.
func exampleBodies(t *testing.T, dir string) map[string]example {
	t.Helper()
	path := filepath.Join(dir, "example_test.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	// The file's imports by the name its code calls each package by.
	byName := map[string]*ast.ImportSpec{}
	for _, spec := range file.Imports {
		byName[importName(spec)] = spec
	}
	bodies := map[string]example{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Example") {
			continue
		}
		start := fset.Position(fn.Body.Lbrace).Offset + 1
		end := fset.Position(fn.Body.Rbrace).Offset
		used := map[string]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			// A package name resolves to no object in the file.
			if id, ok := sel.X.(*ast.Ident); ok && id.Obj == nil {
				if spec, ok := byName[id.Name]; ok {
					used[importText(spec)] = true
				}
			}
			return true
		})
		imports := make([]string, 0, len(used))
		for text := range used {
			imports = append(imports, text)
		}
		slices.Sort(imports)
		bodies[fn.Name.Name] = example{body: strings.Split(string(src[start:end]), "\n"), imports: imports}
	}
	return bodies
}

// importName is the name code calls an imported package by: its explicit
// name, else its path's name.
func importName(spec *ast.ImportSpec) string {
	if spec.Name != nil {
		return spec.Name.Name
	}
	return pathName(spec)
}

// pathName is the last element of an import's path that is not a major
// version, the name its package is declared with.
func pathName(spec *ast.ImportSpec) string {
	parts := strings.Split(strings.Trim(spec.Path.Value, `"`), "/")
	last := parts[len(parts)-1]
	if len(parts) > 1 && regexp.MustCompile(`^v[0-9]+$`).MatchString(last) {
		last = parts[len(parts)-2]
	}
	return last
}

// importText is an import as a declaration writes it: the path, after an
// explicit name that differs from the path's name.
func importText(spec *ast.ImportSpec) string {
	if spec.Name != nil && spec.Name.Name != pathName(spec) {
		return spec.Name.Name + " " + spec.Path.Value
	}
	return spec.Path.Value
}

// splitImports splits a README block into the imports its opening import
// declaration lists, sorted as exampleBodies sorts them, and the lines after
// that declaration. A block without one imports nothing.
func splitImports(t *testing.T, page string, b readmeBlock) ([]string, []string) {
	t.Helper()
	lines := b.code
	i := 0
	for i < len(lines) && strings.TrimSpace(lines[i]) == "" {
		i++
	}
	if i == len(lines) {
		return nil, nil
	}
	end := i
	switch first := strings.TrimSpace(lines[i]); {
	case first == "import (":
		for end < len(lines) && strings.TrimSpace(lines[end]) != ")" {
			end++
		}
		end = min(end+1, len(lines))
	case strings.HasPrefix(first, "import "):
		end = i + 1
	default:
		return nil, lines
	}
	src := "package p\n" + strings.Join(lines[i:end], "\n")
	file, err := parser.ParseFile(token.NewFileSet(), page, src, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("%s:%d: the block's import declaration does not parse: %v", page, b.line, err)
	}
	imports := make([]string, 0, len(file.Imports))
	for _, spec := range file.Imports {
		imports = append(imports, importText(spec))
	}
	slices.Sort(imports)
	return imports, lines[end:]
}

// normalize drops each line's indentation and trailing space, and the blank
// lines at either end.
func normalize(lines []string) []string {
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, strings.TrimSpace(line))
	}
	for len(out) > 0 && out[0] == "" {
		out = out[1:]
	}
	for len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	return out
}

// lineDiff describes the first line where got and want differ, or returns ""
// when they are equal.
func lineDiff(got, want []string) string {
	for i := range max(len(got), len(want)) {
		var g, w string
		if i < len(got) {
			g = got[i]
		}
		if i < len(want) {
			w = want[i]
		}
		if i >= len(got) || i >= len(want) || g != w {
			return fmt.Sprintf("line %d of the code is %q, the Example has %q", i+1, g, w)
		}
	}
	return ""
}
