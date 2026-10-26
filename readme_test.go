package toshi_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// readmeMarker names the Example whose body the next Go block of README.md
// copies: a bare ExampleName is in the module's example_test.go, and a
// <dir>.ExampleName is in <dir>/example_test.go.
var readmeMarker = regexp.MustCompile(`^<!-- (?:(\w+)\.)?(Example\w*) -->$`)

// TestReadmeMatchesExamples proves each Go block of README.md is the body of
// the Example its marker names, ignoring indentation and the block's imports,
// so the README shows code that compiles.
func TestReadmeMatchesExamples(t *testing.T) {
	blocks := readmeBlocks(t, "README.md")
	if len(blocks) == 0 {
		t.Fatal("README.md has no Go block")
	}
	bodies := map[string]map[string][]string{}
	for _, b := range blocks {
		if b.example == "" {
			t.Errorf("README.md:%d: Go block without a <!-- ExampleName --> marker on the line before it", b.line)
			continue
		}
		if _, ok := bodies[b.dir]; !ok {
			bodies[b.dir] = exampleBodies(t, b.dir)
		}
		body, ok := bodies[b.dir][b.example]
		if !ok {
			t.Errorf("README.md:%d: the marker names %s, which example_test.go does not define", b.line, b.example)
			continue
		}
		if diff := lineDiff(normalize(withoutImports(b.code)), normalize(body)); diff != "" {
			t.Errorf("README.md:%d: the block differs from the body of %s: %s", b.line, b.example, diff)
		}
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

// exampleBodies returns the lines of each Example function's body in
// <dir>/example_test.go.
func exampleBodies(t *testing.T, dir string) map[string][]string {
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
	bodies := map[string][]string{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Example") {
			continue
		}
		start := fset.Position(fn.Body.Lbrace).Offset + 1
		end := fset.Position(fn.Body.Rbrace).Offset
		bodies[fn.Name.Name] = strings.Split(string(src[start:end]), "\n")
	}
	return bodies
}

// withoutImports drops the import declaration that opens a README block.
func withoutImports(lines []string) []string {
	i := 0
	for i < len(lines) && strings.TrimSpace(lines[i]) == "" {
		i++
	}
	if i == len(lines) {
		return nil
	}
	first := strings.TrimSpace(lines[i])
	switch {
	case first == "import (":
		for i < len(lines) && strings.TrimSpace(lines[i]) != ")" {
			i++
		}
		return lines[min(i+1, len(lines)):]
	case strings.HasPrefix(first, "import "):
		return lines[i+1:]
	}
	return lines
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
