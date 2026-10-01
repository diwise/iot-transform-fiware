package transform

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type templatePart struct{ text, expression string }

// parseTemplate används för både validering och rendering.
func parseTemplate(input string) ([]templatePart, error) {
	var parts []templatePart
	for input != "" {
		start := strings.Index(input, "{{")
		if start < 0 {
			if strings.Contains(input, "}}") {
				return nil, fmt.Errorf("unmatched template closing delimiter")
			}
			parts = append(parts, templatePart{text: input})
			break
		}
		if strings.Contains(input[:start], "}}") {
			return nil, fmt.Errorf("unmatched template closing delimiter")
		}
		parts = append(parts, templatePart{text: input[:start]})
		input = input[start+2:]
		end := -1
		quoted, escaped := false, false
		for i := 0; i < len(input); i++ {
			if quoted {
				if escaped {
					escaped = false
					continue
				}
				if input[i] == '\\' {
					escaped = true
				} else if input[i] == '"' {
					quoted = false
				}
				continue
			}
			if input[i] == '"' {
				quoted = true
				continue
			}
			if strings.HasPrefix(input[i:], "{{") {
				return nil, fmt.Errorf("nested template delimiter")
			}
			if strings.HasPrefix(input[i:], "}}") {
				end = i
				break
			}
		}
		if end < 0 {
			return nil, fmt.Errorf("unbalanced template expression")
		}
		expr := strings.TrimSpace(input[:end])
		if err := validateExpr(expr); err != nil {
			return nil, fmt.Errorf("expression %q: %w", expr, err)
		}
		parts = append(parts, templatePart{expression: expr})
		input = input[end+2:]
	}
	return parts, nil
}

// splitArgs delar enbart på komman utanför citerade nycklar och nästling.
func splitArgs(input string) ([]string, error) {
	var args []string
	start, depth := 0, 0
	quoted, escaped := false, false
	for i, char := range input {
		if quoted {
			if escaped {
				escaped = false
				continue
			}
			if char == '\\' {
				escaped = true
			} else if char == '"' {
				quoted = false
			}
			continue
		}
		switch char {
		case '"':
			quoted = true
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced function arguments")
			}
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(input[start:i]))
				start = i + 1
			}
		}
	}
	if quoted || depth != 0 {
		return nil, fmt.Errorf("unbalanced function arguments")
	}
	args = append(args, strings.TrimSpace(input[start:]))
	if slices.Contains(args, "") {
		return nil, fmt.Errorf("empty function argument")
	}
	return args, nil
}

type pathStep struct {
	key     string
	index   int
	indexed bool
}

func parsePath(path string) ([]pathStep, error) {
	original := path
	invalid := func() ([]pathStep, error) { return nil, fmt.Errorf("invalid field path %q", original) }
	var steps []pathStep
	for path != "" {
		var key string
		if path[0] == '"' {
			end, escaped := -1, false
			for i := 1; i < len(path); i++ {
				if escaped {
					escaped = false
					continue
				}
				if path[i] == '\\' {
					escaped = true
				} else if path[i] == '"' {
					end = i
					break
				}
			}
			if end < 0 {
				return invalid()
			}
			var err error
			key, err = strconv.Unquote(path[:end+1])
			if err != nil {
				return invalid()
			}
			path = path[end+1:]
		} else {
			end := strings.IndexAny(path, ".[\" ](),{}\t\r\n")
			if end < 0 {
				end = len(path)
			}
			key, path = path[:end], path[end:]
		}
		if key == "" {
			return invalid()
		}
		steps = append(steps, pathStep{key: key})
		for strings.HasPrefix(path, "[") {
			end := strings.IndexByte(path, ']')
			if end <= 1 {
				return invalid()
			}
			digits := path[1:end]
			for _, c := range digits {
				if c < '0' || c > '9' {
					return invalid()
				}
			}
			index, err := strconv.Atoi(digits)
			if err != nil {
				return invalid()
			}
			steps = append(steps, pathStep{index: index, indexed: true})
			path = path[end+1:]
		}
		if path == "" {
			break
		}
		if path[0] != '.' || len(path) == 1 {
			return invalid()
		}
		path = path[1:]
	}
	if len(steps) == 0 {
		return invalid()
	}
	return steps, nil
}
