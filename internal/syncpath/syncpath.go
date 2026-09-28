// Package syncpath defines what a valid synced file path is. The server and
// the client share these rules so they can never disagree about a path.
package syncpath

import (
	"errors"
	"path"
	"strings"
	"unicode/utf8"
)

const MaxLen = 1024

// Validate reports whether p is a valid path: relative, slash-separated,
// already clean (no ".", "..", empty segments or trailing slash), UTF-8, and
// free of backslashes and NUL so it maps safely onto Windows and Unix.
func Validate(p string) error {
	switch {
	case p == "":
		return errors.New("path is empty")
	case len(p) > MaxLen:
		return errors.New("path is too long")
	case !utf8.ValidString(p):
		return errors.New("path is not valid UTF-8")
	case strings.ContainsAny(p, "\\\x00"):
		return errors.New(`path contains "\" or NUL`)
	case strings.HasPrefix(p, "/"):
		return errors.New("path must be relative")
	case path.Clean(p) != p || p == "." || p == ".." || strings.HasPrefix(p, "../"):
		return errors.New(`path must be clean (no ".", "..", "//" or trailing "/")`)
	}
	return nil
}
