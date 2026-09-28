package syncpath

import "testing"

func TestValidate(t *testing.T) {
	valid := []string{"a", "a.txt", "docs/report.pdf", "a/b/c/d", ".hidden", "a..b", "ünïcode/файл"}
	invalid := []string{"", "/abs", "a/", "a//b", "./a", "a/./b", "..", "../a", "a/../b", ".", `a\b`, "a\x00b", "\xff"}

	for _, p := range valid {
		if err := Validate(p); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", p, err)
		}
	}
	for _, p := range invalid {
		if err := Validate(p); err == nil {
			t.Errorf("Validate(%q) = nil, want error", p)
		}
	}
}
