package syncer

import "testing"

var (
	none = Side{}
	a    = Side{Exists: true, Content: [32]byte{'a'}}
	b    = Side{Exists: true, Content: [32]byte{'b'}}
	c    = Side{Exists: true, Content: [32]byte{'c'}}
)

func TestDecide(t *testing.T) {
	for _, tc := range []struct {
		name                string
		base, local, remote Side
		want                Action
	}{
		{"nothing changed", a, a, a, InSync},
		{"never existed", none, none, none, InSync},
		{"created here", none, a, none, Upload},
		{"created on server", none, none, a, Download},
		{"created on both, same content", none, a, a, InSync},
		{"created on both, different content", none, a, b, Conflict},
		{"edited here", a, b, a, Upload},
		{"edited on server", a, a, b, Download},
		{"same edit on both", a, b, b, InSync},
		{"different edits", a, b, c, Conflict},
		{"deleted here", a, none, a, DeleteRemote},
		{"deleted on server", a, a, none, DeleteLocal},
		{"deleted on both", a, none, none, InSync},
		{"deleted here, edited on server", a, none, b, Download},
		{"edited here, deleted on server", a, b, none, Upload},
		{"recreated here after a delete", none, a, none, Upload},
	} {
		if got := Decide(tc.base, tc.local, tc.remote); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

// Check the safety rules on every combination of states, not just the
// hand-picked ones above.
func TestDecideNeverLosesAnEdit(t *testing.T) {
	sides := []Side{none, a, b, c}
	for _, base := range sides {
		for _, local := range sides {
			for _, remote := range sides {
				got := Decide(base, local, remote)
				fail := func(why string) {
					t.Errorf("base=%v local=%v remote=%v: %v %s", base, local, remote, got, why)
				}
				switch got {
				case DeleteLocal, Download:
					// Replacing or removing the local file is only allowed
					// if it holds nothing new (or nothing at all).
					if !local.same(base) && local.Exists {
						fail("destroys an unsynced local edit")
					}
				case DeleteRemote, Upload:
					// Overwriting the server's version is only allowed if it
					// is the one we last saw (the server enforces this too,
					// with the base version check).
					if !remote.same(base) && remote.Exists {
						fail("overwrites a server edit this device never saw")
					}
				case InSync:
					if !local.same(remote) {
						fail("but local and remote differ")
					}
				}
			}
		}
	}
}
