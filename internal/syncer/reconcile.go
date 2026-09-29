package syncer

// Side is one copy of a file: whether it exists, and what's in it.
type Side struct {
	Exists  bool
	Content [32]byte // chunk.ContentID; ignored when !Exists
}

func (a Side) same(b Side) bool {
	return a.Exists == b.Exists && (!a.Exists || a.Content == b.Content)
}

// Action is what to do with one path.
type Action int

const (
	// InSync: this device and the server already agree. Only the local
	// record needs updating.
	InSync Action = iota
	Upload
	DeleteRemote
	Download
	DeleteLocal
	// Conflict: both sides changed the file differently. Keep both: the
	// local edit is moved to a "conflicted copy" and the server's version
	// takes the original name.
	Conflict
)

func (a Action) String() string {
	return [...]string{"in sync", "upload", "delete remote", "download", "delete local", "conflict"}[a]
}

// Decide is a three-way comparison. base is the last state this device and
// the server agreed on; local and remote are the two copies now. Comparing
// each copy with base tells us who changed the file since then.
//
// Rules, in order:
//  1. local == remote: nothing to do (also covers both sides making the same
//     edit, or both deleting).
//  2. only remote changed: apply the server's change here.
//  3. only local changed: send our change to the server.
//  4. both changed, differently:
//     - one side deleted, the other edited: the edit wins. Losing a delete
//     only costs a re-delete; losing an edit loses work.
//     - both edited: conflict, keep both.
//
// Decide is pure (no I/O), so every case is covered by a table test.
func Decide(base, local, remote Side) Action {
	switch {
	case local.same(remote):
		return InSync
	case local.same(base): // only the server changed
		if remote.Exists {
			return Download
		}
		return DeleteLocal
	case remote.same(base): // only this device changed
		if local.Exists {
			return Upload
		}
		return DeleteRemote
	case !local.Exists: // deleted here, edited on the server
		return Download
	case !remote.Exists: // edited here, deleted on the server
		return Upload
	default:
		return Conflict
	}
}
