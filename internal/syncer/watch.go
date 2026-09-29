package syncer

import (
	"context"
	"io/fs"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// settleDelay is how long the folder must be quiet after a file event
// before a sync starts. Saving a file often fires several events in a row
// (create, write, write, rename), and one sync should cover them all.
const settleDelay = 300 * time.Millisecond

// Run keeps the folder in sync until ctx is cancelled.
//
// Three things start a cycle:
//   - file events (fsnotify), after the folder settles: a full cycle;
//   - a timer every PollInterval: a pull-only cycle, to get other devices'
//     changes (M4 replaces polling with a push from the server);
//   - a timer every RescanInterval: a full cycle, in case an event was lost.
//
// File events are only hints that something changed. The scan is what
// decides what changed, so a missed or duplicated event can delay a sync
// but never make it wrong.
func (s *Syncer) Run(ctx context.Context) error {
	w, err := s.watch()
	if err != nil {
		return err
	}
	defer w.Close()

	s.runCycle(ctx, true)
	poll := time.NewTicker(s.opts.PollInterval)
	defer poll.Stop()
	rescan := time.NewTicker(s.opts.RescanInterval)
	defer rescan.Stop()
	var settled <-chan time.Time // nil (blocks forever) until an event arrives

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-w.Events:
			if s.onEvent(w, ev) {
				settled = time.After(settleDelay) // restart the wait
			}
		case err := <-w.Errors:
			// Usually a queue overflow: events were dropped. A full scan
			// finds whatever they were about.
			s.log.Warn("file watcher error, rescanning", "err", err)
			settled = time.After(settleDelay)
		case <-settled:
			settled = nil
			s.runCycle(ctx, true)
		case <-poll.C:
			s.runCycle(ctx, false)
		case <-rescan.C:
			s.runCycle(ctx, true)
		}
	}
}

func (s *Syncer) runCycle(ctx context.Context, full bool) {
	// Errors are already logged per file; a failed cycle (say, the server
	// is down) is simply retried by the next trigger.
	if err := s.cycle(ctx, full); err != nil && ctx.Err() == nil {
		s.log.Error("sync cycle failed", "err", err)
	}
}

// watch starts watching every directory in the folder. fsnotify isn't
// recursive, so each directory needs its own watch.
func (s *Syncer) watch() (*fsnotify.Watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := w.Add(s.root); err != nil {
		w.Close()
		return nil, err
	}
	s.watchTree(w, s.root)
	return w, nil
}

// onEvent reports whether ev should trigger a sync, and adds watches for
// new directories. Files created inside a new directory before its watch
// exists are found by the scan that this event triggers anyway.
func (s *Syncer) onEvent(w *fsnotify.Watcher, ev fsnotify.Event) bool {
	if _, ignored := s.rel(ev.Name); ignored {
		return false // our own state and download temp files
	}
	if ev.Has(fsnotify.Create) {
		s.watchTree(w, ev.Name)
	}
	return true
}

func (s *Syncer) watchTree(w *fsnotify.Watcher, dir string) {
	filepath.WalkDir(dir, func(full string, d fs.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		if full != s.root {
			if _, ignored := s.rel(full); ignored {
				return filepath.SkipDir
			}
			if err := w.Add(full); err != nil {
				s.log.Warn("cannot watch directory", "dir", full, "err", err)
			}
		}
		return nil
	})
}
