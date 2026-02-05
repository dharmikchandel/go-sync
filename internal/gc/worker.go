package gc

import (
	"context"
	"log"
	"time"

	"go-sync/internal/metadata"
	"go-sync/internal/storage"
)

type Worker struct {
	Repo  *metadata.Repository
	Store *storage.S3Store
}

func NewWorker(repo *metadata.Repository, store *storage.S3Store) *Worker {
	return &Worker{Repo: repo, Store: store}
}

func (w *Worker) Start(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Minute)

	go func() {
		for {
			select {
			case <-ticker.C:
				w.cleanupOrphans(ctx)
				w.pruneOldVersions(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (w *Worker) cleanupOrphans(ctx context.Context) {
	versions, err := w.Repo.ListAllVersions(ctx)
	if err != nil {
		log.Println("gc list versions error:", err)
		return
	}

	for _, v := range versions {
		exists, err := w.Store.Exists(ctx, v.StorageKey)
		if err != nil {
			continue
		}

		if !exists {
			log.Println("removing orphan metadata:", v.StorageKey)
			w.Repo.DeleteVersion(ctx, v.ID)
		}
	}
}

func (w *Worker) pruneOldVersions(ctx context.Context) {
	files, err := w.Repo.ListFiles(ctx)
	if err != nil {
		return
	}

	for _, f := range files {
		versions, err := w.Repo.ListVersionsForFile(ctx, f.ID)
		if err != nil {
			continue
		}

		if len(versions) <= 10 {
			continue
		}

		toDelete := versions[:len(versions)-10]

		for _, v := range toDelete {
			w.Store.Delete(ctx, v.StorageKey)
			w.Repo.DeleteVersion(ctx, v.ID)
		}
	}
}