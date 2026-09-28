// Package blob stores immutable file content in S3-compatible object storage.
package blob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Store struct {
	client *minio.Client
	bucket string
}

type Options struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	UseTLS    bool
}

// Open connects to object storage and makes sure the bucket exists, retrying
// until ctx ends so the server tolerates starting before storage is ready.
func Open(ctx context.Context, o Options) (*Store, error) {
	client, err := minio.New(o.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(o.AccessKey, o.SecretKey, ""),
		Secure: o.UseTLS,
	})
	if err != nil {
		return nil, fmt.Errorf("create s3 client: %w", err)
	}
	s := &Store{client: client, bucket: o.Bucket}

	for {
		err = s.ensureBucket(ctx)
		if err == nil {
			return s, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("s3 not reachable: %w", err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (s *Store) ensureBucket(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	err = s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
	// Another replica may have created it between our check and our create.
	if minio.ToErrorResponse(err).Code == "BucketAlreadyOwnedByYou" {
		return nil
	}
	return err
}

// ErrNotFound means no object exists under the key.
var ErrNotFound = errors.New("object not found")

// Put stores data under key, replacing any existing object. Keys are content
// hashes, so a replacement always has identical bytes: concurrent Puts of the
// same key are harmless.
func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

// Get returns the whole object. Objects are single blocks (at most a few MiB),
// so reading them into memory is fine.
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	// io.ReadAll handles readers that return data and io.EOF together, which
	// minio's object reader does on its last read. The old code dropped
	// that final chunk, so small downloads came back empty.
	data, err := io.ReadAll(obj)
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return nil, ErrNotFound
	}
	return data, err
}
