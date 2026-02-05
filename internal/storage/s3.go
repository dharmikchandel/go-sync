package storage

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Store struct {
	client *minio.Client
	bucket string
}

func New(endpoint, accessKey, secretKey, bucket string) (*S3Store, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	return &S3Store{client: client, bucket: bucket}, nil
}

func (s *S3Store) Upload(ctx context.Context, key string, reader io.Reader, size int64) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, reader, size, minio.PutObjectOptions{})
	return err
}

func (s *S3Store) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
}

func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

// func (s *S3Store) Copy(ctx context.Context, src, dst string) error {
// 	_, err := s.client.CopyObject(
// 		ctx,
// 		minio.CopyDestOptions{
// 			Bucket: s.bucket,
// 			Object: dst,
// 		},
// 		minio.CopySrcOptions{
// 			Bucket: s.bucket,
// 			Object: src,
// 		},
// 	)
// 	return err
// }

func (s *S3Store) Move(ctx context.Context, src, dst string) error {
	_, err := s.client.CopyObject(
		ctx,
		minio.CopyDestOptions{
			Bucket: s.bucket,
			Object: dst,
		},
		minio.CopySrcOptions{
			Bucket: s.bucket,
			Object: src,
		},
	)
	if err != nil {
		return err
	}

	return s.client.RemoveObject(ctx, s.bucket, src, minio.RemoveObjectOptions{})
}