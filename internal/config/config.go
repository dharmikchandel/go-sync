// Package config loads server configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	GRPCAddr    string
	ReplicaID   string
	PostgresURL string

	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Bucket    string
	S3UseTLS    bool

	// StartupTimeout bounds how long the server waits for its dependencies
	// (Postgres, S3) to become reachable before giving up.
	StartupTimeout time.Duration
}

// Load reads configuration from the environment. Required variables have no
// defaults so that a misconfigured deployment fails fast instead of silently
// talking to the wrong database.
func Load() (Config, error) {
	hostname, _ := os.Hostname()

	c := Config{
		GRPCAddr:       getenv("GOSYNC_GRPC_ADDR", ":50051"),
		ReplicaID:      getenv("GOSYNC_REPLICA_ID", hostname),
		PostgresURL:    os.Getenv("GOSYNC_POSTGRES_URL"),
		S3Endpoint:     os.Getenv("GOSYNC_S3_ENDPOINT"),
		S3AccessKey:    os.Getenv("GOSYNC_S3_ACCESS_KEY"),
		S3SecretKey:    os.Getenv("GOSYNC_S3_SECRET_KEY"),
		S3Bucket:       getenv("GOSYNC_S3_BUCKET", "gosync-blobs"),
		S3UseTLS:       os.Getenv("GOSYNC_S3_TLS") == "true",
		StartupTimeout: 30 * time.Second,
	}

	required := map[string]string{
		"GOSYNC_POSTGRES_URL":  c.PostgresURL,
		"GOSYNC_S3_ENDPOINT":   c.S3Endpoint,
		"GOSYNC_S3_ACCESS_KEY": c.S3AccessKey,
		"GOSYNC_S3_SECRET_KEY": c.S3SecretKey,
	}
	for name, v := range required {
		if v == "" {
			return Config{}, fmt.Errorf("missing required env var %s", name)
		}
	}
	return c, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
