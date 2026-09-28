// Package api holds protocol constants shared by the client and server, so
// the client binary doesn't depend on server code.
package api

import "github.com/dharmikchandel/go-sync/internal/chunk"

// UserMetadataKey is the request metadata that names the calling user.
const UserMetadataKey = "x-gosync-user"

// MaxMessageSize allows a full block plus protobuf framing in one message
// (gRPC's default limit is exactly 4 MiB, the same as a block).
const MaxMessageSize = chunk.BlockSize + 1<<20
