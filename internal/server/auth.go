package server

import (
	"context"
	"regexp"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	gosyncv1 "github.com/dharmikchandel/go-sync/gen/gosync/v1"
	"github.com/dharmikchandel/go-sync/internal/api"
	"github.com/dharmikchandel/go-sync/internal/meta"
)

var validUserName = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)

type userIDKey struct{}

// userID returns the ID the auth interceptor stored in ctx.
func userID(ctx context.Context) int64 {
	return ctx.Value(userIDKey{}).(int64)
}

// authenticate resolves the caller's user from the api.UserMetadataKey
// metadata.
//
// This is identification, not authentication: anyone can claim any name.
// It's a placeholder with the right shape, because the user always comes from
// request metadata that an interceptor checks, never from a message field. Real
// device tokens replace it later without touching any handler.
//
// It resolves the user for every SyncService method except
// GetServerInfo. Health checks and reflection pass through untouched.
func authenticate(repo *meta.Repo) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !strings.HasPrefix(info.FullMethod, "/"+gosyncv1.SyncService_ServiceDesc.ServiceName+"/") ||
			info.FullMethod == gosyncv1.SyncService_GetServerInfo_FullMethodName {
			return handler(ctx, req)
		}

		md, _ := metadata.FromIncomingContext(ctx)
		names := md.Get(api.UserMetadataKey)
		if len(names) != 1 || !validUserName.MatchString(names[0]) {
			return nil, status.Errorf(codes.Unauthenticated,
				"missing or invalid %s metadata (want 1-64 chars of a-z 0-9 _ -)", api.UserMetadataKey)
		}
		id, err := repo.EnsureUser(ctx, names[0])
		if err != nil {
			return nil, err
		}
		return handler(context.WithValue(ctx, userIDKey{}, id), req)
	}
}
