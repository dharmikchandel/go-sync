// Command go-sync is the go-sync client.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	gosyncv1 "github.com/dharmikchandel/go-sync/gen/gosync/v1"
	"github.com/dharmikchandel/go-sync/internal/client"
)

const usage = `usage: go-sync [-server addr] [-user name] <command> [args]

files:
  put [-base N] <local-file> [remote-path]   upload a file as a new version
  get [-version N] <remote-path> [local-file] download a file (default: current version)
  rm [-base N] <remote-path>                 delete a file (history is kept)
  ls                                         list files
  history <remote-path>                      list a file's versions
  restore <remote-path> <version>            make an old version current again
  changes [-since N]                         show the change feed after seq N

server:
  info    show which server replica answered and its version
  health  exit 0 if the server reports SERVING (used by container healthchecks)

-base is the version your copy is based on. put and rm default to the
server's current version, which means "overwrite whatever is there".
Pass -base to have a stale write rejected instead.
`

type globals struct {
	server string
	user   string
}

func main() {
	var g globals
	flag.StringVar(&g.server, "server", envOr("GOSYNC_SERVER", "localhost:50051"), "server address")
	flag.StringVar(&g.user, "user", envOr("GOSYNC_USER", "demo"), "user name")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	commands := map[string]func(context.Context, globals, []string) error{
		"put":     put,
		"get":     get,
		"rm":      rm,
		"ls":      ls,
		"history": history,
		"restore": restore,
		"changes": changes,
		"info":    info,
		"health":  checkHealth,
	}
	cmd, ok := commands[flag.Arg(0)]
	if !ok {
		flag.Usage()
		os.Exit(2)
	}
	if err := cmd(ctx, g, flag.Args()[1:]); err != nil {
		// Show a server error's message, not the "rpc error: code = ..." wrapper.
		if st, ok := status.FromError(err); ok {
			err = errors.New(st.Message())
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func (g globals) dial() (*client.Client, error) {
	return client.Dial(g.server, g.user)
}

// parseArgs parses a subcommand's flags and checks its positional arg count.
func parseArgs(fs *flag.FlagSet, args []string, min, max int) ([]string, error) {
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() < min || fs.NArg() > max {
		return nil, errors.New("wrong number of arguments (see go-sync -h)")
	}
	return fs.Args(), nil
}

func put(ctx context.Context, g globals, args []string) error {
	fs := flag.NewFlagSet("put", flag.ContinueOnError)
	base := fs.Int64("base", -1, "version the local file is based on")
	args, err := parseArgs(fs, args, 1, 2)
	if err != nil {
		return err
	}
	local := args[0]
	remote := filepath.Base(local)
	if len(args) == 2 {
		remote = args[1]
	}

	c, err := g.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	if *base < 0 {
		if *base, err = c.CurrentVersion(ctx, remote); err != nil {
			return err
		}
	}
	v, err := c.Upload(ctx, local, remote, *base)
	if err != nil {
		return err
	}
	fmt.Printf("%s -> v%d (%s)\n", v.Path, v.Version, formatSize(v.Size))
	return nil
}

func get(ctx context.Context, g globals, args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	version := fs.Int64("version", 0, "version to download (0 = current)")
	args, err := parseArgs(fs, args, 1, 2)
	if err != nil {
		return err
	}
	remote := args[0]
	local := path.Base(remote)
	if len(args) == 2 {
		local = args[1]
	}

	c, err := g.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	v, err := c.Download(ctx, remote, *version, local)
	if err != nil {
		return err
	}
	fmt.Printf("%s v%d -> %s (%s)\n", v.Path, v.Version, local, formatSize(v.Size))
	return nil
}

func rm(ctx context.Context, g globals, args []string) error {
	fs := flag.NewFlagSet("rm", flag.ContinueOnError)
	base := fs.Int64("base", -1, "version you expect to delete")
	args, err := parseArgs(fs, args, 1, 1)
	if err != nil {
		return err
	}

	c, err := g.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	if *base < 0 {
		if *base, err = c.CurrentVersion(ctx, args[0]); err != nil {
			return err
		}
	}
	v, err := c.Delete(ctx, args[0], *base)
	if err != nil {
		return err
	}
	fmt.Printf("%s deleted (tombstone v%d)\n", v.Path, v.Version)
	return nil
}

func ls(ctx context.Context, g globals, args []string) error {
	if _, err := parseArgs(flag.NewFlagSet("ls", flag.ContinueOnError), args, 0, 0); err != nil {
		return err
	}
	c, err := g.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	// The change feed from seq 0 is a full listing: every file, at its
	// current version, deleted ones included.
	files, _, err := c.Changes(ctx, 0)
	if err != nil {
		return err
	}
	slices.SortFunc(files, func(a, b *gosyncv1.FileVersion) int { return strings.Compare(a.Path, b.Path) })
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "PATH\tVERSION\tSIZE\tMODIFIED")
	for _, f := range files {
		if !f.Deleted {
			fmt.Fprintf(w, "%s\tv%d\t%s\t%s\n", f.Path, f.Version, formatSize(f.Size), formatTime(f))
		}
	}
	return w.Flush()
}

func history(ctx context.Context, g globals, args []string) error {
	args, err := parseArgs(flag.NewFlagSet("history", flag.ContinueOnError), args, 1, 1)
	if err != nil {
		return err
	}
	c, err := g.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	versions, err := c.History(ctx, args[0])
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "VERSION\tSIZE\tCREATED\t")
	for _, v := range versions {
		note := ""
		if v.Deleted {
			note = "deleted"
		}
		fmt.Fprintf(w, "v%d\t%s\t%s\t%s\n", v.Version, formatSize(v.Size), formatTime(v), note)
	}
	return w.Flush()
}

func restore(ctx context.Context, g globals, args []string) error {
	args, err := parseArgs(flag.NewFlagSet("restore", flag.ContinueOnError), args, 2, 2)
	if err != nil {
		return err
	}
	version, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || version < 1 {
		return fmt.Errorf("invalid version %q", args[1])
	}

	c, err := g.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	base, err := c.CurrentVersion(ctx, args[0])
	if err != nil {
		return err
	}
	v, err := c.Restore(ctx, args[0], version, base)
	if err != nil {
		return err
	}
	fmt.Printf("%s: v%d restored as v%d (no data re-uploaded)\n", v.Path, version, v.Version)
	return nil
}

func changes(ctx context.Context, g globals, args []string) error {
	fs := flag.NewFlagSet("changes", flag.ContinueOnError)
	since := fs.Int64("since", 0, "only show changes after this seq")
	if _, err := parseArgs(fs, args, 0, 0); err != nil {
		return err
	}
	c, err := g.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	list, next, err := c.Changes(ctx, *since)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "SEQ\tPATH\tVERSION\tCHANGE")
	for _, f := range list {
		change := "updated"
		if f.Deleted {
			change = "deleted"
		} else if f.Version == 1 {
			change = "created"
		}
		fmt.Fprintf(w, "%d\t%s\tv%d\t%s\n", f.Seq, f.Path, f.Version, change)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	fmt.Printf("\nnext cursor: -since %d\n", next)
	return nil
}

func info(ctx context.Context, g globals, _ []string) error {
	c, err := g.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.RPC().GetServerInfo(ctx, &gosyncv1.GetServerInfoRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("server %s (replica %s)\n", resp.Version, resp.ReplicaId)
	return nil
}

// checkHealth uses a bare connection: the container healthcheck runs it and
// has no user.
func checkHealth(ctx context.Context, g globals, _ []string) error {
	conn, err := grpc.NewClient(g.server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("server is %s", resp.Status)
	}
	fmt.Println("SERVING")
	return nil
}

func formatSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func formatTime(v *gosyncv1.FileVersion) string {
	return v.CreatedAt.AsTime().Local().Format("2006-01-02 15:04:05")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
