// Package buildinfo carries build-time metadata injected via -ldflags.
// It lives in its own package so both the CLI (cmd/storm) and the engine
// (pkg/storm, for result reports) can read the same values — a report that
// cannot name the exact build that produced it is not reproducible
// (AGENTS.md §19).
//
// Defaults cover `go run`/plain `go build`; Makefile and the release
// workflow override them:
//
//	-X github.com/gostorm-dev/go-storm/internal/buildinfo.Version=v0.5.6
package buildinfo

var (
	// Version is the git tag (or "dev" for untagged local builds).
	Version = "dev"
	// Commit is the full commit SHA the binary was built from.
	Commit = "none"
	// Date is the UTC build timestamp (RFC3339).
	Date = "unknown"
)
