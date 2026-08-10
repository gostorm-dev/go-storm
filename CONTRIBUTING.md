# Contributing to go-storm

Thanks for taking the time to contribute!

## Getting started

```bash
git clone https://github.com/hariomop12/go-storm.git
cd go-storm
go build ./cmd/storm
```

## Running the tests

```bash
go test ./... -race
```

The suite covers request execution, rate limiting, percentiles, the JSON
report, and distributed helpers. Keep it green before opening a PR.

## Project layout

- `pkg/storm` — the load-testing engine (shared by local and distributed mode)
- `internal/dist` — Redis queue, agent, and coordinator for distributed runs
- `cmd/storm` — the `storm` CLI (Cobra commands)
- `internal/config` — CLI flag → engine config wiring

## Code style

- Run `gofmt` and `go vet` before submitting:
  ```bash
  gofmt -w .
  go vet ./...
  ```
- No comments unless they explain *why*, not *what*.
- Exported functions in `pkg/storm` get doc comments — they form the public API.

## Commit messages

Follow the existing style: short imperative summary, then a blank line and a
bullet list of what changed and why.

## Reporting issues

Open a GitHub issue with the `storm run` / `storm run-dist` command you used,
the expected result, and the actual output.
