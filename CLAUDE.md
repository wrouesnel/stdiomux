# CLAUDE.md

- Development happens on the private Forgejo, `origin` (will-forge/stdiomux). Push there
  after each commit.
- `github` (wrouesnel/stdiomux) is public. Push to it only when the user asks.
- `upstream` is libp2p/go-yamux, fetch-only. The fork lives in `yamux/`; to merge an
  upstream release, use a subtree merge (`git merge -X subtree=yamux <tag>`).
- Run `go vet ./...`, `go test ./...` and `go test -race ./...` before committing.
