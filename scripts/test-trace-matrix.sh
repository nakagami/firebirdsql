#!/usr/bin/env bash
# Isolated live matrix. Requires Docker. No external database is contacted.
set -euo pipefail
cd "$(dirname "$0")/.."
root="$PWD"
version="${1:-5.0.4}"
case "$version" in
  2.5.9) image='jacobalberty/firebird@sha256:6efd02fc057818e3f9b5da77d56997bafd883a582e666cf7e5371aefec7f84ac' ;;
  3.0.10) image='jacobalberty/firebird@sha256:765807beb822a182205259dbcadf5b06df66fa6c51a746ac67d48deb981026d7' ;;
  4.0.6) image='firebirdsql/firebird@sha256:1bef0d3c18c2e1dc3e3de6a2fe7d9e79dbe27e534f95b23915846fadecd8f5dd' ;;
  5.0.0) image='firebirdsql/firebird@sha256:e75a3b245fd5a88c6f464c63f85604b84aa0dc3b8aa1f0afaf7abe25a09419db' ;;
  5.0.3) image='firebirdsql/firebird@sha256:d1e15bdcc0da40bf5e8af24ba6ba1d14b23d0a43fd21faa4ec8b2089feefb3b8' ;;
  5.0.4) image='firebirdsql/firebird@sha256:1529560400fc7847f015c57fc9d70729c44d9a9d8f9000df19c5ce1950b4c202' ;;
  *) echo "unsupported matrix version: $version" >&2; exit 2 ;;
esac
name="firebirdsql-trace-test-$$"
trap 'docker rm -fv "$name" >/dev/null 2>&1 || true' EXIT
# Published ports are unnecessary: the test runner shares the server network,
# including the dynamically allocated auxiliary port used by existing tests.
docker run -d --name "$name" --platform linux/amd64 \
  -e ISC_PASSWORD=masterkey -e FIREBIRD_ROOT_PASSWORD=masterkey "$image" >/dev/null
if [[ "$version" == 3.0.10 ]]; then
  for attempt in {1..30}; do
    if docker exec "$name" test -f /firebird/etc/firebird.conf; then break; fi
    sleep 1
  done
  docker exec "$name" sed -i 's/^DatabaseAccess =.*/DatabaseAccess = Full/' /firebird/etc/firebird.conf
  docker restart "$name" >/dev/null
fi
# Bash's TCP probe is available in the Go image, including Go 1.22.
docker run --rm --platform linux/amd64 --network "container:$name" \
  -v "$root:/work:ro" -v firebirdsql-matrix-mod:/go/pkg/mod \
  -v firebirdsql-matrix-build:/root/.cache/go-build -w /work \
  -e FIREBIRDSQL_TRACE_TEST=1 -e "TEST_PATTERN=${TEST_PATTERN:-.}" \
  "golang:${GO_VERSION:-1.22}-bookworm" bash -c '
    for attempt in {1..60}; do
      if (echo >/dev/tcp/127.0.0.1/3050) 2>/dev/null; then break; fi
      sleep 1
    done
    go test -race -v ./... -run "$TEST_PATTERN" -count=1 -timeout 15m
  '
