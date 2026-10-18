#!/usr/bin/env bash
# Build the "worker bundle": the compose files with every `build:` section
# removed, plus an .env template.
#
# A worker machine only PULLS. Leaving the build contexts in place means compose
# resolves paths like `../d-core-async-proxy/Dockerfile`, which do not exist
# there -- and on Windows a missing path surfaces as the famously unhelpful
# "GetFileAttributeEx: The system cannot find the file specified".
#
# Interpolation (${REGISTRY}, ${TAG}) is preserved, so the same .env mechanism
# works; only the build keys are dropped.
set -euo pipefail

OUT="${1:-dist/worker}"
mkdir -p "$OUT"

python3 - "$OUT" <<'PY'
import re, sys, pathlib, shutil
out = pathlib.Path(sys.argv[1])
files = ["docker-compose.yaml", "docker-compose.observability.yaml", "docker-compose.load.yaml"]
for f in files:
    src = pathlib.Path(f)
    if not src.exists():
        continue
    text = src.read_text()
    # drop `build:` and its indented block (context/dockerfile/args…)
    text = re.sub(r'^    build:\n(?:      [^\n]*\n)+', '', text, flags=re.M)
    (out / f).write_text(text)
    print(f"  {f}: {text.count('build:')} build section(s) left")
(out / ".env.example").write_text(
    "# Worker machine: point at the machine serving the registry, then\n"
    "#   docker compose -f docker-compose.yaml -f docker-compose.observability.yaml pull\n"
    "#   docker compose -f docker-compose.yaml -f docker-compose.observability.yaml up -d\n"
    "REGISTRY=<registry-host>:5001\n"
    "# TAG=latest\n")
PY

echo "worker bundle written to $OUT"
ls -1 "$OUT"
