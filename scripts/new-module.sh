#!/usr/bin/env bash
# Scaffold a d-core module that is born compliant: build.clj, Makefile, .version,
# README, GEMINI.md, design notes, tests and the :test/:lint/:build aliases.
#
#   ./scripts/new-module.sh d-core-foo "What it does" [target-dir] [--local-repo PATH]
set -euo pipefail

module="${1:?usage: new-module.sh <module-name> <description> [target-dir] [--local-repo PATH]}"
description="${2:?a one-line description is required}"
target="${3:-../$module}"
local_repo=""

while [ $# -gt 0 ]; do
  case "$1" in
    --local-repo) local_repo="${2:?--local-repo needs a path}"; shift 2 ;;
    *) shift ;;
  esac
done

if [ -e "$target" ]; then
  echo "refusing to overwrite $target" >&2
  exit 1
fi

suffix="${module#d-core-}"
ns="d-core.${suffix//-/.}"
# Clojure munges hyphens to underscores in paths: d-core.foo-bar -> d_core/foo_bar
ns_path="$(printf '%s' "$ns" | tr '-' '_' | tr '.' '/')"
template_dir="$(cd "$(dirname "$0")" && pwd)/module-template"

mkdir -p "$target"
cp -R "$template_dir/." "$target/"

# placeholder directory names are rendered after the copy
mkdir -p "$target/src/$(dirname "$ns_path")" "$target/test/$(dirname "$ns_path")"
mv "$target/src/{{ns_path}}" "$target/src/$ns_path"
mv "$target/test/{{ns_path}}" "$target/test/$ns_path"
mv "$target/docs/{{module}}.md" "$target/docs/$module.md"

render() {
  local file="$1"
  python3 - "$file" "$module" "$description" "$ns" "$ns_path" "$local_repo" <<'PY'
import sys
path, module, description, ns, ns_path, local_repo = sys.argv[1:7]
s = open(path).read()
s = s.replace("{{module}}", module).replace("{{description}}", description)
s = s.replace("{{ns_path}}", ns_path).replace("{{ns}}", ns)
if local_repo:
    s = s.replace("{:paths", '{:mvn/local-repo "%s"\n :paths' % local_repo, 1)
open(path, "w").write(s)
PY
}

while IFS= read -r -d '' file; do
  case "$file" in
    *.jar|*.class) continue ;;
  esac
  render "$file"
done < <(find "$target" -type f -print0)

echo "created $target ($module, ns $ns)"
echo "next: cd $target && clojure -M:test && clojure -T:build jar"
