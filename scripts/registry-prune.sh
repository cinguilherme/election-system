#!/usr/bin/env bash
# Keep the newest N *images* per repository in the local registry and reclaim space.
#
#   ./scripts/registry-prune.sh            # keep 10 (default), report only
#   ./scripts/registry-prune.sh --apply    # delete and garbage-collect
#   KEEP=3 ./scripts/registry-prune.sh     # different retention
#
# Retention is by DIGEST, not by tag. That is the honest unit: deleting a
# manifest deletes every tag pointing at it, so counting tags overshoots badly
# whenever one build carries several tags (retagging, or a re-push of identical
# layers). "Keep the last 10 images" means 10 digests.
#
# The registry has no retention policy of its own, and deleting manifests leaves
# the blobs behind until garbage collection runs. GC must not run while the
# registry serves, hence the exec at the end.
#
# Written for bash 3.2: macOS ships it, and `declare -A` is unavailable there.
set -euo pipefail

REGISTRY="${REGISTRY:-localhost:5001}"
REGISTRY_CONTAINER="${REGISTRY_CONTAINER:-election-system-registry-1}"
KEEP="${KEEP:-10}"
MANIFEST_ACCEPT='application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.index.v1+json'
APPLY=0
[[ "${1:-}" == "--apply" ]] && APPLY=1

say() { printf '%s\n' "$*"; }

digest_of() {
  # The Accept header must list every manifest type the registry may hold: a
  # buildx push stores an OCI index, and a HEAD with only the v2 type answers
  # 404 -- which reads as "tag missing" and silently skips retention forever.
  curl -sSI -H "Accept: ${MANIFEST_ACCEPT}" "http://${REGISTRY}/v2/$1/manifests/$2" \
    | tr -d '\r' | awk -F': ' 'tolower($1)=="docker-content-digest"{print $2}'
}

repos=$(curl -fsS "http://${REGISTRY}/v2/_catalog" \
  | python3 -c 'import json,sys;print("\n".join(json.load(sys.stdin).get("repositories",[])))')

if [[ -z "$repos" ]]; then
  say "registry ${REGISTRY}: no repositories yet"
  exit 0
fi

for repo in $repos; do
  tags=$(curl -fsS "http://${REGISTRY}/v2/${repo}/tags/list" \
    | python3 -c 'import json,sys;print("\n".join(json.load(sys.stdin).get("tags") or []))')

  if [[ -z "$tags" ]]; then
    say "${repo}: no tags (already swept)"
    continue
  fi

  # One line per digest: "<digest> <highest tag in that digest's group>"
  grouped=""
  for tag in $tags; do
    digest=$(digest_of "$repo" "$tag")
    [[ -z "$digest" ]] && { say "  ! ${tag}: unresolved, skipping"; continue; }
    grouped="${grouped}${digest} ${tag}
"
  done
  grouped=$(printf '%s' "$grouped" | sort -u | awk '{best[$1]=$2} END {for (d in best) print d, best[d]}')

  total=$(printf '%s\n' "$grouped" | grep -c . || true)
  say "${repo}: ${total} image(s), $(printf '%s\n' "$tags" | grep -c . || true) tag(s)"

  if (( total <= KEEP )); then
    continue
  fi

  doomed=$(printf '%s\n' "$grouped" | sort -k2 -V | head -n $(( total - KEEP )) | awk '{print $1}')

  for digest in $doomed; do
    if (( APPLY )); then
      if curl -fsS -X DELETE "http://${REGISTRY}/v2/${repo}/manifests/${digest}" >/dev/null; then
        say "  deleted ${digest:0:19}…"
      else
        say "  ! ${digest:0:19}…: delete failed"
      fi
    else
      say "  would delete ${digest:0:19}…"
    fi
  done
done

if (( APPLY )); then
  say "garbage-collecting blobs in ${REGISTRY_CONTAINER}…"
  docker exec "$REGISTRY_CONTAINER" /bin/registry garbage-collect \
    --delete-untagged /etc/docker/registry/config.yml >/dev/null
  say "done."
else
  say "(dry run -- pass --apply to delete and garbage-collect)"
fi
