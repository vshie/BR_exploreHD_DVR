#!/usr/bin/env bash
# Patch BR_exploreHD_DVR static UI in a running extension container (lost on image redeploy / reboot from clean image).
set -euo pipefail

HOST="${DEPLOY_HOST:-192.168.2.2}"
USER="${DEPLOY_USER:-pi}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCAL="${ROOT}/app/static/index.html"
REMOTE_TMP="/tmp/br_explorehd_dvr_index.html"

if [[ ! -f "$LOCAL" ]]; then
  echo "Missing $LOCAL" >&2
  exit 1
fi

echo "==> Resolving container on ${USER}@${HOST} ..."
if [[ -n "${DEPLOY_CONTAINER:-}" ]]; then
  CONTAINER="${DEPLOY_CONTAINER}"
else
  CONTAINER="$(
    ssh -o ConnectTimeout=10 "${USER}@${HOST}" \
      "docker ps --format '{{.Names}}' | grep -Ei 'brexplore|explorehd.*dvr' | head -1" || true
  )"
fi
if [[ -z "${CONTAINER}" ]]; then
  echo "No matching container. Try: ssh ${USER}@${HOST} 'docker ps --format \"{{.Names}}\"'" >&2
  exit 1
fi
echo "    Container: ${CONTAINER}"

echo "==> Copying index.html ..."
scp -o ConnectTimeout=10 "${LOCAL}" "${USER}@${HOST}:${REMOTE_TMP}"

echo "==> Installing into container and restarting ..."
ssh -o ConnectTimeout=10 "${USER}@${HOST}" \
  "docker cp '${REMOTE_TMP}' '${CONTAINER}:/app/static/index.html' && docker restart '${CONTAINER}'"

echo "==> Done. Hard-refresh the extension tab (Shift+Reload). In-situ patch is lost if the container is recreated from the image."
