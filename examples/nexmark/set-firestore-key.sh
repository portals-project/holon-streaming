#!/usr/bin/env bash
set -euo pipefail

# Usage: ./toggle_start_flag.sh on|off
if [ $# -ne 1 ] || [[ ! "$1" =~ ^(on|off)$ ]]; then
  echo "Usage: $0 on|off"
  exit 1
fi

# === CONFIGURE THIS ===
# Your Firestore project ID:
TARGET_PROJECT="flawless-empire-458911-b7"
# ======================

# 1) Where this script lives...
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# 2) ...and your repo root is two levels up:
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
KEY_FILE="${PROJECT_ROOT}/.gcp/firebase-user-account.json"

if [ ! -f "$KEY_FILE" ]; then
  echo "❌ Service-account key not found at $KEY_FILE"
  exit 1
fi

# 3) Activate the service account
#echo "→ Activating service account from $KEY_FILE"
#gcloud auth activate-service-account --key-file="$KEY_FILE"

# 4) Set the correct project for gcloud
#echo "→ Setting GCP project to $TARGET_PROJECT"
#gcloud config set project "$TARGET_PROJECT"

# 5) Determine the boolean value
if [ "$1" = "on" ]; then
  BOOLEAN_VALUE=true
else
  BOOLEAN_VALUE=false
fi

echo "→ Setting flags_ruben/start_flag.start to ${BOOLEAN_VALUE}"

# 6) Get an access token from the activated service account
ACCESS_TOKEN="$(gcloud auth print-access-token)"
if [ -z "$ACCESS_TOKEN" ]; then
  echo "❌ Failed to get an access token."
  exit 1
else
  echo "→ Access token obtained successfully."
fi

# 7) Build the Firestore REST URL
DOC_PATH="projects/${TARGET_PROJECT}/databases/(default)/documents/flags_ruben/start_flag"
URL="https://firestore.googleapis.com/v1/${DOC_PATH}?updateMask.fieldPaths=start"

echo "→ Firestore URL: ${URL}"

# 8) Prepare the JSON payload
read -r -d '' PAYLOAD <<EOF || true
{"fields":{"start":{"booleanValue":${BOOLEAN_VALUE}}}}
EOF

echo "→ Payload to send:"
echo "${PAYLOAD}"

# 9) Send the PATCH request
echo "→ Setting flags_ruben/start_flag.start = ${BOOLEAN_VALUE}"
HTTP_STATUS=$(
  curl -sS -o /dev/null -w "%{http_code}" \
    -X PATCH \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H "Content-Type: application/json" \
    --data "${PAYLOAD}" \
    "${URL}"
)

if [ "$HTTP_STATUS" -ge 200 ] && [ "$HTTP_STATUS" -lt 300 ]; then
  echo "✅ Done."
else
  echo "❌. HTTP status: ${HTTP_STATUS}"
  exit 1
fi
