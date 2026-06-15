#!/usr/bin/env bash
#
# Sanity-check the AWS CLI setup before running an experiment.
#
# Verifies (in order):
#   1. aws CLI is installed
#   2. Credentials work  (sts get-caller-identity)
#   3. .holon.env values resolve  (describe-instance, key file exists)
#   4. start/stop permissions on the configured EC2 instance  (DryRun)
#
# Exit code 0 on full pass, non-zero if any check fails.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG_FILE="$REPO_ROOT/.holon.env"

c_red()    { printf '\033[31m%s\033[0m' "$*"; }
c_green()  { printf '\033[32m%s\033[0m' "$*"; }
c_yellow() { printf '\033[33m%s\033[0m' "$*"; }

ok()   { echo "  $(c_green '✓') $*"; }
fail() { echo "  $(c_red '✗') $*"; FAILED=1; }
warn() { echo "  $(c_yellow '⚠') $*"; }
hdr()  { echo; echo "==> $*"; }

FAILED=0

# 1. aws CLI installed --------------------------------------------------------
hdr "AWS CLI"
if ! command -v aws >/dev/null 2>&1; then
  fail "aws not found on PATH. Install from https://aws.amazon.com/cli/"
  exit 1
fi
ok "aws found: $(command -v aws)"
ok "version: $(aws --version 2>&1)"

# 2. Credentials --------------------------------------------------------------
hdr "Credentials (sts get-caller-identity)"
if ident=$(aws sts get-caller-identity --output json 2>&1); then
  account=$(echo "$ident" | grep -o '"Account": *"[^"]*"' | head -n1 | sed 's/.*"\(.*\)"/\1/')
  arn=$(echo     "$ident" | grep -o '"Arn": *"[^"]*"'     | head -n1 | sed 's/.*"\(.*\)"/\1/')
  ok "Account: $account"
  ok "ARN:     $arn"
else
  fail "Could not authenticate."
  echo "$ident" | sed 's/^/    /'
  echo
  echo "  Configure with: aws configure"
  echo "  Or set env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION"
  exit 1
fi

# 3. Local .holon.env ---------------------------------------------------------
hdr ".holon.env"
if [[ ! -f "$CONFIG_FILE" ]]; then
  warn "No .holon.env at $CONFIG_FILE"
  warn "Skipping instance/permission checks. Run ./run-experiment.sh once to create it,"
  warn "or copy .holon.env.example -> .holon.env and edit."
  exit 0
fi
# shellcheck disable=SC1090
source "$CONFIG_FILE"
ok "Loaded $CONFIG_FILE"

missing=0
for v in INSTANCE_ID AWS_REGION KEY REMOTE_USER REMOTE_BASE; do
  if [[ -z "${!v:-}" ]]; then
    fail "$v is not set in .holon.env"
    missing=1
  fi
done
(( missing )) && exit 1

ok "INSTANCE_ID=$INSTANCE_ID  AWS_REGION=$AWS_REGION  REMOTE_USER=$REMOTE_USER"

if [[ -f "$KEY" ]]; then
  ok "SSH key present: $KEY"
else
  fail "SSH key not found: $KEY"
fi

# 4. EC2 instance reachability ------------------------------------------------
hdr "EC2 instance ($INSTANCE_ID @ $AWS_REGION)"
desc=$(aws ec2 describe-instances \
  --instance-ids "$INSTANCE_ID" \
  --region "$AWS_REGION" \
  --query 'Reservations[0].Instances[0].{State:State.Name,Type:InstanceType,IP:PublicIpAddress}' \
  --output text 2>&1)
if [[ $? -ne 0 ]]; then
  fail "Could not describe instance:"
  echo "$desc" | sed 's/^/    /'
  exit 1
fi
ok "Found instance — State/Type/IP: $desc"

# 5. start/stop permissions (DryRun) -----------------------------------------
hdr "Permission probe (DryRun start/stop)"
# The DryRun flag returns a specific error when permissions are sufficient.
for action in start-instances stop-instances; do
  out=$(aws ec2 "$action" --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" --dry-run 2>&1 || true)
  if echo "$out" | grep -q "DryRunOperation"; then
    ok "$action: permitted"
  elif echo "$out" | grep -q "UnauthorizedOperation"; then
    fail "$action: NOT permitted by IAM policy"
    echo "$out" | sed 's/^/    /'
  else
    warn "$action: unexpected response"
    echo "$out" | sed 's/^/    /'
  fi
done

echo
if (( FAILED )); then
  echo "$(c_red 'Some checks failed.') Fix the issues above before running ./run-experiment.sh."
  exit 1
fi
echo "$(c_green 'All AWS checks passed.') You are ready to run ./run-experiment.sh."
