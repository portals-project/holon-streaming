#!/usr/bin/env bash
#
# HOLON unified experiment runner.
#
# End-to-end automation:
#   AWS check  ->  template selection  ->  start EC2  ->  ssh wait
#   ->  sync files  ->  deploy  ->  trigger start gate
#   ->  monitor for RUNTIME ms  ->  collect results  ->  stop EC2
#
# Usage:
#   ./run-experiment.sh                     # interactive
#   ./run-experiment.sh --template q7-baseline
#   ./run-experiment.sh --list-templates
#   ./run-experiment.sh --dry-run
#   ./run-experiment.sh --keep-running      # skip EC2 stop on exit
#   ./run-experiment.sh --ascii             # plain-ASCII banner
#   ./run-experiment.sh --help

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=experiment/01-setup.sh
source "$REPO_ROOT/experiment/01-setup.sh"    # helpers, args, config, AWS check, EXIT trap
# shellcheck source=experiment/02-template.sh
source "$REPO_ROOT/experiment/02-template.sh" # template selection, knob defaults, summary
# shellcheck source=experiment/03-ec2-setup.sh
source "$REPO_ROOT/experiment/03-ec2-setup.sh" # start EC2, SSH wait, sync files, verify
# shellcheck source=experiment/04-run.sh
source "$REPO_ROOT/experiment/04-run.sh"      # deploy, confirm RUNNING, trigger start gate
# shellcheck source=experiment/05-monitor.sh
source "$REPO_ROOT/experiment/05-monitor.sh"  # runtime progress loop
# shellcheck source=experiment/06-export.sh
source "$REPO_ROOT/experiment/06-export.sh"   # collect results, print summary
