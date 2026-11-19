#!/bin/bash
# Run the live collector detached in a `screen` session, with a hard time limit.
#   usage: ./run_in_screen.sh [duration]        duration for timeout(1), default 8d
#   attach: screen -r trainose        detach: Ctrl-A then D        stop early: screen -r, then Ctrl-C
# After the duration timeout sends SIGTERM and the collector shuts down gracefully
# (-k 300 kills it if it hangs). Run from the project directory.
DURATION=${1:-8d}
cd "$(dirname "$(readlink -f "$0")")/.." || exit 1
mkdir -p data
screen -dmS trainose bash -c "timeout -k 300 $DURATION python3 -m collector \
  --db data/trains.db --archive-dir data/archive --log-file data/collector.log; \
  echo \"exit=\$? \$(date -u +%FT%TZ)\" >> data/exit.txt"
screen -ls | grep trainose
