#!/bin/bash
# Timetable pull after a collector run (docs/DESIGN.md, D24): fetches the last days of the run,
# which the collector's own D-2 rule never reaches. Safe to run repeatedly: it fetches only board
# days that are missing and final, and does nothing otherwise. Schedule it (cron, systemd timer)
# for the morning after the collector stops, or run it by hand.
# Set TRAINOSE_HOME to the project directory if you move this script away from examples/.
#   exit 0  done (or nothing to do)       exit 75  the collector is still running, try later
#   exit 2  that day's board is not final yet, try later     exit 1/3  failed, see the log
# Test overrides: TRAINOSE_DB, TRAINOSE_ARCHIVE, TRAINOSE_PULL_LOG, TRAINOSE_PULL_EXTRA (e.g. --force)
cd "${TRAINOSE_HOME:-$(dirname "$(readlink -f "$0")")/..}" || exit 1
DB=${TRAINOSE_DB:-data/trains.db}
ARCH=${TRAINOSE_ARCHIVE:-data/archive}
LOG=${TRAINOSE_PULL_LOG:-data/lone_pull.log}
mkdir -p "$(dirname "$LOG")"
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }

# single writer: never touch the database while the collector itself is running
if pgrep -af "python3 -m collector --db $DB" | grep -v -e '--board-catchup-only' | grep -q .; then
    log "collector still running on $DB, not pulling now (exit 75)"
    exit 75
fi

# the last data day = the Athens date of the newest stored position
E=$(python3 -I - "$DB" <<'PY'
import datetime as d, sqlite3, sys
from zoneinfo import ZoneInfo
c = sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True)
m = c.execute("SELECT max(ts) FROM position").fetchone()[0]
print(d.datetime.fromtimestamp(m, ZoneInfo("Europe/Athens")).date() if m else "")
PY
)
if [ -z "$E" ]; then log "no positions in $DB, nothing to pull (exit 1)"; exit 1; fi
log "last data day is $E, pulling every board day up to it"

# shellcheck disable=SC2086
python3 -m collector --db "$DB" --archive-dir "$ARCH" --board-catchup-only --through "$E" ${TRAINOSE_PULL_EXTRA:-} 2>&1 | tee -a "$LOG"
rc=${PIPESTATUS[0]}
if [ "$rc" -ne 0 ]; then log "pull ended with exit code $rc"; exit "$rc"; fi

# verify: every day from start_date to E must now be in board_fetch
MISSING=$(python3 -I - "$DB" "$E" <<'PY'
import datetime as d, sqlite3, sys
c = sqlite3.connect("file:" + sys.argv[1] + "?mode=ro", uri=True)
start = d.date.fromisoformat(c.execute("SELECT value FROM meta WHERE key='start_date'").fetchone()[0])
end = d.date.fromisoformat(sys.argv[2])
have = {r[0] for r in c.execute("SELECT date FROM board_fetch")}
print(" ".join(str(start + d.timedelta(n)) for n in range((end - start).days + 1) if str(start + d.timedelta(n)) not in have))
PY
)
if [ -n "$MISSING" ]; then log "board days still missing: $MISSING (exit 3)"; exit 3; fi
log "board complete from the start date through $E (exit 0)"
