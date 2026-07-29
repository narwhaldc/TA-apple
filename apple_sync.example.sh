#!/bin/bash
#   apple_sync.example.sh - cron wrapper for TA-apple ingest (design C: file-drop pull).
#
#   Pulls Health Auto Export JSON files from your cloud remote into a local watch
#   dir, then runs the puller. This is deliberately decoupled from any "cloud sync
#   finished" signal: each run ingests whatever has landed; partially-synced days
#   just finish next run (the puller dedup store makes re-sees free).
#
#   SETUP:
#     1. Copy this OUTSIDE the repo (e.g. ~/hae_sync.sh) so your paths/remote name
#        are not committed.
#     2. Edit REMOTE + INBOX + PULLER below to match your setup. INBOX must match
#        watch_dir in tools/apple_targets.json.
#     3. chmod +x ~/hae_sync.sh
#     4. crontab -e  ->  run every 15 min (the puller flock makes overlaps safe;
#        rclone move is a no-op when nothing new has synced):
#          */15 * * * * /home/YOU/hae_sync.sh
#
#   cron runs with a minimal environment, so we set PATH and use absolute paths
#   (rclone often lives in ~/bin; python may be python3.11).

export PATH="$HOME/bin:/usr/local/bin:/usr/bin:$PATH"

REMOTE="YOUR_RCLONE_REMOTE:Health Auto Export/HAE_to_splunk"   # e.g. gdrive:... or dropbox:...
INBOX="$HOME/hae_inbox"                                        # must equal watch_dir in apple_targets.json
PULLER="$HOME/src/TA-apple/tools/apple_to_hec.py"
PYTHON="python3.11"
LOG="$HOME/hae_sync.log"

mkdir -p "$INBOX"

# move (not copy) so the cloud folder stays tidy after transfer; the puller's
# content-hash dedup is the safety net against any overlap. Use "copy" instead if
# you want to retain the cloud originals.
rclone move "$REMOTE" "$INBOX" --include "*.json" --drive-use-trash=false >> "$LOG" 2>&1

# runs after rclone completes (sequential); the puller only sends what is present.
"$PYTHON" "$PULLER" >> "$LOG" 2>&1
