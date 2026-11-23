#!/bin/sh
set -eu
test -r specs/2026-05-14-42-implement-greeting.md
cat > greeting.sh <<'EOF'
printf '%s\n' 'hello, delivery'
EOF
printf '%s\n' '__LOOPER_RESULT__={"status":"completed","summary":"Implemented greeting script","changedFiles":["greeting.sh"],"issueNumber":42,"issueResolution":"full"}'
