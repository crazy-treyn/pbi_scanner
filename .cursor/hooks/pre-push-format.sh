#!/usr/bin/env bash
# Cursor pre-push gate: run make format-fix && make format-check before git push.
set -euo pipefail

input="$(cat)"
command="$(
  printf '%s' "$input" | python3 -c "import json, sys; print(json.load(sys.stdin).get('command', ''))"
)"

if ! printf '%s' "$command" | grep -qE '^git push(\s|$)'; then
  printf '%s\n' '{ "permission": "allow" }'
  exit 0
fi

root="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$root"
log_file="$root/.cursor/hooks/pre-push-format.last-run"

deny() {
  local user_message="$1"
  local agent_message="$2"
  python3 -c "import json, sys; print(json.dumps({'permission': 'deny', 'user_message': sys.argv[1], 'agent_message': sys.argv[2]}))" \
    "$user_message" "$agent_message"
  exit 2
}

if ! make format-fix format-check; then
  deny \
    "Format check failed. Run \`make format-fix\` and \`make format-check\` locally, fix any remaining issues, commit, then push again." \
    "Pre-push format hook blocked git push because make format-check failed."
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
  deny \
    "format-fix changed files. Review the diff, commit the formatting changes, then push again." \
    "Pre-push format hook blocked git push because make format-fix modified the working tree."
fi

{
  printf 'timestamp=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'command=%s\n' "$command"
  printf 'format_fix=ok\n'
  printf 'format_check=ok\n'
  printf 'git_tree_clean=ok\n'
} >"$log_file"

printf '%s\n' '{ "permission": "allow" }'
exit 0
