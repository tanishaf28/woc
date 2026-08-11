#!/bin/bash
cd /home/t_fonsec/woc/scripts
for f in *.sh; do
  if ! grep -qE '\./woc|\$BINARY|\$\{BINARY\}' "$f" 2>/dev/null; then
    continue
  fi
  echo "=== $f ==="
  # top-level VAR=value declarations (not inside functions/heredocs, simple heuristic: starts at column 0, no leading whitespace)
  grep -oE '^[A-Z_][A-Z0-9_]*=' "$f" | sed 's/=$//' | sort -u | while read -r var; do
    # count occurrences of $var or ${var} in the whole file
    count=$(grep -oE "\\\$\{?${var}\}?" "$f" | wc -l)
    if [ "$count" -le 1 ]; then
      echo "  DEAD: $var (referenced $count times total, including its own assignment)"
    fi
  done
done
