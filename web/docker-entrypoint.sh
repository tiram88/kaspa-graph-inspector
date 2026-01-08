#!/bin/sh
set -eu

replace_in_file() {
  placeholder="$1"
  value="$2"

  if [ -z "$value" ]; then
    return 0
  fi

  escaped=$(printf '%s' "$value" | sed -e 's/[\/&|]/\\&/g')
  find /app -type f \( -name '*.js' -o -name '*.html' -o -name '*.css' -o -name '*.json' -o -name '*.map' \) -print0 \
    | xargs -0 sed -i "s|${placeholder}|${escaped}|g"
}

replace_in_file "__REACT_APP_API_ADDRESS__" "${REACT_APP_API_ADDRESS:-}"
replace_in_file "__REACT_APP_EXPLORER_ADDRESS__" "${REACT_APP_EXPLORER_ADDRESS:-}"
replace_in_file "__REACT_APP_KASPA_LIVE_ADDRESS__" "${REACT_APP_KASPA_LIVE_ADDRESS:-}"

exec "$@"