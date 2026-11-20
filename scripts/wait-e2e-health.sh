#!/usr/bin/env bash
# Probe the published host port used by the E2E client.
set -eu
url=http://127.0.0.1:27282/health
echo "Waiting for $url"
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
    remaining=$((deadline - SECONDS))
    (( remaining > 0 )) || break
    if status=$(curl --silent --max-time "$((remaining < 2 ? remaining : 2))" \
        --output /dev/null --write-out '%{http_code}' "$url") && [[ $status == 200 ]]; then
        echo "ready"
        exit 0
    fi
    (( SECONDS < deadline )) && sleep 1
done
echo "E2E server did not return HTTP 200 within 60 seconds" >&2
exit 1
