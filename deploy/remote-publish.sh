#!/usr/bin/env bash
set -euo pipefail

stage="$1"
publish_path="$2"
server_file="$3"
host="$4"
snippet=/etc/nginx/snippets/ilyukhaline-subscription.conf
ready=false

rollback() {
  trap - ERR
  set +e
  if [ "${ready}" = true ]; then
    if [ -f "${stage}/server.backup" ]; then
      cp -a "${stage}/server.backup" "${server_file}"
    else
      rm -f "${server_file}"
    fi
    if [ -f "${stage}/subscription.backup" ]; then
      cp -a "${stage}/subscription.backup" "${publish_path}"
    else
      rm -f "${publish_path}"
    fi
    if [ -f "${stage}/snippet.backup" ]; then
      cp -a "${stage}/snippet.backup" "${snippet}"
    else
      rm -f "${snippet}"
    fi
    nginx -t
    nginx -s reload
  fi
  rm -rf "${stage}"
  exit 1
}
trap rollback ERR
trap 'rm -rf "${stage}"' EXIT

mkdir -p "${stage}" "$(dirname "${publish_path}")" /etc/nginx/snippets
chmod 700 "${stage}"
if [ -f "${server_file}" ]; then
  cp -a "${server_file}" "${stage}/server.backup"
fi
if [ -f "${publish_path}" ]; then
  cp -a "${publish_path}" "${stage}/subscription.backup"
fi
if [ -f "${snippet}" ]; then
  cp -a "${snippet}" "${stage}/snippet.backup"
fi
ready=true
install -m 640 "${stage}/subscription.txt" "${publish_path}.new"
chgrp www-data "${publish_path}.new"
mv -f "${publish_path}.new" "${publish_path}"
install -m 644 "${stage}/subscription.conf" "${snippet}.new"
mv -f "${snippet}.new" "${snippet}"

if [ ! -f "${server_file}" ]; then
  mkdir -p "$(dirname "${server_file}")"
  printf 'server {\n  listen 80;\n  server_name %s;\n  root /var/www/ilyukhaline;\n}\n' "${host}" > "${server_file}"
fi
if [ ! -f "/etc/letsencrypt/live/${host}/fullchain.pem" ]; then
  certbot --nginx --non-interactive --agree-tos --register-unsafely-without-email --redirect -d "${host}"
fi

python3 - "${server_file}" "${host}" "${snippet}" <<'PY'
import sys
from pathlib import Path
path = Path(sys.argv[1])
host = sys.argv[2]
snippet = sys.argv[3]
text = path.read_text()
include = f"include {snippet};"
if include not in text:
    lines = text.splitlines(keepends=True)
    server_start = None
    depth = 0
    inserted = False
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("server") and "{" in stripped and depth == 0:
            server_start = index
            depth = 1
            server_text = ""
            continue
        if server_start is None:
            continue
        server_text += line
        depth += line.count("{") - line.count("}")
        if depth == 0:
            if f"server_name {host};" in server_text:
                lines.insert(index, f"  {include}\n")
                inserted = True
                break
            server_start = None
    if not inserted:
        raise SystemExit(f"no server block for {host}")
    path.write_text("".join(lines))
PY
nginx -t
nginx -s reload
trap - ERR
printf 'subscription published\n'
