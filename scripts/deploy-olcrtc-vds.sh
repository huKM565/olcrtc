#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG=""
COMMAND=""
OUTPUT=""
INPUT=""
TOOL=""
ROOMS_FILE=""
LABEL=""
SECONDS=""
declare -a CONFIGS=()
declare -a SECRETS=()
declare -a ROOM_PROVIDERS=()
declare -a ROOM_TRANSPORTS=()
declare -a ROOM_LABELS=()
declare -a ROOM_URLS=()
declare -a ROOM_KEYS=()

usage() {
  printf 'usage: %s --config FILE [--config FILE] check|build|deploy|status|subscription --output FILE|publish --input FILE\n' "${0##*/}" >&2
  printf '       %s provision-salutejazz --tool FILE --rooms-file FILE\n' "${0##*/}" >&2
  printf '       %s --config FILE room-outage --label j1 --seconds 60\n' "${0##*/}" >&2
  exit 2
}

redact() {
  local line secret
  while IFS= read -r line || [ -n "${line}" ]; do
    for secret in "${SECRETS[@]}"; do
      if [ -n "${secret}" ]; then
        line="${line//"${secret}"/[REDACTED]}"
      fi
    done
    printf '%s\n' "${line}"
  done
}

die() {
  printf 'deploy-olcrtc-vds: %s\n' "$*" | redact >&2
  exit 1
}

ssh_run() {
  if [ -n "${SSH_OUTPUT_FILE:-}" ]; then
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${SSH_TARGET}" "$@" > "${SSH_OUTPUT_FILE}" 2> >(redact >&2)
  else
    ssh -o BatchMode=yes -o ConnectTimeout=10 "${SSH_TARGET}" "$@" 2> >(redact >&2) | redact
  fi
}

scp_upload() {
  scp -q "$1" "${SSH_TARGET}:$2" 2> >(redact >&2)
}

require_value() {
  local name="$1"
  if [ -z "${!name-}" ]; then
    die "missing required config field ${name}"
  fi
}

load_config() {
  [ -f "${CONFIG}" ] || die "config not found: ${CONFIG}"
  # shellcheck source=/dev/null
  . "${CONFIG}"

  local name provider index url key previous other other_key port
  for name in SSH_TARGET ARTIFACT_DIR PUBLISH_PATH PUBLISH_URL NGINX_SERVER_FILE VLESS_UUID VLESS_LISTEN OLCRTC_SOURCE XRAY_BINARY; do
    require_value "${name}"
  done
  [[ "${SSH_TARGET}" =~ ^root@[A-Za-z0-9._:-]+$ ]] || die "SSH_TARGET must be root@host"
  if [ -n "${PUBLISH_SSH_TARGET:-}" ]; then
    [[ "${PUBLISH_SSH_TARGET}" =~ ^root@[A-Za-z0-9._:-]+$ ]] || die "PUBLISH_SSH_TARGET must be root@host"
  fi
  [[ "${ARTIFACT_DIR}" = /* && "${ARTIFACT_DIR}" != *[[:space:]]* ]] || die "ARTIFACT_DIR must be an absolute path without whitespace"
  [[ "${PUBLISH_PATH}" =~ ^/var/www/ilyukhaline/[A-Za-z0-9/._-]+$ && "${PUBLISH_PATH}" != *..* ]] || die "PUBLISH_PATH must be an escaped path under /var/www/ilyukhaline"
  [[ "${PUBLISH_URL}" =~ ^https://[^/[:space:]]+/[^?#[:space:]]+$ ]] || die "PUBLISH_URL must be an HTTPS URL without query or fragment"
  [[ "${NGINX_SERVER_FILE}" = /* && "${NGINX_SERVER_FILE}" != *[[:space:]]* ]] || die "NGINX_SERVER_FILE must be an absolute path without whitespace"
  [[ "${VLESS_UUID}" =~ ^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$ ]] || die "VLESS_UUID is not a UUID"
  if [ "${VLESS_PUBLIC_COMPAT:-false}" = true ]; then
    [[ "${VLESS_LISTEN}" =~ ^(0\.0\.0\.0|127\.0\.0\.1|\[::1\]):[0-9]+$ ]] || die "VLESS_LISTEN must use loopback or explicit public compatibility"
  else
    [[ "${VLESS_LISTEN}" =~ ^(127\.0\.0\.1|\[::1\]):[0-9]+$ ]] || die "VLESS_LISTEN must use a loopback address"
  fi
  [[ "${OLCRTC_SOURCE}" = /* ]] || die "OLCRTC_SOURCE must be an absolute path"
  [[ "${XRAY_BINARY}" = /* ]] || die "XRAY_BINARY must be an absolute path"
  port="${VLESS_LISTEN##*:}"
  ((port >= 1 && port <= 65535)) || die "VLESS_LISTEN port is out of range"

  ROOM_PROVIDERS=()
  ROOM_TRANSPORTS=()
  ROOM_LABELS=()
  ROOM_URLS=()
  ROOM_KEYS=()
  SECRETS=("${VLESS_UUID}" "${PUBLISH_URL}")
  [[ "${TELEMOST_ENABLED:-true}" =~ ^(true|false)$ ]] || die "TELEMOST_ENABLED must be true or false"
  [[ "${SALUTEJAZZ_ENABLED:-false}" =~ ^(true|false)$ ]] || die "SALUTEJAZZ_ENABLED must be true or false"
  local -a providers=(MAMBA VKCALLS)
  if [ "${TELEMOST_ENABLED:-true}" = true ]; then
    providers+=(TELEMOST)
  fi
  if [ "${SALUTEJAZZ_ENABLED:-false}" = true ]; then
    require_value SALUTEJAZZ_ROOMS_FILE
    local rooms_file="${SALUTEJAZZ_ROOMS_FILE}"
    [[ "${rooms_file}" = /* && -f "${rooms_file}" ]] || die "SALUTEJAZZ_ROOMS_FILE must be an absolute existing file"
    [ "$(stat -c '%a' "${rooms_file}")" = 600 ] || die "SALUTEJAZZ_ROOMS_FILE must have mode 600"
    # shellcheck source=/dev/null
    . "${rooms_file}"
    providers+=(SALUTEJAZZ)
  fi
  local mamba_count="${MAMBA_COUNT:-4}"
  [[ "${mamba_count}" =~ ^(4|8|12)$ ]] || die "MAMBA_COUNT must be 4, 8 or 12"
  for provider in "${providers[@]}"; do
    local room_count=4
    if [ "${provider}" = MAMBA ]; then
      room_count="${mamba_count}"
    fi
    for ((index = 1; index <= room_count; index++)); do
      name="${provider}_URL_${index}"
      require_value "${name}"
      url="${!name}"
      name="${provider}_KEY_${index}"
      require_value "${name}"
      key="${!name}"
      if [ "${provider}" = SALUTEJAZZ ]; then
        [[ "${url}" =~ ^[a-z0-9]{4,16}:[a-z0-9]{4,16}$ ]] || die "SALUTEJAZZ_URL_${index} must be a code:password room reference"
      else
        [[ "${url}" = https://* && "${url}" != *"'"* && "${url}" != *"#"* ]] || die "${provider}_URL_${index} must be an HTTPS URL without quotes or a fragment"
      fi
      [[ "${key}" =~ ^[0-9a-fA-F]{64}$ ]] || die "${provider}_KEY_${index} must be 64 hexadecimal characters"
      if [ "${provider}" = TELEMOST ]; then
        [[ "${url}" =~ ^https://telemost\.yandex\.ru/j/[A-Za-z0-9_-]+$ ]] ||
          die "TELEMOST_URL_${index} must be a permanent clean https://telemost.yandex.ru/j/ROOM URL"
      fi
      for ((previous = 1; previous < index; previous++)); do
        other="${provider}_URL_${previous}"
        other_key="${provider}_KEY_${previous}"
        [ "${url}" != "${!other}" ] || die "duplicate ${provider} room URL at indexes ${previous} and ${index}"
        [ "${key}" = "${!other_key}" ] || die "${provider} keys must match for the ##rooms subscription format"
      done
      ROOM_PROVIDERS+=("${provider,,}")
      if [ "${provider}" = SALUTEJAZZ ]; then
        ROOM_LABELS+=("J${index}")
      else
        ROOM_LABELS+=("${provider:0:1}${index}")
      fi
      ROOM_URLS+=("${url}")
      ROOM_KEYS+=("${key}")
      if [ "${provider}" = MAMBA ] || [ "${provider}" = SALUTEJAZZ ]; then
        ROOM_TRANSPORTS+=("datachannel")
      else
        ROOM_TRANSPORTS+=("vp8channel")
      fi
      SECRETS+=("${url}" "${key}")
    done
  done
}

check_remote() {
  # shellcheck disable=SC2016
  ssh_run '
    printf "== OS ==\n"
    if [ -r /etc/os-release ]; then . /etc/os-release; printf "%s\n" "${PRETTY_NAME:-unknown}"; fi
    uname -m
    printf "== Memory ==\n"
    awk "/MemTotal|MemAvailable/ {print}" /proc/meminfo
    printf "== Disk ==\n"
    df -h /
    printf "== Listen ports ==\n"
    ss -ltn
    printf "== Existing IlyukhaLine units ==\n"
    systemctl list-unit-files "ilyukhaline-*" --no-legend 2>/dev/null || true
    printf "== Existing cores and VPN services ==\n"
    command -v olcrtc || true
    command -v xray || true
    systemctl list-units --all --type=service --no-legend 2>/dev/null |
      grep -Ei "ilyukhaline|olcrtc|xray|3x-ui|hysteria|amnezia" || true
  '
}

build_local() {
  [ -f "${OLCRTC_SOURCE}/magefile.go" ] || die "OLCRTC_SOURCE is not an olcRTC checkout"
  [ -x "${XRAY_BINARY}" ] || die "XRAY_BINARY is not executable"
  if [ -d "${OLCRTC_SOURCE}/.git" ]; then
    git -C "${OLCRTC_SOURCE}" diff --quiet || die "olcRTC source has uncommitted changes"
    git -C "${OLCRTC_SOURCE}" diff --cached --quiet || die "olcRTC source has staged changes"
  fi
  mkdir -p "${ARTIFACT_DIR}"
  (
    cd "${OLCRTC_SOURCE}"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 mage build
  )
  [ -f "${OLCRTC_SOURCE}/build/olcrtc-linux-amd64" ] || die "local olcRTC build produced no binary"
  cp "${OLCRTC_SOURCE}/build/olcrtc-linux-amd64" "${ARTIFACT_DIR}/olcrtc-linux-amd64"
  cp "${XRAY_BINARY}" "${ARTIFACT_DIR}/xray"
  chmod 755 "${ARTIFACT_DIR}/olcrtc-linux-amd64" "${ARTIFACT_DIR}/xray"
  (
    cd "${ARTIFACT_DIR}"
    sha256sum olcrtc-linux-amd64 > olcrtc-linux-amd64.sha256
    sha256sum xray > xray.sha256
  )
  printf 'built local Linux amd64 olcRTC and staged Xray in %s\n' "${ARTIFACT_DIR}"
}

vless_target() {
  local host port
  host="${VLESS_LISTEN%:*}"
  host="${host#[}"
  host="${host%]}"
  port="${VLESS_LISTEN##*:}"
  if [ "${host}" = 0.0.0.0 ] || [ "${host}" = :: ]; then
    host=127.0.0.1
  fi
  if [[ "${host}" = *:* ]]; then
    printf '[%s]:%s\n' "${host}" "${port}"
  else
    printf '%s:%s\n' "${host}" "${port}"
  fi
}

render_room() {
  local index="$1" engine_provider file
  case "${ROOM_PROVIDERS[$index]}" in
    mamba) engine_provider=jitsi ;;
    vkcalls) engine_provider=vkcalls ;;
    telemost) engine_provider=telemost ;;
    salutejazz) engine_provider=salutejazz ;;
    *) die "unknown provider ${ROOM_PROVIDERS[$index]}" ;;
  esac
  file="${ARTIFACT_DIR}/staging/${ROOM_LABELS[$index],,}.yaml"
  printf '%s\n' \
    'mode: srv' \
    'auth:' \
    "  provider: ${engine_provider}" \
    'room:' \
    "  id: '${ROOM_URLS[$index]}'" \
    'crypto:' \
    "  key: '${ROOM_KEYS[$index]}'" \
    'net:' \
    "  transport: ${ROOM_TRANSPORTS[$index]}" \
    '  dns: "1.1.1.1:53"' > "${file}"
}

render_deployment() {
  local index template host port
  rm -rf "${ARTIFACT_DIR}/staging"
  mkdir -p "${ARTIFACT_DIR}/staging"
  for index in "${!ROOM_LABELS[@]}"; do
    render_room "${index}"
  done
  host="${VLESS_LISTEN%:*}"
  host="${host#[}"
  host="${host%]}"
  port="${VLESS_LISTEN##*:}"
  template="$(<"${REPO_ROOT}/deploy/xray-vless.json.template")"
  template="${template//__VLESS_HOST__/${host}}"
  template="${template//__VLESS_PORT__/${port}}"
  template="${template//__VLESS_UUID__/${VLESS_UUID}}"
  printf '%s\n' "${template}" > "${ARTIFACT_DIR}/staging/xray.json"
  cp "${REPO_ROOT}/deploy/systemd/ilyukhaline-olcrtc@.service" "${ARTIFACT_DIR}/staging/"
  cp "${REPO_ROOT}/deploy/systemd/ilyukhaline-xray.service" "${ARTIFACT_DIR}/staging/"
  printf '[Service]\nRestartSec=300s\n' > "${ARTIFACT_DIR}/staging/vk-restart.conf"
  cp "${ARTIFACT_DIR}/olcrtc-linux-amd64" "${ARTIFACT_DIR}/staging/olcrtc"
  cp "${ARTIFACT_DIR}/xray" "${ARTIFACT_DIR}/staging/xray"
}

deploy_remote() {
  [ -x "${ARTIFACT_DIR}/olcrtc-linux-amd64" ] || die "run build first"
  [ -x "${ARTIFACT_DIR}/xray" ] || die "run build first"
  render_deployment
  local stage file name olcrtc_hash xray_hash unit state attempt
  stage="/tmp/ilyukhaline/deploy-$$"
  unit="ilyukhaline-deploy-$$.service"
  olcrtc_hash="$(awk '{print $1}' "${ARTIFACT_DIR}/olcrtc-linux-amd64.sha256")"
  xray_hash="$(awk '{print $1}' "${ARTIFACT_DIR}/xray.sha256")"
  ssh_run "mkdir -p '${stage}' && chmod 700 '${stage}'" >/dev/null
  for file in "${ARTIFACT_DIR}/staging"/*; do
    name="${file##*/}"
    scp_upload "${file}" "${stage}/${name}"
  done
  scp_upload "${REPO_ROOT}/deploy/remote-deploy.sh" "${stage}/remote-deploy.sh"
  ssh_run systemd-run --unit="${unit}" --property=Type=oneshot --property=RemainAfterExit=yes --no-block \
    bash "${stage}/remote-deploy.sh" "${stage}" "${olcrtc_hash}" "${xray_hash}" "${VLESS_LISTEN##*:}" \
    "$(vless_target)" "${ROOM_LABELS[@],,}" >/dev/null
  for ((attempt = 1; attempt <= 90; attempt++)); do
    state="$(ssh_run systemctl show "${unit}" --property=ActiveState,ExecMainStatus || true)"
    if [[ "${state}" = *ActiveState=failed* ]]; then
      ssh_run journalctl -u "${unit}" --no-pager || true
      ssh_run systemctl stop "${unit}" >/dev/null || true
      die "remote deployment failed"
    fi
    if [[ "${state}" = *ActiveState=active* && "${state}" = *ExecMainStatus=0* ]]; then
      ssh_run journalctl -u "${unit}" --no-pager || true
      ssh_run systemctl stop "${unit}" >/dev/null
      return 0
    fi
    sleep 2
  done
  ssh_run journalctl -u "${unit}" --no-pager || true
  die "remote deployment observation timed out; transaction was left running"
}

status_remote() {
  local index label state xray_state
  printf 'VLESS %s\n' "${VLESS_LISTEN}"
  for index in "${!ROOM_LABELS[@]}"; do
    label="${ROOM_LABELS[$index]}"
    state="$(ssh_run systemctl is-active "ilyukhaline-olcrtc@${label,,}.service" || true)"
    printf '%s %s %s\n' "${label}" "${ROOM_PROVIDERS[$index]}" "${state:-unknown}"
  done
  xray_state="$(ssh_run systemctl is-active ilyukhaline-xray.service || true)"
  printf 'Xray %s health=%s\n' "${xray_state:-unknown}" "$(ssh_run /opt/ilyukhaline/bin/xray run -test -config /etc/ilyukhaline/xray.json >/dev/null && printf ok || printf failed)"
}

fetch_bundle() {
  local destination="$1" labels remote_script
  : > "${destination}"
  chmod 600 "${destination}"
  labels="${ROOM_LABELS[*],,}"
  # shellcheck disable=SC2016
  remote_script='
    printf "###XRAY\n"
    cat /etc/ilyukhaline/xray.json
    for label in __ROOM_LABELS__; do
      printf "###ROOM %s\n" "${label}"
      cat "/etc/ilyukhaline/${label}.yaml"
    done
  '
  remote_script="${remote_script//__ROOM_LABELS__/${labels}}"
  SSH_OUTPUT_FILE="${destination}"
  ssh_run "${remote_script}"
  unset SSH_OUTPUT_FILE
}

subscription_remote() {
  [ -n "${OUTPUT}" ] || usage
  [[ "${OUTPUT}" = /* ]] || die "--output must be an absolute path"
  local work index config group bundle profile_count
  declare -a generator_args=()
  work="${ARTIFACT_DIR}/subscription-work"
  rm -rf "${work}"
  mkdir -p "${work}"
  chmod 700 "${work}"
  index=0
  profile_count=0
  for config in "${CONFIGS[@]}"; do
    CONFIG="${config}"
    load_config
    if ((index == 0)); then
      group=Германия
    else
      group=Финляндия
    fi
    bundle="${work}/vds-${index}.bundle"
    fetch_bundle "${bundle}"
    generator_args+=("--vds" "${group}=${bundle}")
    if [ "${MAMBA_COUNT:-4}" = 12 ]; then
      profile_count=$((profile_count + 4))
    else
      profile_count=$((profile_count + 2))
    fi
    if [ "${TELEMOST_ENABLED:-true}" = true ]; then
      profile_count=$((profile_count + 1))
    fi
    if [ "${SALUTEJAZZ_ENABLED:-false}" = true ]; then
      profile_count=$((profile_count + 1))
    fi
    index=$((index + 1))
  done
  if ! python3 "${REPO_ROOT}/deploy/generate_subscription.py" "${generator_args[@]}" --output "${OUTPUT}"; then
    rm -rf "${work}"
    die "subscription generation failed"
  fi
  rm -rf "${work}"
  printf 'generated %s profiles\n' "${profile_count}"
}

publish_remote() {
  [ -n "${INPUT}" ] || usage
  [ -f "${INPUT}" ] || die "generated subscription not found"
  SSH_TARGET="${PUBLISH_SSH_TARGET:-${SSH_TARGET}}"
  local without_scheme host uri template stage
  without_scheme="${PUBLISH_URL#https://}"
  host="${without_scheme%%/*}"
  uri="/${without_scheme#*/}"
  [ "${PUBLISH_PATH}" = "/var/www/ilyukhaline${uri}" ] || die "PUBLISH_PATH does not match the HTTPS path"
  template="$(<"${REPO_ROOT}/deploy/nginx-ilyukhaline-subscription.conf.template")"
  template="${template//__PUBLISH_URI__/${uri}}"
  template="${template//__PUBLISH_PATH__/${PUBLISH_PATH}}"
  stage="/tmp/ilyukhaline/publish-$$"
  ssh_run "rm -rf '${stage}' && mkdir -p '${stage}' && chmod 700 '${stage}'" >/dev/null
  printf '%s\n' "${template}" > "${ARTIFACT_DIR}/subscription.conf"
  chmod 600 "${ARTIFACT_DIR}/subscription.conf"
  scp_upload "${INPUT}" "${stage}/subscription.txt"
  scp_upload "${ARTIFACT_DIR}/subscription.conf" "${stage}/subscription.conf"
  rm -f "${ARTIFACT_DIR}/subscription.conf"
  ssh_run bash -s "${stage}" "${PUBLISH_PATH}" "${NGINX_SERVER_FILE}" "${host}" < "${REPO_ROOT}/deploy/remote-publish.sh"
}

room_outage() {
  [[ "${LABEL}" =~ ^j[1-4]$ ]] || die "room-outage label must be j1, j2, j3 or j4"
  [[ "${SECONDS}" =~ ^[0-9]+$ ]] || die "room-outage seconds must be a number"
  ((SECONDS >= 1 && SECONDS <= 300)) || die "room-outage seconds must be between 1 and 300"
  OUTAGE_UNIT="ilyukhaline-olcrtc@${LABEL}.service"
  ssh_run systemctl stop "${OUTAGE_UNIT}" >/dev/null
  trap 'ssh_run systemctl restart "${OUTAGE_UNIT}" >/dev/null' EXIT
  sleep "${SECONDS}"
  ssh_run systemctl restart "${OUTAGE_UNIT}" >/dev/null
  trap - EXIT
}

while (($#)); do
  case "$1" in
    --config)
      (($# >= 2)) || usage
      CONFIGS+=("$2")
      shift 2
      ;;
    --output)
      (($# >= 2)) || usage
      OUTPUT="$2"
      shift 2
      ;;
    --input)
      (($# >= 2)) || usage
      INPUT="$2"
      shift 2
      ;;
    --tool)
      (($# >= 2)) || usage
      TOOL="$2"
      shift 2
      ;;
    --rooms-file)
      (($# >= 2)) || usage
      ROOMS_FILE="$2"
      shift 2
      ;;
    --label)
      (($# >= 2)) || usage
      LABEL="$2"
      shift 2
      ;;
    --seconds)
      (($# >= 2)) || usage
      SECONDS="$2"
      shift 2
      ;;
    check|build|deploy|subscription|publish|status|provision-salutejazz|room-outage)
      [ -z "${COMMAND}" ] || usage
      COMMAND="$1"
      shift
      ;;
    *) usage ;;
  esac
done
if [ "${COMMAND}" = provision-salutejazz ]; then
  [ "${#CONFIGS[@]}" -eq 0 ] || usage
  [[ "${TOOL}" = /* && -x "${TOOL}" ]] || die "provision tool must be an absolute executable path"
  [[ "${ROOMS_FILE}" = /* ]] || die "rooms file must be an absolute path"
  python3 "${REPO_ROOT}/deploy/provision_salutejazz.py" --tool "${TOOL}" --output "${ROOMS_FILE}"
  exit $?
fi
[ "${#CONFIGS[@]}" -ge 1 ] && [ -n "${COMMAND}" ] || usage
[ "${#CONFIGS[@]}" -le 2 ] || usage
if [ "${COMMAND}" != subscription ] && [ "${#CONFIGS[@]}" -ne 1 ]; then
  usage
fi

CONFIG="${CONFIGS[0]}"
load_config
case "${COMMAND}" in
  check) check_remote ;;
  build) build_local ;;
  deploy) deploy_remote ;;
  status) status_remote ;;
  subscription) subscription_remote ;;
  publish) publish_remote ;;
  room-outage) room_outage ;;
esac
