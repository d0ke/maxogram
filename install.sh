#!/usr/bin/env bash
set -euo pipefail

APP_NAME="maxogram"
APP_DIR="/opt/maxogram"
ENV_DIR="/etc/maxogram"
ENV_FILE="${ENV_DIR}/maxogram.env"
COMPOSE_FILE="${APP_DIR}/docker-compose.app.yml"
DOCKER_IMAGE="${DOCKER_IMAGE:-docker.io/d0ke/maxogram:latest}"
LEGACY_SYSTEMD_UNIT="/etc/systemd/system/maxogram.service"
LEGACY_CRON_FILE="/etc/cron.d/maxogram-restart"
LEGACY_RESTART_TIMER_SERVICE="/etc/systemd/system/maxogram-restart.service"
LEGACY_RESTART_TIMER_UNIT="/etc/systemd/system/maxogram-restart.timer"

DEFAULT_DB_HOST="127.0.0.1"
DEFAULT_DB_PORT="5432"
DEFAULT_DB_NAME="maxogram"
DEFAULT_DB_USER="maxogram_app"
DEFAULT_DB_SCHEMA="maxogram"

MODE="${1:-}"

OS_ID="unknown"
OS_ID_LIKE=""
DISTRO_FAMILY="generic"
PKG_MANAGER=""
GENERIC_MODE="false"
PACKAGES_REFRESHED="false"

BASE_PACKAGE_GROUPS=()
POSTGRES_PACKAGE_GROUPS=()
DOCKER_PACKAGE_GROUPS=()
POSTGRES_SERVICE_CANDIDATES=()
DOCKER_SERVICE_CANDIDATES=()

POSTGRES_SERVICE_UNIT=""
DOCKER_SERVICE_UNIT=""
POSTGRES_DEFAULT_DATA_DIR=""
SYSTEMCTL_BIN=""
PSQL_BIN=""
CREATEDB_BIN=""
POSTGRES_MAJOR_VERSION=""
POSTGRES_CLUSTER_NAME=""
POSTGRES_BIN_DIR=""
POSTGRES_DATA_DIR=""
POSTGRES_CONFIG_FILE=""
POSTGRES_SOCKET_DIR=""
POSTGRES_PORT=""
POSTGRES_SELECTION_SOURCE=""
POSTGRES_SELECTED_RECORD=""
POSTGRES_DISCOVERY_CANDIDATES=()
POSTGRES_ETC_ROOT="${POSTGRES_ETC_ROOT:-/etc/postgresql}"
POSTGRES_LIB_ROOT="${POSTGRES_LIB_ROOT:-/usr/lib/postgresql}"
POSTGRES_PGSQL_ROOT_PREFIX="${POSTGRES_PGSQL_ROOT_PREFIX:-/usr/pgsql-}"

TG_BOT_TOKEN=""
MAX_BOT_TOKEN=""
DB_HOST="${DEFAULT_DB_HOST}"
DB_PORT="${DEFAULT_DB_PORT}"
DB_NAME="${DEFAULT_DB_NAME}"
DB_USER="${DEFAULT_DB_USER}"
DB_PASSWORD=""
DB_SCHEMA="${DEFAULT_DB_SCHEMA}"

ENV_FILE_ALREADY_PRESENT="false"
LOCAL_DB_TARGET="false"
UPDATE_ROLE_PASSWORD="false"

log() {
  printf '[%s] %s\n' "${APP_NAME}" "$*"
}

warn() {
  printf '[%s] WARNING: %s\n' "${APP_NAME}" "$*" >&2
}

die() {
  printf '[%s] ERROR: %s\n' "${APP_NAME}" "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  sudo bash install.sh auto
  sudo bash install.sh manual
  sudo bash install.sh update

Modes:
  auto   Ask Telegram and MAX bot tokens. If an existing local maxogram_app role
         is found and no env file exists yet, ask for that database password too.
         Existing env values stay available with "press Enter to keep current value".
  manual Ask tokens plus database host, port, name, user, password, and schema.
         Existing env values stay available with "press Enter to keep current value".
  update Reuse /etc/maxogram/maxogram.env, pull the latest Docker image, run
         in-container check-config + db-upgrade, then recreate the container.

Supported install paths:
  - Debian and Ubuntu
  - Fedora and RHEL-family systems
  - openSUSE
  - Arch Linux
  - Generic Linux best-effort fallback via detected package manager or preinstalled tools
EOF
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    die "Run this installer as root."
  fi
}

ensure_systemd_available() {
  if ! have_cmd systemctl; then
    die "systemctl is required because this installer manages Docker and PostgreSQL services."
  fi
  SYSTEMCTL_BIN="$(command -v systemctl)"
}

split_words() {
  local source="$1"
  if [[ -z "${source}" ]]; then
    return 0
  fi
  # shellcheck disable=SC2206
  local parts=( ${source} )
  printf '%s\n' "${parts[@]}"
}

detect_package_manager_from_commands() {
  if have_cmd apt-get; then
    printf 'apt\n'
  elif have_cmd dnf; then
    printf 'dnf\n'
  elif have_cmd zypper; then
    printf 'zypper\n'
  elif have_cmd pacman; then
    printf 'pacman\n'
  fi
}

configure_package_manager() {
  BASE_PACKAGE_GROUPS=()
  POSTGRES_PACKAGE_GROUPS=()
  DOCKER_PACKAGE_GROUPS=()
  POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
  DOCKER_SERVICE_CANDIDATES=("docker" "docker.service")
  POSTGRES_DEFAULT_DATA_DIR=""

  case "${PKG_MANAGER}" in
    apt)
      BASE_PACKAGE_GROUPS=("ca-certificates coreutils curl grep sed gawk procps tar")
      POSTGRES_PACKAGE_GROUPS=("postgresql postgresql-client postgresql-contrib")
      DOCKER_PACKAGE_GROUPS=("docker.io docker-compose-v2" "docker.io docker-compose-plugin" "docker.io")
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
      ;;
    dnf)
      BASE_PACKAGE_GROUPS=("ca-certificates coreutils curl grep sed gawk procps-ng tar")
      POSTGRES_PACKAGE_GROUPS=("postgresql-server postgresql postgresql-contrib")
      DOCKER_PACKAGE_GROUPS=("docker docker-compose-plugin" "moby-engine docker-compose-plugin" "docker")
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
      POSTGRES_DEFAULT_DATA_DIR="/var/lib/pgsql/data"
      ;;
    zypper)
      BASE_PACKAGE_GROUPS=("ca-certificates coreutils curl grep sed gawk procps tar")
      POSTGRES_PACKAGE_GROUPS=("postgresql-server postgresql postgresql-contrib" "postgresql16-server postgresql16 postgresql16-contrib" "postgresql15-server postgresql15 postgresql15-contrib" "postgresql14-server postgresql14 postgresql14-contrib")
      DOCKER_PACKAGE_GROUPS=("docker docker-compose-switch" "docker docker-compose" "moby-engine docker-compose-switch" "moby-engine docker-compose")
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
      POSTGRES_DEFAULT_DATA_DIR="/var/lib/pgsql/data"
      ;;
    pacman)
      BASE_PACKAGE_GROUPS=("ca-certificates coreutils curl grep sed gawk procps-ng tar")
      POSTGRES_PACKAGE_GROUPS=("postgresql")
      DOCKER_PACKAGE_GROUPS=("docker docker-compose" "docker")
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
      POSTGRES_DEFAULT_DATA_DIR="/var/lib/postgres/data"
      ;;
    *)
      BASE_PACKAGE_GROUPS=()
      POSTGRES_PACKAGE_GROUPS=()
      DOCKER_PACKAGE_GROUPS=("docker docker-compose-plugin" "docker.io docker-compose-plugin" "docker docker-compose" "docker.io")
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
      POSTGRES_DEFAULT_DATA_DIR=""
      ;;
  esac
}

detect_platform() {
  local os_release="/etc/os-release"
  if [[ -f "${os_release}" ]]; then
    # shellcheck disable=SC1091
    source "${os_release}"
    OS_ID="${ID:-unknown}"
    OS_ID_LIKE="${ID_LIKE:-}"
  else
    warn "/etc/os-release is missing; falling back to generic Linux detection."
  fi

  case "${OS_ID}" in
    debian|ubuntu)
      DISTRO_FAMILY="debian"
      PKG_MANAGER="apt"
      ;;
    fedora|rhel|centos|rocky|almalinux)
      DISTRO_FAMILY="fedora"
      PKG_MANAGER="dnf"
      ;;
    opensuse*|opensuse-leap|opensuse-tumbleweed|sles|sled)
      DISTRO_FAMILY="opensuse"
      PKG_MANAGER="zypper"
      ;;
    arch|archlinux)
      DISTRO_FAMILY="arch"
      PKG_MANAGER="pacman"
      ;;
    *)
      if [[ " ${OS_ID_LIKE} " == *" debian "* ]]; then
        DISTRO_FAMILY="debian"
        PKG_MANAGER="apt"
      elif [[ " ${OS_ID_LIKE} " == *" fedora "* ]] || [[ " ${OS_ID_LIKE} " == *" rhel "* ]]; then
        DISTRO_FAMILY="fedora"
        PKG_MANAGER="dnf"
      elif [[ " ${OS_ID_LIKE} " == *" suse "* ]]; then
        DISTRO_FAMILY="opensuse"
        PKG_MANAGER="zypper"
      elif [[ " ${OS_ID_LIKE} " == *" arch "* ]]; then
        DISTRO_FAMILY="arch"
        PKG_MANAGER="pacman"
      else
        DISTRO_FAMILY="generic"
        PKG_MANAGER="$(detect_package_manager_from_commands || true)"
        GENERIC_MODE="true"
      fi
      ;;
  esac

  if [[ -z "${PKG_MANAGER}" ]]; then
    DISTRO_FAMILY="generic"
    GENERIC_MODE="true"
  fi

  configure_package_manager

  if [[ "${GENERIC_MODE}" == "true" ]]; then
    if [[ -n "${PKG_MANAGER}" ]]; then
      log "Unknown distro '${OS_ID}' detected; using generic Linux fallback via package manager '${PKG_MANAGER}'."
    else
      log "Unknown distro '${OS_ID}' detected; using generic Linux best-effort fallback with preinstalled tools only."
    fi
  else
    log "Detected ${DISTRO_FAMILY} family with package manager '${PKG_MANAGER}'."
  fi
}

refresh_package_metadata() {
  if [[ "${PACKAGES_REFRESHED}" == "true" ]]; then
    return 0
  fi

  case "${PKG_MANAGER}" in
    apt)
      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      ;;
    zypper)
      zypper --non-interactive refresh
      ;;
    *)
      ;;
  esac

  PACKAGES_REFRESHED="true"
}

try_install_packages() {
  if [[ $# -eq 0 ]]; then
    return 0
  fi

  if [[ -z "${PKG_MANAGER}" ]]; then
    return 1
  fi

  refresh_package_metadata

  case "${PKG_MANAGER}" in
    apt)
      if apt-get install -y "$@"; then
        return 0
      fi
      ;;
    dnf)
      if dnf install -y "$@"; then
        return 0
      fi
      ;;
    zypper)
      if zypper --non-interactive install --auto-agree-with-licenses --no-confirm "$@"; then
        return 0
      fi
      ;;
    pacman)
      if pacman -Sy --noconfirm --needed "$@"; then
        return 0
      fi
      ;;
  esac

  return 1
}

install_from_candidate_groups() {
  local description="$1"
  shift

  if [[ $# -eq 0 ]]; then
    return 1
  fi

  local group
  local package_count
  local group_packages=()
  for group in "$@"; do
    mapfile -t group_packages < <(split_words "${group}")
    package_count="${#group_packages[@]}"
    if [[ "${package_count}" -eq 0 ]]; then
      continue
    fi
    if try_install_packages "${group_packages[@]}"; then
      return 0
    fi
  done

  warn "Unable to install ${description} via package manager '${PKG_MANAGER}'."
  return 1
}

ensure_required_cmd() {
  local command_name="$1"
  local help_text="$2"
  if ! have_cmd "${command_name}"; then
    die "${help_text}"
  fi
}

service_enable_now_candidates() {
  local candidate
  local unit
  for candidate in "$@"; do
    [[ -n "${candidate}" ]] || continue
    if [[ "${candidate}" == *.service || "${candidate}" == *.timer ]]; then
      unit="${candidate}"
    else
      unit="${candidate}.service"
    fi
    if "${SYSTEMCTL_BIN}" enable --now "${unit}" >/dev/null 2>&1; then
      printf '%s\n' "${unit}"
      return 0
    fi
  done
  return 1
}

service_restart_candidates() {
  local candidate
  local unit
  for candidate in "$@"; do
    [[ -n "${candidate}" ]] || continue
    if [[ "${candidate}" == *.service || "${candidate}" == *.timer ]]; then
      unit="${candidate}"
    else
      unit="${candidate}.service"
    fi
    if "${SYSTEMCTL_BIN}" restart "${unit}" >/dev/null 2>&1; then
      printf '%s\n' "${unit}"
      return 0
    fi
  done
  return 1
}

service_reload_or_restart_candidates() {
  local candidate
  local unit
  for candidate in "$@"; do
    [[ -n "${candidate}" ]] || continue
    if [[ "${candidate}" == *.service || "${candidate}" == *.timer ]]; then
      unit="${candidate}"
    else
      unit="${candidate}.service"
    fi
    if "${SYSTEMCTL_BIN}" reload "${unit}" >/dev/null 2>&1; then
      return 0
    fi
    if "${SYSTEMCTL_BIN}" restart "${unit}" >/dev/null 2>&1; then
      return 0
    fi
  done
  return 1
}

find_postgres_binary() {
  local name="$1"
  local preferred_version="${2:-}"
  local preferred_bin_dir="${3:-}"
  local candidate
  local resolved

  if [[ -n "${preferred_bin_dir}" && -x "${preferred_bin_dir}/${name}" ]]; then
    printf '%s\n' "${preferred_bin_dir}/${name}"
    return 0
  fi

  if [[ -n "${preferred_version}" ]]; then
    for candidate in \
      "${POSTGRES_LIB_ROOT}/${preferred_version}/bin/${name}" \
      "${POSTGRES_PGSQL_ROOT_PREFIX}${preferred_version}/bin/${name}"; do
      if [[ -x "${candidate}" ]]; then
        printf '%s\n' "${candidate}"
        return 0
      fi
    done
  fi

  if have_cmd "${name}"; then
    resolved="$(command -v "${name}")"
    if [[ -x "${resolved}" ]]; then
      printf '%s\n' "${resolved}"
      return 0
    fi
  fi

  for candidate in "/usr/bin/${name}"; do
    if [[ -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  for candidate in "${POSTGRES_LIB_ROOT}"/*/bin/"${name}" "${POSTGRES_PGSQL_ROOT_PREFIX}"*/bin/"${name}"; do
    if [[ -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  return 1
}

postgres_bin_dir_for_version() {
  local version="$1"
  local candidate

  [[ -n "${version}" ]] || return 1

  for candidate in \
    "${POSTGRES_LIB_ROOT}/${version}/bin" \
    "${POSTGRES_PGSQL_ROOT_PREFIX}${version}/bin"; do
    if [[ -d "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done

  return 1
}

refresh_postgres_binaries() {
  PSQL_BIN="$(find_postgres_binary psql "${POSTGRES_MAJOR_VERSION:-}" "${POSTGRES_BIN_DIR:-}" || true)"
  CREATEDB_BIN="$(find_postgres_binary createdb "${POSTGRES_MAJOR_VERSION:-}" "${POSTGRES_BIN_DIR:-}" || true)"
}

reset_local_postgres_target() {
  POSTGRES_MAJOR_VERSION=""
  POSTGRES_CLUSTER_NAME=""
  POSTGRES_SERVICE_UNIT=""
  POSTGRES_BIN_DIR=""
  POSTGRES_DATA_DIR=""
  POSTGRES_CONFIG_FILE=""
  POSTGRES_SOCKET_DIR=""
  POSTGRES_PORT=""
  POSTGRES_SELECTION_SOURCE=""
  POSTGRES_SELECTED_RECORD=""
  POSTGRES_DISCOVERY_CANDIDATES=()
}

run_as_postgres() {
  runuser -u postgres -- "$@"
}

escape_sql_literal() {
  printf '%s' "$1" | sed "s/'/''/g"
}

validate_pg_identifier() {
  [[ "$1" =~ ^[a-z_][a-z0-9_]*$ ]]
}

validate_env_value() {
  [[ "$1" != *$'\n'* ]] && [[ "$1" != *$'\r'* ]]
}

normalize_env_file_value() {
  local value="$1"
  if [[ "${value}" == \"*\" && "${value}" == *\" ]]; then
    value="${value:1:${#value}-2}"
    value="${value//\\\\/\\}"
    value="${value//\\\"/\"}"
    value="${value//\\\$/\$}"
    value="${value//\\\`/\`}"
  fi
  printf '%s' "${value}"
}

is_local_db_host() {
  local host="$1"
  case "${host}" in
    ""|127.0.0.1|localhost|::1)
      return 0
      ;;
    /*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}


docker_runtime_db_host() {
  if is_local_db_host "${DB_HOST}" || [[ "${DB_HOST}" == "host.docker.internal" ]]; then
    printf '%s\n' "host.docker.internal"
  else
    printf '%s\n' "${DB_HOST}"
  fi
}

docker_needs_host_gateway_mapping() {
  [[ "$(docker_runtime_db_host)" == "host.docker.internal" ]]
}

ensure_base_packages() {
  if [[ "${#BASE_PACKAGE_GROUPS[@]}" -eq 0 ]]; then
    warn "No known base-package set for this distro. Continuing with whatever tools are already installed."
    return 0
  fi

  install_from_candidate_groups "base packages" "${BASE_PACKAGE_GROUPS[@]}" || true
}

resolve_postgres_data_dir() {
  if [[ -n "${POSTGRES_DEFAULT_DATA_DIR}" ]]; then
    printf '%s\n' "${POSTGRES_DEFAULT_DATA_DIR}"
    return 0
  fi

  if [[ -f /var/lib/postgres/data/PG_VERSION ]] || [[ -d /var/lib/postgres ]]; then
    printf '%s\n' "/var/lib/postgres/data"
  else
    printf '%s\n' "/var/lib/pgsql/data"
  fi
}

postgres_port_from_pid_file() {
  local data_dir="$1"
  local pid_file="${data_dir}/postmaster.pid"
  [[ -f "${pid_file}" ]] || return 1
  sed -n '4p' "${pid_file}" 2>/dev/null | tr -d '[:space:]\r'
}

postgres_socket_dir_from_pid_file() {
  local data_dir="$1"
  local pid_file="${data_dir}/postmaster.pid"
  local socket_dir=""

  [[ -f "${pid_file}" ]] || return 1
  socket_dir="$(sed -n '5p' "${pid_file}" 2>/dev/null | tr -d '\r')"
  if [[ -z "${socket_dir}" ]]; then
    if [[ "${DISTRO_FAMILY}" == "debian" ]]; then
      socket_dir="/var/run/postgresql"
    else
      socket_dir="/tmp"
    fi
  fi
  printf '%s\n' "${socket_dir}"
}

postgres_pid_from_pid_file() {
  local data_dir="$1"
  local pid_file="${data_dir}/postmaster.pid"
  [[ -f "${pid_file}" ]] || return 1
  sed -n '1p' "${pid_file}" 2>/dev/null | tr -d '[:space:]\r'
}

postgres_version_from_data_dir() {
  local data_dir="$1"
  local version_file="${data_dir}/PG_VERSION"
  [[ -f "${version_file}" ]] || return 1
  sed -n '1p' "${version_file}" 2>/dev/null | tr -d '[:space:]\r'
}

build_postgres_candidate() {
  printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
    "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}" "${11}"
}

postgres_candidate_label() {
  local record="$1"
  local version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid
  IFS='|' read -r version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid <<< "${record}"

  if [[ -n "${cluster}" ]]; then
    printf '%s/%s on %s' "${version}" "${cluster}" "${port}"
  else
    printf '%s on %s' "${data_dir}" "${port}"
  fi
}

postgres_candidate_reason_label() {
  local record="$1"
  local version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid
  IFS='|' read -r version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid <<< "${record}"

  if [[ -n "${cluster}" ]]; then
    printf 'cluster %s/%s' "${version}" "${cluster}"
  else
    printf 'instance %s' "${data_dir}"
  fi
}

postgres_die_ambiguous_candidates() {
  local reason="$1"
  shift
  local record

  warn "${reason}"
  for record in "$@"; do
    warn "Candidate: $(postgres_candidate_label "${record}")"
  done
  die "${reason}"
}

postgres_candidate_query_raw() {
  local record="$1"
  local sql="$2"
  local db_name="${3:-postgres}"
  local version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid
  local candidate_psql=""

  IFS='|' read -r version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid <<< "${record}"
  [[ -n "${port}" ]] || return 1
  [[ -n "${socket_dir}" ]] || return 1

  candidate_psql="$(find_postgres_binary psql "${version}" "${bin_dir}" || true)"
  [[ -n "${candidate_psql}" ]] || return 1

  run_as_postgres env PGHOST="${socket_dir}" PGPORT="${port}" \
    "${candidate_psql}" -X -v ON_ERROR_STOP=1 -h "${socket_dir}" -p "${port}" -d "${db_name}" -tAc "${sql}" 2>/dev/null
}

postgres_candidate_query_scalar() {
  local record="$1"
  local sql="$2"
  local db_name="${3:-postgres}"

  postgres_candidate_query_raw "${record}" "${sql}" "${db_name}" | tr -d '[:space:]\r'
}

postgres_candidate_match_score() {
  local record="$1"
  local db_exists=""
  local role_exists=""
  local score=0

  db_exists="$(postgres_candidate_query_scalar "${record}" "SELECT 1 FROM pg_database WHERE datname='$(escape_sql_literal "${DB_NAME}")';" || true)"
  role_exists="$(postgres_candidate_query_scalar "${record}" "SELECT 1 FROM pg_roles WHERE rolname='$(escape_sql_literal "${DB_USER}")';" || true)"

  if [[ "${db_exists}" == "1" ]]; then
    score=$((score + 2))
  fi
  if [[ "${role_exists}" == "1" ]]; then
    score=$((score + 1))
  fi

  printf '%s\n' "${score}"
}

selected_postgres_query_raw() {
  local sql="$1"
  local db_name="${2:-postgres}"

  [[ -n "${PSQL_BIN}" ]] || die "PostgreSQL target is not resolved: psql path is missing."
  [[ -n "${POSTGRES_SOCKET_DIR}" ]] || die "PostgreSQL target is not resolved: socket directory is missing."
  [[ -n "${POSTGRES_PORT}" ]] || die "PostgreSQL target is not resolved: port is missing."

  run_as_postgres env PGHOST="${POSTGRES_SOCKET_DIR}" PGPORT="${POSTGRES_PORT}" \
    "${PSQL_BIN}" -X -v ON_ERROR_STOP=1 -h "${POSTGRES_SOCKET_DIR}" -p "${POSTGRES_PORT}" -d "${db_name}" -tAc "${sql}"
}

selected_postgres_query_scalar() {
  local sql="$1"
  local db_name="${2:-postgres}"
  selected_postgres_query_raw "${sql}" "${db_name}" | tr -d '[:space:]\r'
}

selected_postgres_query_line() {
  local sql="$1"
  local db_name="${2:-postgres}"
  selected_postgres_query_raw "${sql}" "${db_name}" | head -n 1 | tr -d '\r' | sed -e 's/[[:space:]]*$//'
}

list_postgres_service_units() {
  [[ -n "${SYSTEMCTL_BIN}" ]] || return 1
  "${SYSTEMCTL_BIN}" list-units --type=service --all --plain --no-legend 2>/dev/null \
    | awk '{print $1}' \
    | grep -E 'postgres|pgsql' || true
}

detect_postgres_service_unit_for_pid() {
  local pid="$1"
  local unit=""
  local main_pid=""
  local active_state=""
  local units=()

  while IFS= read -r unit; do
    [[ -n "${unit}" ]] || continue
    units+=("${unit}")
  done < <(list_postgres_service_units)

  for unit in "${units[@]}"; do
    main_pid="$("${SYSTEMCTL_BIN}" show -p MainPID --value "${unit}" 2>/dev/null || true)"
    if [[ "${main_pid}" == "${pid}" ]]; then
      printf '%s\n' "${unit}"
      return 0
    fi
  done

  for unit in "${units[@]}"; do
    active_state="$("${SYSTEMCTL_BIN}" show -p ActiveState --value "${unit}" 2>/dev/null || true)"
    if [[ "${active_state}" == "active" ]]; then
      printf '%s\n' "${unit}"
      return 0
    fi
  done

  return 1
}

collect_debian_pg_lsclusters_candidates() {
  local filter="${1:-online}"
  local raw_output=""
  local line=""
  local version cluster port status data_dir config_file service_unit bin_dir socket_dir pid

  POSTGRES_DISCOVERY_CANDIDATES=()
  have_cmd pg_lsclusters || return 1
  raw_output="$(pg_lsclusters 2>/dev/null || true)"
  [[ -n "${raw_output}" ]] || return 1

  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    IFS='|' read -r version cluster port status data_dir <<< "${line}"
    [[ -n "${version}" && -n "${cluster}" ]] || continue
    if [[ "${filter}" == "online" && "${status}" != "online" ]]; then
      continue
    fi
    config_file="${POSTGRES_ETC_ROOT}/${version}/${cluster}/postgresql.conf"
    service_unit="postgresql@${version}-${cluster}.service"
    bin_dir="$(postgres_bin_dir_for_version "${version}" || true)"
    socket_dir="$(postgres_socket_dir_from_pid_file "${data_dir}" || true)"
    pid="$(postgres_pid_from_pid_file "${data_dir}" || true)"
    POSTGRES_DISCOVERY_CANDIDATES+=("$(build_postgres_candidate "${version}" "${cluster}" "${port}" "${status}" "${data_dir}" "${config_file}" "${service_unit}" "${bin_dir}" "${socket_dir}" "pg_lsclusters" "${pid}")")
  done < <(printf '%s\n' "${raw_output}" | awk 'NR > 1 { print $1 "|" $2 "|" $3 "|" $4 "|" $6 }')

  [[ "${#POSTGRES_DISCOVERY_CANDIDATES[@]}" -gt 0 ]]
}

collect_generic_postgres_candidates() {
  local ps_output=""
  local line=""
  local pid=""
  local args=""
  local data_dir=""
  local version=""
  local port=""
  local socket_dir=""
  local config_file=""
  local bin_dir=""
  local service_unit=""
  local seen=" "

  POSTGRES_DISCOVERY_CANDIDATES=()
  have_cmd ps || return 1
  ps_output="$(ps -eo pid=,args= 2>/dev/null || true)"
  [[ -n "${ps_output}" ]] || return 1

  while IFS= read -r line; do
    [[ -n "${line}" ]] || continue
    pid="$(printf '%s\n' "${line}" | awk '{print $1}')"
    args="${line#${pid}}"
    args="${args# }"

    [[ "${args}" == *"postgres:"* ]] && continue
    [[ "${args}" == *"postmaster:"* ]] && continue
    [[ "${args}" == *" -D "* ]] || continue
    if [[ "${args}" =~ (^|[[:space:]])-D[[:space:]]+([^[:space:]]+) ]]; then
      data_dir="${BASH_REMATCH[2]}"
    else
      continue
    fi
    [[ -f "${data_dir}/postmaster.pid" ]] || continue
    if [[ "${seen}" == *" ${data_dir} "* ]]; then
      continue
    fi
    seen="${seen}${data_dir} "

    version="$(postgres_version_from_data_dir "${data_dir}" || true)"
    port="$(postgres_port_from_pid_file "${data_dir}" || true)"
    socket_dir="$(postgres_socket_dir_from_pid_file "${data_dir}" || true)"
    if [[ "${args}" =~ config_file=([^[:space:]]+) ]]; then
      config_file="${BASH_REMATCH[1]}"
    else
      config_file=""
    fi
    bin_dir="$(postgres_bin_dir_for_version "${version}" || true)"
    service_unit="$(detect_postgres_service_unit_for_pid "${pid}" || true)"

    POSTGRES_DISCOVERY_CANDIDATES+=("$(build_postgres_candidate "${version}" "" "${port}" "online" "${data_dir}" "${config_file}" "${service_unit}" "${bin_dir}" "${socket_dir}" "postmaster.pid" "${pid}")")
  done <<< "${ps_output}"

  [[ "${#POSTGRES_DISCOVERY_CANDIDATES[@]}" -gt 0 ]]
}

select_local_postgres_candidate() {
  local record=""
  local matches=()
  local version=""
  local cluster=""
  local port=""
  local status=""
  local data_dir=""
  local config_file=""
  local service_unit=""
  local bin_dir=""
  local socket_dir=""
  local source=""
  local pid=""
  local match_score=0
  local best_score=0
  local best_record=""
  local best_version=-1
  local best_version_matches=()

  if [[ "${ENV_FILE_ALREADY_PRESENT}" == "true" ]] && is_local_db_host "${DB_HOST}" && [[ "${DB_PORT}" =~ ^[0-9]+$ ]]; then
    for record in "${POSTGRES_DISCOVERY_CANDIDATES[@]}"; do
      IFS='|' read -r version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid <<< "${record}"
      if [[ "${port}" == "${DB_PORT}" ]]; then
        matches+=("${record}")
      fi
    done
    if [[ "${#matches[@]}" -eq 1 ]]; then
      POSTGRES_SELECTION_SOURCE="existing env port"
      POSTGRES_SELECTED_RECORD="${matches[0]}"
      return 0
    elif [[ "${#matches[@]}" -gt 1 ]]; then
      postgres_die_ambiguous_candidates "Multiple live PostgreSQL targets match existing env port ${DB_PORT}." "${matches[@]}"
    fi
  fi

  matches=()
  for record in "${POSTGRES_DISCOVERY_CANDIDATES[@]}"; do
    match_score="$(postgres_candidate_match_score "${record}" || true)"
    match_score="${match_score:-0}"
    if (( match_score > best_score )); then
      best_score="${match_score}"
      best_record="${record}"
      matches=("${record}")
    elif (( match_score == best_score && match_score > 0 )); then
      matches+=("${record}")
    fi
  done
  if (( best_score > 0 )); then
    if [[ "${#matches[@]}" -eq 1 ]]; then
      POSTGRES_SELECTION_SOURCE="existing Maxogram role/database"
      POSTGRES_SELECTED_RECORD="${best_record}"
      return 0
    fi
    postgres_die_ambiguous_candidates "Multiple live PostgreSQL targets already contain Maxogram role/database state." "${matches[@]}"
  fi

  for record in "${POSTGRES_DISCOVERY_CANDIDATES[@]}"; do
    IFS='|' read -r version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid <<< "${record}"
    version="${version:-0}"
    if (( version > best_version )); then
      best_version="${version}"
      best_version_matches=("${record}")
    elif (( version == best_version )); then
      best_version_matches+=("${record}")
    fi
  done

  if [[ "${#best_version_matches[@]}" -eq 1 ]]; then
    POSTGRES_SELECTION_SOURCE="highest live major version"
    POSTGRES_SELECTED_RECORD="${best_version_matches[0]}"
    return 0
  fi

  postgres_die_ambiguous_candidates "Multiple live PostgreSQL targets share the highest major version ${best_version}." "${best_version_matches[@]}"
}

attempt_start_highest_debian_cluster() {
  local record=""
  local version=""
  local cluster=""
  local port=""
  local status=""
  local data_dir=""
  local config_file=""
  local service_unit=""
  local bin_dir=""
  local socket_dir=""
  local source=""
  local pid=""
  local best_version=-1
  local matches=()

  collect_debian_pg_lsclusters_candidates "all" || return 1

  for record in "${POSTGRES_DISCOVERY_CANDIDATES[@]}"; do
    IFS='|' read -r version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid <<< "${record}"
    version="${version:-0}"
    if (( version > best_version )); then
      best_version="${version}"
      matches=("${record}")
    elif (( version == best_version )); then
      matches+=("${record}")
    fi
  done

  if [[ "${#matches[@]}" -gt 1 ]]; then
    postgres_die_ambiguous_candidates "No PostgreSQL clusters are online, and multiple offline clusters share the highest major version ${best_version}." "${matches[@]}"
  fi

  record="${matches[0]}"
  IFS='|' read -r version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid <<< "${record}"
  [[ -n "${service_unit}" ]] || die "Could not determine a cluster-specific systemd unit for PostgreSQL ${version}/${cluster}."

  log "No PostgreSQL clusters are online; trying to start ${service_unit}."
  service_enable_now_candidates "${service_unit}" >/dev/null || die "Could not start PostgreSQL cluster ${version}/${cluster} automatically."
}

finalize_local_postgres_target() {
  local record="$1"
  local version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid
  local detected_config=""
  local detected_data_dir=""
  local detected_port=""
  local log_label=""

  IFS='|' read -r version cluster port status data_dir config_file service_unit bin_dir socket_dir source pid <<< "${record}"

  POSTGRES_MAJOR_VERSION="${version}"
  POSTGRES_CLUSTER_NAME="${cluster}"
  POSTGRES_SERVICE_UNIT="${service_unit}"
  POSTGRES_BIN_DIR="${bin_dir}"
  POSTGRES_DATA_DIR="${data_dir}"
  POSTGRES_CONFIG_FILE="${config_file}"
  POSTGRES_SOCKET_DIR="${socket_dir}"
  POSTGRES_PORT="${port}"
  POSTGRES_SELECTION_SOURCE="${POSTGRES_SELECTION_SOURCE} via ${source}"

  refresh_postgres_binaries
  [[ -n "${PSQL_BIN}" ]] || die "Could not locate psql for the selected PostgreSQL target."
  [[ -n "${CREATEDB_BIN}" ]] || die "Could not locate createdb for the selected PostgreSQL target."
  POSTGRES_BIN_DIR="$(dirname "${PSQL_BIN}")"

  if [[ "${DISTRO_FAMILY}" == "debian" ]]; then
    [[ -n "${bin_dir}" ]] || die "Could not locate version-specific PostgreSQL binaries for cluster ${version}/${cluster}."
    [[ "${PSQL_BIN}" == "${bin_dir}/psql" ]] || die "Refusing to use pg_wrapper for cluster ${version}/${cluster}; version-specific psql is required."
    [[ "${CREATEDB_BIN}" == "${bin_dir}/createdb" ]] || die "Refusing to use pg_wrapper for cluster ${version}/${cluster}; version-specific createdb is required."
  fi

  [[ -n "${POSTGRES_SOCKET_DIR}" ]] || die "Could not determine the Unix socket directory for the selected PostgreSQL target."
  [[ -n "${POSTGRES_PORT}" ]] || die "Could not determine the port for the selected PostgreSQL target."

  detected_config="$(selected_postgres_query_line "SHOW config_file;" || true)"
  if [[ -n "${detected_config}" ]]; then
    POSTGRES_CONFIG_FILE="${detected_config}"
  fi
  [[ -n "${POSTGRES_CONFIG_FILE}" && -f "${POSTGRES_CONFIG_FILE}" ]] || die "Could not locate postgresql.conf for the selected PostgreSQL target."

  detected_data_dir="$(selected_postgres_query_line "SHOW data_directory;" || true)"
  if [[ -n "${detected_data_dir}" ]]; then
    POSTGRES_DATA_DIR="${detected_data_dir}"
  fi
  [[ -n "${POSTGRES_DATA_DIR}" ]] || die "Could not determine the data directory for the selected PostgreSQL target."

  detected_port="$(selected_postgres_query_scalar "SHOW port;" || true)"
  [[ -n "${detected_port}" ]] || die "Could not determine the port for the selected PostgreSQL target."
  POSTGRES_PORT="${detected_port}"

  if [[ -z "${POSTGRES_SOCKET_DIR}" ]]; then
    POSTGRES_SOCKET_DIR="$(postgres_socket_dir_from_pid_file "${POSTGRES_DATA_DIR}" || true)"
  fi
  [[ -n "${POSTGRES_SOCKET_DIR}" ]] || die "Could not determine the Unix socket directory for the selected PostgreSQL target."

  if [[ -n "${POSTGRES_CLUSTER_NAME}" ]]; then
    log_label="cluster ${POSTGRES_MAJOR_VERSION}/${POSTGRES_CLUSTER_NAME}"
  else
    log_label="instance ${POSTGRES_DATA_DIR}"
  fi
  log "Selected PostgreSQL ${log_label} on ${POSTGRES_PORT} via ${POSTGRES_SELECTION_SOURCE}."
}

resolve_local_postgres_target() {
  local selected_record=""

  reset_local_postgres_target

  if [[ "${DISTRO_FAMILY}" == "debian" ]] && have_cmd pg_lsclusters; then
    collect_debian_pg_lsclusters_candidates "online" || true
    if [[ "${#POSTGRES_DISCOVERY_CANDIDATES[@]}" -eq 0 ]]; then
      attempt_start_highest_debian_cluster || true
      collect_debian_pg_lsclusters_candidates "online" || true
    fi
  else
    collect_generic_postgres_candidates || true
  fi

  if [[ "${#POSTGRES_DISCOVERY_CANDIDATES[@]}" -eq 0 ]]; then
    die "Could not discover a live local PostgreSQL target. Start PostgreSQL manually and rerun the installer."
  fi

  select_local_postgres_candidate
  selected_record="${POSTGRES_SELECTED_RECORD}"
  [[ -n "${selected_record}" ]] || die "Could not select a local PostgreSQL target."
  finalize_local_postgres_target "${selected_record}"
}

initialize_postgres_cluster_if_needed() {
  local data_dir
  data_dir="$(resolve_postgres_data_dir)"
  [[ -n "${data_dir}" ]] || return 0

  if [[ -f "${data_dir}/PG_VERSION" ]]; then
    return 0
  fi

  if [[ "${PKG_MANAGER}" == "apt" ]]; then
    return 0
  fi

  if have_cmd postgresql-setup; then
    log "Initializing PostgreSQL with postgresql-setup..."
    postgresql-setup --initdb
    return 0
  fi

  local initdb_bin
  initdb_bin="$(find_postgres_binary initdb || true)"
  [[ -n "${initdb_bin}" ]] || die "PostgreSQL is installed but initdb was not found."

  log "Initializing PostgreSQL cluster in ${data_dir}..."
  install -d -m 700 -o postgres -g postgres "${data_dir}"
  run_as_postgres "${initdb_bin}" -D "${data_dir}"
}

ensure_postgres_installed() {
  log "Ensuring PostgreSQL is installed..."

  refresh_postgres_binaries
  if [[ -z "${PSQL_BIN}" || -z "${CREATEDB_BIN}" ]]; then
    if [[ "${#POSTGRES_PACKAGE_GROUPS[@]}" -gt 0 ]]; then
      install_from_candidate_groups "PostgreSQL" "${POSTGRES_PACKAGE_GROUPS[@]}" || true
    else
      warn "No supported package-manager recipe for PostgreSQL on this distro. Expecting PostgreSQL to already be installed."
    fi
    refresh_postgres_binaries
  fi

  [[ -n "${PSQL_BIN}" ]] || die "PostgreSQL client tools are missing. Install PostgreSQL manually and rerun the installer."
  [[ -n "${CREATEDB_BIN}" ]] || die "createdb is missing. Install PostgreSQL client tools and rerun the installer."
  id postgres >/dev/null 2>&1 || die "The 'postgres' system user is missing."

  initialize_postgres_cluster_if_needed

  if POSTGRES_SERVICE_UNIT="$(service_enable_now_candidates "${POSTGRES_SERVICE_CANDIDATES[@]}")"; then
    log "Using PostgreSQL service ${POSTGRES_SERVICE_UNIT}."
  else
    warn "PostgreSQL service name could not be detected automatically; continuing with live target discovery."
  fi
}

detect_postgres_port() {
  [[ -n "${POSTGRES_PORT}" ]] || die "Local PostgreSQL target was not resolved."
  printf '%s\n' "${POSTGRES_PORT}"
}

detect_postgres_file() {
  local setting="$1"
  [[ "${setting}" =~ ^[a-z_]+$ ]] || die "Invalid PostgreSQL setting name: ${setting}"

  if [[ "${setting}" == "config_file" && -n "${POSTGRES_CONFIG_FILE}" ]]; then
    printf '%s\n' "${POSTGRES_CONFIG_FILE}"
    return 0
  fi

  selected_postgres_query_line "SHOW ${setting};"
}

set_postgres_port() {
  local desired_port="$1"
  local current_port="$2"
  local current_target_key=""

  if [[ -z "${desired_port}" || "${desired_port}" == "${current_port}" ]]; then
    return 0
  fi

  [[ "${desired_port}" =~ ^[0-9]+$ ]] || die "PostgreSQL port must be an integer."
  [[ -n "${POSTGRES_CONFIG_FILE}" && -f "${POSTGRES_CONFIG_FILE}" ]] || die "Cannot locate postgresql.conf for the selected PostgreSQL target."
  [[ -n "${POSTGRES_SERVICE_UNIT}" ]] || die "Cannot restart PostgreSQL because the selected service unit is unknown."

  current_target_key="${POSTGRES_MAJOR_VERSION}|${POSTGRES_CLUSTER_NAME}|${POSTGRES_DATA_DIR}"

  log "Changing PostgreSQL port from ${current_port} to ${desired_port}..."
  if grep -Eq '^[[:space:]]*#?[[:space:]]*port[[:space:]]*=' "${POSTGRES_CONFIG_FILE}"; then
    sed -Ei "s|^[[:space:]]*#?[[:space:]]*port[[:space:]]*=.*$|port = ${desired_port}|" "${POSTGRES_CONFIG_FILE}"
  else
    printf '\nport = %s\n' "${desired_port}" >> "${POSTGRES_CONFIG_FILE}"
  fi

  service_restart_candidates "${POSTGRES_SERVICE_UNIT}" >/dev/null || die "Failed to restart PostgreSQL after changing the port."
  resolve_local_postgres_target

  if [[ "${POSTGRES_MAJOR_VERSION}|${POSTGRES_CLUSTER_NAME}|${POSTGRES_DATA_DIR}" != "${current_target_key}" ]]; then
    die "PostgreSQL restart selected a different local target after the port change."
  fi
  if [[ "${POSTGRES_PORT}" != "${desired_port}" ]]; then
    die "PostgreSQL port change did not take effect; expected ${desired_port}, got ${POSTGRES_PORT}."
  fi
}

load_existing_env() {
  if [[ -f "${ENV_FILE}" ]]; then
    ENV_FILE_ALREADY_PRESENT="true"
    local line=""
    local key=""
    local value=""

    while IFS= read -r line || [[ -n "${line}" ]]; do
      line="${line%$'\r'}"
      [[ -n "${line}" ]] || continue
      [[ "${line}" == \#* ]] && continue
      [[ "${line}" == *=* ]] || continue

      key="${line%%=*}"
      value="${line#*=}"
      value="$(normalize_env_file_value "${value}")"

      case "${key}" in
        MAXOGRAM_TG_BOT_TOKEN)
          TG_BOT_TOKEN="${value}"
          ;;
        MAXOGRAM_MAX_BOT_TOKEN)
          MAX_BOT_TOKEN="${value}"
          ;;
        MAXOGRAM_DB_HOST)
          DB_HOST="${value}"
          ;;
        MAXOGRAM_DB_PORT)
          DB_PORT="${value}"
          ;;
        MAXOGRAM_DB_DATABASE)
          DB_NAME="${value}"
          ;;
        MAXOGRAM_DB_USER)
          DB_USER="${value}"
          ;;
        MAXOGRAM_DB_PASSWORD)
          DB_PASSWORD="${value}"
          ;;
        MAXOGRAM_DB_SCHEMA)
          DB_SCHEMA="${value}"
          ;;
      esac
    done < "${ENV_FILE}"
  fi
}

prompt_value() {
  local __var_name="$1"
  local prompt_text="$2"
  local default_value="${3:-}"
  local current_value
  current_value="${!__var_name:-}"
  local prompt_suffix=""

  if [[ -n "${current_value}" ]]; then
    default_value="${current_value}"
    prompt_suffix=" [press Enter to keep current value]"
  fi

  local input=""
  if [[ -n "${default_value}" ]]; then
    read -r -p "${prompt_text}${prompt_suffix} [${default_value}]: " input
    input="${input:-${default_value}}"
  else
    read -r -p "${prompt_text}: " input
  fi

  printf -v "${__var_name}" '%s' "${input}"
}

prompt_secret() {
  local __var_name="$1"
  local prompt_text="$2"
  local current_value
  current_value="${!__var_name:-}"

  local prompt_suffix=""
  if [[ -n "${current_value}" ]]; then
    prompt_suffix=" [press Enter to keep current value]"
  fi

  local input=""
  read -r -s -p "${prompt_text}${prompt_suffix}: " input
  printf '\n'
  if [[ -z "${input}" && -n "${current_value}" ]]; then
    input="${current_value}"
  fi
  printf -v "${__var_name}" '%s' "${input}"
}

prompt_secret_or_generate() {
  local __var_name="$1"
  local prompt_text="$2"
  local generated_log_message="$3"
  local current_value
  current_value="${!__var_name:-}"

  local prompt_suffix=""
  if [[ -n "${current_value}" ]]; then
    prompt_suffix=" [press Enter to keep current value]"
  else
    prompt_suffix=" [press Enter to auto-generate]"
  fi

  local input=""
  read -r -s -p "${prompt_text}${prompt_suffix}: " input
  printf '\n'
  if [[ -z "${input}" ]]; then
    if [[ -n "${current_value}" ]]; then
      input="${current_value}"
    else
      input="$(generate_password)"
      if [[ -n "${generated_log_message}" ]]; then
        log "${generated_log_message}"
      fi
    fi
  fi
  printf -v "${__var_name}" '%s' "${input}"
}

generate_password() {
  if have_cmd openssl; then
    openssl rand -base64 24 | tr -dc 'A-Za-z0-9._!-' | head -c 32
  else
    date +%s | sha256sum | cut -c1-32
  fi
}

require_non_empty_runtime_values() {
  [[ -n "${TG_BOT_TOKEN}" ]] || die "Telegram bot token is required."
  [[ -n "${MAX_BOT_TOKEN}" ]] || die "MAX bot token is required."
  [[ -n "${DB_HOST}" ]] || die "Database host is required."
  [[ -n "${DB_PORT}" ]] || die "Database port is required."
  [[ -n "${DB_NAME}" ]] || die "Database name is required."
  [[ -n "${DB_USER}" ]] || die "Database user is required."
  [[ -n "${DB_PASSWORD}" ]] || die "Database password is required."
  [[ -n "${DB_SCHEMA}" ]] || die "Database schema is required."
  [[ "${DB_PORT}" =~ ^[0-9]+$ ]] || die "Database port must be an integer."
}

local_postgres_role_exists() {
  local role_name="$1"
  [[ -n "${PSQL_BIN}" ]] || return 1
  [[ "$(selected_postgres_query_scalar "SELECT 1 FROM pg_roles WHERE rolname='$(escape_sql_literal "${role_name}")';")" == "1" ]]
}

prepare_local_postgres_context() {
  LOCAL_DB_TARGET="true"
  ensure_postgres_installed
  resolve_local_postgres_target

  local detected_port
  detected_port="$(detect_postgres_port || true)"
  if [[ -n "${detected_port}" ]]; then
    DB_PORT="${detected_port}"
  fi
}

collect_auto_inputs() {
  local existing_local_role="false"

  if is_local_db_host "${DB_HOST}"; then
    prepare_local_postgres_context
    if [[ "${ENV_FILE_ALREADY_PRESENT}" != "true" ]] && local_postgres_role_exists "${DB_USER}"; then
      existing_local_role="true"
    fi
  fi

  prompt_secret TG_BOT_TOKEN "Telegram bot token"
  [[ -n "${TG_BOT_TOKEN}" ]] || die "Telegram bot token is required."

  prompt_secret MAX_BOT_TOKEN "MAX bot token"
  [[ -n "${MAX_BOT_TOKEN}" ]] || die "MAX bot token is required."

  if [[ "${existing_local_role}" == "true" ]]; then
    prompt_secret_or_generate DB_PASSWORD "Database password for role ${DB_USER}" "Generated database password for ${DB_USER}."
    UPDATE_ROLE_PASSWORD="true"
  else
    prompt_secret_or_generate DB_PASSWORD "Database password" "Generated database password for ${DB_USER}."
    if [[ "${LOCAL_DB_TARGET}" == "true" ]]; then
      UPDATE_ROLE_PASSWORD="true"
    else
      UPDATE_ROLE_PASSWORD="false"
    fi
  fi
}

collect_manual_inputs() {
  prompt_secret TG_BOT_TOKEN "Telegram bot token"
  [[ -n "${TG_BOT_TOKEN}" ]] || die "Telegram bot token is required."

  prompt_secret MAX_BOT_TOKEN "MAX bot token"
  [[ -n "${MAX_BOT_TOKEN}" ]] || die "MAX bot token is required."

  prompt_value DB_HOST "Database host" "${DB_HOST}"
  [[ -n "${DB_HOST}" ]] || die "Database host is required."

  if is_local_db_host "${DB_HOST}"; then
    prepare_local_postgres_context
  else
    LOCAL_DB_TARGET="false"
  fi

  prompt_value DB_PORT "PostgreSQL port" "${DB_PORT}"
  prompt_value DB_NAME "Database name" "${DB_NAME}"
  prompt_value DB_USER "Database user" "${DB_USER}"
  prompt_value DB_SCHEMA "Schema name" "${DB_SCHEMA}"
  prompt_secret DB_PASSWORD "Database password"
  [[ -n "${DB_PASSWORD}" ]] || die "Database password is required in manual mode."

  if [[ "${LOCAL_DB_TARGET}" == "true" ]]; then
    UPDATE_ROLE_PASSWORD="true"
  else
    log "Using remote PostgreSQL at ${DB_HOST}:${DB_PORT}; local PostgreSQL installation and provisioning will be skipped."
    UPDATE_ROLE_PASSWORD="false"
  fi
}

collect_update_inputs() {
  if [[ "${ENV_FILE_ALREADY_PRESENT}" != "true" ]]; then
    die "Update mode requires an existing ${ENV_FILE}."
  fi

  require_non_empty_runtime_values

  if is_local_db_host "${DB_HOST}"; then
    prepare_local_postgres_context
  else
    LOCAL_DB_TARGET="false"
    log "Update mode will reuse remote PostgreSQL at ${DB_HOST}:${DB_PORT}."
  fi

  UPDATE_ROLE_PASSWORD="false"
}

validate_app_dir() {
  [[ "${APP_DIR}" == /* ]] || die "APP_DIR must be an absolute path."
  [[ "${APP_DIR}" != "/" ]] || die "APP_DIR must not be root."
}

docker_compose_available() {
  have_cmd docker && docker compose version >/dev/null 2>&1
}

docker_daemon_available() {
  have_cmd docker && docker info >/dev/null 2>&1
}

ensure_docker_installed() {
  log "Ensuring Docker and Docker Compose are installed..."

  if ! docker_compose_available; then
    if [[ "${#DOCKER_PACKAGE_GROUPS[@]}" -gt 0 ]]; then
      install_from_candidate_groups "Docker runtime" "${DOCKER_PACKAGE_GROUPS[@]}" || true
    else
      warn "No supported package-manager recipe for Docker on this distro. Expecting Docker to already be installed."
    fi
  fi

  have_cmd docker || die "Docker CLI is missing. Install Docker manually and rerun the installer."
  docker compose version >/dev/null 2>&1 || die "Docker Compose is unavailable. Install the Docker Compose plugin and rerun the installer."

  if ! docker_daemon_available; then
    if DOCKER_SERVICE_UNIT="$(service_enable_now_candidates "${DOCKER_SERVICE_CANDIDATES[@]}")"; then
      log "Using Docker service ${DOCKER_SERVICE_UNIT}."
    else
      warn "Docker service name could not be detected automatically; continuing with direct daemon checks."
    fi
  fi

  docker_daemon_available || die "Docker daemon is unavailable. Start Docker and rerun the installer."
}

ensure_deploy_dir() {
  validate_app_dir
  install -d -m 755 "${APP_DIR}"
}

write_compose_file() {
  ensure_deploy_dir

  local runtime_db_host=""
  runtime_db_host="$(docker_runtime_db_host)"

  cat > "${COMPOSE_FILE}" <<EOF
services:
  ${APP_NAME}:
    image: ${DOCKER_IMAGE}
    container_name: ${APP_NAME}
    restart: unless-stopped
    env_file:
      - ${ENV_FILE}
    environment:
      MAXOGRAM_DB_HOST: ${runtime_db_host}
EOF

  if docker_needs_host_gateway_mapping; then
    cat >> "${COMPOSE_FILE}" <<'EOF'
    extra_hosts:
      - "host.docker.internal:host-gateway"
EOF
  fi

  chmod 644 "${COMPOSE_FILE}"
}

docker_compose_cmd() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

docker_run_maxogram_cli() {
  local runtime_db_host=""
  local add_host_flag=()

  runtime_db_host="$(docker_runtime_db_host)"
  if docker_needs_host_gateway_mapping; then
    add_host_flag=(--add-host "host.docker.internal:host-gateway")
  fi

  docker run --rm \
    "${add_host_flag[@]}" \
    --env-file "${ENV_FILE}" \
    -e "MAXOGRAM_DB_HOST=${runtime_db_host}" \
    --entrypoint python \
    "${DOCKER_IMAGE}" \
    -m maxogram --root /app "$@"
}

ensure_database_role_and_db() {
  validate_pg_identifier "${DB_NAME}" || die "Invalid database name: ${DB_NAME}"
  validate_pg_identifier "${DB_USER}" || die "Invalid database user: ${DB_USER}"
  validate_pg_identifier "${DB_SCHEMA}" || die "Invalid schema name: ${DB_SCHEMA}"
  [[ "${DB_PORT}" =~ ^[0-9]+$ ]] || die "PostgreSQL port must be an integer."

  local escaped_password
  escaped_password="$(escape_sql_literal "${DB_PASSWORD}")"

  local role_exists
  role_exists="$(selected_postgres_query_scalar "SELECT 1 FROM pg_roles WHERE rolname='$(escape_sql_literal "${DB_USER}")';" || true)"
  if [[ "${role_exists}" == "1" ]]; then
    if [[ "${UPDATE_ROLE_PASSWORD}" == "true" ]]; then
      log "Updating password for existing PostgreSQL role ${DB_USER}..."
      run_as_postgres env PGHOST="${POSTGRES_SOCKET_DIR}" PGPORT="${POSTGRES_PORT}" \
        "${PSQL_BIN}" -X -v ON_ERROR_STOP=1 -h "${POSTGRES_SOCKET_DIR}" -p "${POSTGRES_PORT}" -d postgres -c \
        "ALTER ROLE \"${DB_USER}\" LOGIN PASSWORD '${escaped_password}';"
    else
      log "Reusing existing PostgreSQL role ${DB_USER}."
    fi
  else
    log "Creating PostgreSQL role ${DB_USER}..."
    run_as_postgres env PGHOST="${POSTGRES_SOCKET_DIR}" PGPORT="${POSTGRES_PORT}" \
      "${PSQL_BIN}" -X -v ON_ERROR_STOP=1 -h "${POSTGRES_SOCKET_DIR}" -p "${POSTGRES_PORT}" -d postgres -c \
      "CREATE ROLE \"${DB_USER}\" LOGIN PASSWORD '${escaped_password}';"
  fi

  local db_exists
  db_exists="$(selected_postgres_query_scalar "SELECT 1 FROM pg_database WHERE datname='$(escape_sql_literal "${DB_NAME}")';" || true)"
  if [[ "${db_exists}" != "1" ]]; then
    log "Creating PostgreSQL database ${DB_NAME}..."
    run_as_postgres env PGHOST="${POSTGRES_SOCKET_DIR}" PGPORT="${POSTGRES_PORT}" \
      "${CREATEDB_BIN}" -h "${POSTGRES_SOCKET_DIR}" -p "${POSTGRES_PORT}" -O "${DB_USER}" "${DB_NAME}"
  else
    log "Reusing existing PostgreSQL database ${DB_NAME}."
  fi
}

create_schema_and_search_path() {
  PGPASSWORD="${DB_PASSWORD}" "${PSQL_BIN}" \
    -v ON_ERROR_STOP=1 \
    -h "${DB_HOST}" \
    -p "${DB_PORT}" \
    -U "${DB_USER}" \
    -d "${DB_NAME}" \
    -c "CREATE SCHEMA IF NOT EXISTS \"${DB_SCHEMA}\" AUTHORIZATION \"${DB_USER}\";"

  run_as_postgres env PGHOST="${POSTGRES_SOCKET_DIR}" PGPORT="${POSTGRES_PORT}" \
    "${PSQL_BIN}" -X -v ON_ERROR_STOP=1 -h "${POSTGRES_SOCKET_DIR}" -p "${POSTGRES_PORT}" -d postgres -c \
    "ALTER ROLE \"${DB_USER}\" IN DATABASE \"${DB_NAME}\" SET search_path TO \"${DB_SCHEMA}\", public;"
}

write_env_file() {
  validate_env_value "${TG_BOT_TOKEN}" || die "Telegram token contains unsupported newlines."
  validate_env_value "${MAX_BOT_TOKEN}" || die "MAX token contains unsupported newlines."
  validate_env_value "${DB_PASSWORD}" || die "Database password contains unsupported newlines."
  validate_env_value "${DB_HOST}" || die "Database host contains unsupported newlines."
  validate_env_value "${DB_NAME}" || die "Database name contains unsupported newlines."
  validate_env_value "${DB_USER}" || die "Database user contains unsupported newlines."
  validate_env_value "${DB_SCHEMA}" || die "Database schema contains unsupported newlines."

  install -d -m 700 "${ENV_DIR}"
  umask 077
  cat > "${ENV_FILE}" <<EOF
MAXOGRAM_TG_BOT_TOKEN=${TG_BOT_TOKEN}
MAXOGRAM_MAX_BOT_TOKEN=${MAX_BOT_TOKEN}
MAXOGRAM_DB_DATABASE=${DB_NAME}
MAXOGRAM_DB_USER=${DB_USER}
MAXOGRAM_DB_PASSWORD=${DB_PASSWORD}
MAXOGRAM_DB_HOST=${DB_HOST}
MAXOGRAM_DB_PORT=${DB_PORT}
MAXOGRAM_DB_SCHEMA=${DB_SCHEMA}
EOF
  chmod 600 "${ENV_FILE}"
}

pull_docker_image() {
  log "Pulling Docker image ${DOCKER_IMAGE}..."
  docker_compose_cmd pull "${APP_NAME}"
}

validate_container_config() {
  log "Validating runtime configuration inside the container..."
  docker_run_maxogram_cli check-config >/dev/null
}

apply_container_migrations() {
  log "Applying database migrations inside the container..."
  docker_run_maxogram_cli db-upgrade
}

deploy_container() {
  log "Recreating the Docker service..."
  docker_compose_cmd up -d
}

remove_legacy_host_artifacts() {
  local daemon_reload_needed="false"

  if [[ -f "${LEGACY_SYSTEMD_UNIT}" ]]; then
    "${SYSTEMCTL_BIN}" disable --now "${APP_NAME}.service" >/dev/null 2>&1 || true
    rm -f "${LEGACY_SYSTEMD_UNIT}"
    daemon_reload_needed="true"
  fi

  if [[ -f "${LEGACY_CRON_FILE}" ]]; then
    rm -f "${LEGACY_CRON_FILE}"
  fi

  if [[ -f "${LEGACY_RESTART_TIMER_SERVICE}" || -f "${LEGACY_RESTART_TIMER_UNIT}" ]]; then
    "${SYSTEMCTL_BIN}" disable --now "${APP_NAME}-restart.timer" >/dev/null 2>&1 || true
    rm -f "${LEGACY_RESTART_TIMER_SERVICE}" "${LEGACY_RESTART_TIMER_UNIT}"
    daemon_reload_needed="true"
  fi

  if [[ "${daemon_reload_needed}" == "true" ]]; then
    "${SYSTEMCTL_BIN}" daemon-reload
    log "Removed legacy host-level Maxogram systemd artifacts."
  fi
}

main() {
  require_root
  ensure_systemd_available

  if [[ "${MODE}" != "auto" && "${MODE}" != "manual" && "${MODE}" != "update" ]]; then
    usage
    exit 1
  fi

  detect_platform
  load_existing_env
  ensure_base_packages
  ensure_required_cmd sed "sed is required by the installer."
  ensure_required_cmd grep "grep is required by the installer."

  case "${MODE}" in
    auto)
      collect_auto_inputs
      ;;
    manual)
      collect_manual_inputs
      ;;
    update)
      collect_update_inputs
      ;;
  esac

  require_non_empty_runtime_values

  local detected_port=""
  if [[ "${LOCAL_DB_TARGET}" == "true" ]]; then
    detected_port="$(detect_postgres_port || true)"
    if [[ "${MODE}" == "manual" && -n "${detected_port}" ]]; then
      set_postgres_port "${DB_PORT}" "${detected_port}"
      DB_PORT="$(detect_postgres_port || true)"
      [[ -n "${DB_PORT}" ]] || die "Could not detect the PostgreSQL port after reconfiguration."
    elif [[ -n "${detected_port}" ]]; then
      DB_PORT="${detected_port}"
    fi
  fi

  if [[ "${LOCAL_DB_TARGET}" == "true" && "${MODE}" != "update" ]]; then
    ensure_database_role_and_db
    create_schema_and_search_path
  fi

  write_env_file
  write_compose_file
  ensure_docker_installed
  pull_docker_image
  validate_container_config
  apply_container_migrations
  remove_legacy_host_artifacts
  deploy_container

  local runtime_db_host=""
  runtime_db_host="$(docker_runtime_db_host)"

  cat <<EOF

Installation complete.

Mode:             ${MODE}
Image:            ${DOCKER_IMAGE}
Deploy directory: ${APP_DIR}
Compose file:     ${COMPOSE_FILE}
Config file:      ${ENV_FILE}
Database:         ${DB_NAME} on ${DB_HOST}:${DB_PORT}
Runtime DB host:  ${runtime_db_host}
Schema:           ${DB_SCHEMA}
Restart policy:   unless-stopped

Useful commands:
  docker compose -f ${COMPOSE_FILE} ps
  docker compose -f ${COMPOSE_FILE} logs -f
  docker compose -f ${COMPOSE_FILE} pull
  docker compose -f ${COMPOSE_FILE} up -d
EOF
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
