#!/usr/bin/env bash
set -euo pipefail

APP_NAME="maxogram"
APP_USER="maxogram"
APP_GROUP="maxogram"
APP_HOME="/var/lib/maxogram"
APP_DIR="/opt/maxogram"
ENV_DIR="/etc/maxogram"
ENV_FILE="${ENV_DIR}/maxogram.env"
SYSTEMD_UNIT="/etc/systemd/system/maxogram.service"
CRON_FILE="/etc/cron.d/maxogram-restart"
RESTART_TIMER_SERVICE="/etc/systemd/system/maxogram-restart.service"
RESTART_TIMER_UNIT="/etc/systemd/system/maxogram-restart.timer"

APP_SOURCE_URL="https://codeload.github.com/d0ke/maxogram/tar.gz/refs/heads/main"
APP_SOURCE_LABEL="GitHub tarball (main)"

PYTHON_FALLBACK_VERSION="3.13.0"
PYTHON_EXEC=""
PYTHON_MIN_MAJOR="3"
PYTHON_MIN_MINOR="13"

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
CRON_PACKAGE_GROUPS=()
PYTHON_PACKAGE_GROUPS=()
BUILD_DEP_PACKAGE_GROUPS=()
POSTGRES_SERVICE_CANDIDATES=()
CRON_SERVICE_CANDIDATES=()
PYTHON_COMMAND_CANDIDATES=()

POSTGRES_SERVICE_UNIT=""
CRON_SERVICE_UNIT=""
WATCHDOG_MODE="none"
POSTGRES_DATA_DIR=""
SYSTEMCTL_BIN=""
NOLOGIN_SHELL=""
PSQL_BIN=""
CREATEDB_BIN=""

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
  manual Ask tokens plus database host, port, name, user, password, and schema.
  update Reuse /etc/maxogram/maxogram.env, refresh code and dependencies, run
         check-config + db-upgrade, then restart the service.

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
    die "systemctl is required because this installer manages systemd services and timers."
  fi
  SYSTEMCTL_BIN="$(command -v systemctl)"
}

detect_nologin_shell() {
  if have_cmd nologin; then
    NOLOGIN_SHELL="$(command -v nologin)"
  elif [[ -x /usr/sbin/nologin ]]; then
    NOLOGIN_SHELL="/usr/sbin/nologin"
  elif [[ -x /sbin/nologin ]]; then
    NOLOGIN_SHELL="/sbin/nologin"
  else
    NOLOGIN_SHELL="/bin/false"
  fi
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
  CRON_PACKAGE_GROUPS=()
  PYTHON_PACKAGE_GROUPS=()
  BUILD_DEP_PACKAGE_GROUPS=()
  POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
  CRON_SERVICE_CANDIDATES=("cron" "cron.service" "crond" "crond.service" "cronie" "cronie.service")
  PYTHON_COMMAND_CANDIDATES=("python3.13" "python3" "python")
  POSTGRES_DATA_DIR=""

  case "${PKG_MANAGER}" in
    apt)
      BASE_PACKAGE_GROUPS=("ca-certificates coreutils curl grep sed gawk procps tar")
      POSTGRES_PACKAGE_GROUPS=("postgresql postgresql-client postgresql-contrib")
      CRON_PACKAGE_GROUPS=("cron")
      PYTHON_PACKAGE_GROUPS=("python3.13 python3.13-venv python3.13-dev")
      BUILD_DEP_PACKAGE_GROUPS=("build-essential libbz2-dev libffi-dev libgdbm-dev liblzma-dev libncursesw5-dev libreadline-dev libsqlite3-dev libssl-dev tk-dev uuid-dev xz-utils zlib1g-dev")
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
      CRON_SERVICE_CANDIDATES=("cron" "cron.service")
      PYTHON_COMMAND_CANDIDATES=("python3.13" "python3")
      ;;
    dnf)
      BASE_PACKAGE_GROUPS=("ca-certificates coreutils curl grep sed gawk procps-ng tar")
      POSTGRES_PACKAGE_GROUPS=("postgresql-server postgresql postgresql-contrib")
      CRON_PACKAGE_GROUPS=("cronie")
      PYTHON_PACKAGE_GROUPS=("python3.13 python3.13-devel" "python3 python3-devel")
      BUILD_DEP_PACKAGE_GROUPS=("bzip2-devel gcc gdbm-devel libffi-devel make ncurses-devel openssl-devel readline-devel sqlite-devel tk-devel xz-devel zlib-devel")
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service" "postgresql-16" "postgresql-15")
      CRON_SERVICE_CANDIDATES=("crond" "crond.service" "cronie" "cronie.service")
      PYTHON_COMMAND_CANDIDATES=("python3.13" "python3")
      POSTGRES_DATA_DIR="/var/lib/pgsql/data"
      ;;
    zypper)
      BASE_PACKAGE_GROUPS=("ca-certificates coreutils curl grep sed gawk procps tar")
      POSTGRES_PACKAGE_GROUPS=("postgresql-server postgresql postgresql-contrib" "postgresql16-server postgresql16 postgresql16-contrib" "postgresql15-server postgresql15 postgresql15-contrib" "postgresql14-server postgresql14 postgresql14-contrib")
      CRON_PACKAGE_GROUPS=("cron")
      PYTHON_PACKAGE_GROUPS=("python313 python313-devel" "python3.13 python3.13-devel" "python3 python3-devel")
      BUILD_DEP_PACKAGE_GROUPS=("gcc make libbz2-devel libffi-devel gdbm-devel liblzma-devel ncurses-devel readline-devel sqlite3-devel libopenssl-devel tk-devel xz zlib-devel")
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
      CRON_SERVICE_CANDIDATES=("cron" "cron.service")
      PYTHON_COMMAND_CANDIDATES=("python3.13" "python3" "python")
      POSTGRES_DATA_DIR="/var/lib/pgsql/data"
      ;;
    pacman)
      BASE_PACKAGE_GROUPS=("ca-certificates coreutils curl grep sed gawk procps-ng tar")
      POSTGRES_PACKAGE_GROUPS=("postgresql")
      CRON_PACKAGE_GROUPS=("cronie")
      PYTHON_PACKAGE_GROUPS=("python")
      BUILD_DEP_PACKAGE_GROUPS=("base-devel bzip2 gdbm libffi ncurses openssl readline sqlite tk xz zlib")
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
      CRON_SERVICE_CANDIDATES=("cronie" "cronie.service" "crond" "crond.service")
      PYTHON_COMMAND_CANDIDATES=("python" "python3")
      POSTGRES_DATA_DIR="/var/lib/postgres/data"
      ;;
    *)
      BASE_PACKAGE_GROUPS=()
      POSTGRES_PACKAGE_GROUPS=()
      CRON_PACKAGE_GROUPS=()
      PYTHON_PACKAGE_GROUPS=()
      BUILD_DEP_PACKAGE_GROUPS=()
      POSTGRES_SERVICE_CANDIDATES=("postgresql" "postgresql.service")
      CRON_SERVICE_CANDIDATES=("cron" "cron.service" "crond" "crond.service" "cronie" "cronie.service")
      PYTHON_COMMAND_CANDIDATES=("python3.13" "python3" "python")
      POSTGRES_DATA_DIR=""
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
  local candidate
  local resolved

  if have_cmd "${name}"; then
    command -v "${name}"
    return 0
  fi

  for candidate in "/usr/bin/${name}" "/usr/lib/postgresql*/bin/${name}" "/usr/pgsql-*/bin/${name}"; do
    for resolved in ${candidate}; do
      if [[ -x "${resolved}" ]]; then
        printf '%s\n' "${resolved}"
        return 0
      fi
    done
  done

  return 1
}

refresh_postgres_binaries() {
  PSQL_BIN="$(find_postgres_binary psql || true)"
  CREATEDB_BIN="$(find_postgres_binary createdb || true)"
}

run_as_postgres() {
  runuser -u postgres -- "$@"
}

run_as_app() {
  runuser -u "${APP_USER}" -- "$@"
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

quote_env_value() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//\$/\\$}"
  value="${value//\`/\\\`}"
  printf '"%s"' "${value}"
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

ensure_base_packages() {
  if [[ "${#BASE_PACKAGE_GROUPS[@]}" -eq 0 ]]; then
    warn "No known base-package set for this distro. Continuing with whatever tools are already installed."
    return 0
  fi

  install_from_candidate_groups "base packages" "${BASE_PACKAGE_GROUPS[@]}" || true
}

resolve_postgres_data_dir() {
  if [[ -n "${POSTGRES_DATA_DIR}" ]]; then
    printf '%s\n' "${POSTGRES_DATA_DIR}"
    return 0
  fi

  if [[ -f /var/lib/postgres/data/PG_VERSION ]] || [[ -d /var/lib/postgres ]]; then
    printf '%s\n' "/var/lib/postgres/data"
  else
    printf '%s\n' "/var/lib/pgsql/data"
  fi
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
  elif ! run_as_postgres "${PSQL_BIN}" -d postgres -tAc "SELECT 1" >/dev/null 2>&1; then
    die "Could not start PostgreSQL automatically. Start it manually and rerun the installer."
  else
    warn "PostgreSQL service name could not be detected, but the server is already responding."
  fi
}

detect_postgres_port() {
  run_as_postgres "${PSQL_BIN}" -d postgres -tAc "SHOW port;" | tr -d '[:space:]'
}

detect_postgres_file() {
  local setting="$1"
  run_as_postgres "${PSQL_BIN}" -d postgres -tAc "SHOW ${setting};" | tr -d '[:space:]'
}

set_postgres_port() {
  local desired_port="$1"
  local current_port="$2"

  if [[ -z "${desired_port}" || "${desired_port}" == "${current_port}" ]]; then
    return 0
  fi

  [[ "${desired_port}" =~ ^[0-9]+$ ]] || die "PostgreSQL port must be an integer."

  local config_file
  config_file="$(detect_postgres_file "config_file" || true)"
  [[ -n "${config_file}" && -f "${config_file}" ]] || die "Cannot locate postgresql.conf."

  log "Changing PostgreSQL port from ${current_port} to ${desired_port}..."
  if grep -Eq '^[[:space:]]*#?[[:space:]]*port[[:space:]]*=' "${config_file}"; then
    sed -Ei "s|^[[:space:]]*#?[[:space:]]*port[[:space:]]*=.*$|port = ${desired_port}|" "${config_file}"
  else
    printf '\nport = %s\n' "${desired_port}" >> "${config_file}"
  fi

  if [[ -n "${POSTGRES_SERVICE_UNIT}" ]]; then
    service_restart_candidates "${POSTGRES_SERVICE_UNIT}" >/dev/null || die "Failed to restart PostgreSQL after changing the port."
  fi
}

load_existing_env() {
  if [[ -f "${ENV_FILE}" ]]; then
    ENV_FILE_ALREADY_PRESENT="true"
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    TG_BOT_TOKEN="${MAXOGRAM_TG_BOT_TOKEN:-${TG_BOT_TOKEN}}"
    MAX_BOT_TOKEN="${MAXOGRAM_MAX_BOT_TOKEN:-${MAX_BOT_TOKEN}}"
    DB_HOST="${MAXOGRAM_DB_HOST:-${DB_HOST}}"
    DB_PORT="${MAXOGRAM_DB_PORT:-${DB_PORT}}"
    DB_NAME="${MAXOGRAM_DB_DATABASE:-${DB_NAME}}"
    DB_USER="${MAXOGRAM_DB_USER:-${DB_USER}}"
    DB_PASSWORD="${MAXOGRAM_DB_PASSWORD:-${DB_PASSWORD}}"
    DB_SCHEMA="${MAXOGRAM_DB_SCHEMA:-${DB_SCHEMA}}"
  fi
}

prompt_value() {
  local __var_name="$1"
  local prompt_text="$2"
  local default_value="${3:-}"
  local current_value
  current_value="${!__var_name:-}"

  if [[ -n "${current_value}" ]]; then
    default_value="${current_value}"
  fi

  local input=""
  if [[ -n "${default_value}" ]]; then
    read -r -p "${prompt_text} [${default_value}]: " input
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

generate_password() {
  if have_cmd openssl; then
    openssl rand -base64 24 | tr -dc 'A-Za-z0-9._-!' | head -c 32
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
  [[ "$(run_as_postgres "${PSQL_BIN}" -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='$(escape_sql_literal "${role_name}")';" | tr -d '[:space:]')" == "1" ]]
}

prepare_local_postgres_context() {
  LOCAL_DB_TARGET="true"
  ensure_postgres_installed

  local detected_port
  detected_port="$(detect_postgres_port || true)"
  if [[ -n "${detected_port}" ]]; then
    DB_PORT="${detected_port}"
  fi
}

collect_auto_inputs() {
  if is_local_db_host "${DB_HOST}"; then
    prepare_local_postgres_context
  fi

  prompt_secret TG_BOT_TOKEN "Telegram bot token"
  [[ -n "${TG_BOT_TOKEN}" ]] || die "Telegram bot token is required."

  prompt_secret MAX_BOT_TOKEN "MAX bot token"
  [[ -n "${MAX_BOT_TOKEN}" ]] || die "MAX bot token is required."

  if [[ "${LOCAL_DB_TARGET}" == "true" && "${ENV_FILE_ALREADY_PRESENT}" != "true" ]] && local_postgres_role_exists "${DB_USER}"; then
    prompt_secret DB_PASSWORD "Database password for existing role ${DB_USER}"
    [[ -n "${DB_PASSWORD}" ]] || die "Database password is required to reuse existing role ${DB_USER}."
    UPDATE_ROLE_PASSWORD="false"
  elif [[ -z "${DB_PASSWORD}" ]]; then
    DB_PASSWORD="$(generate_password)"
    log "Generated database password for ${DB_USER}."
    UPDATE_ROLE_PASSWORD="false"
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

ensure_app_user() {
  if id "${APP_USER}" >/dev/null 2>&1; then
    log "OS user ${APP_USER} already exists."
    return 0
  fi

  log "Creating OS user ${APP_USER}..."
  useradd \
    --system \
    --user-group \
    --home-dir "${APP_HOME}" \
    --create-home \
    --shell "${NOLOGIN_SHELL}" \
    "${APP_USER}"
}

validate_app_dir() {
  [[ "${APP_DIR}" == /* ]] || die "APP_DIR must be an absolute path."
  [[ "${APP_DIR}" != "/" ]] || die "APP_DIR must not be root."
}

ensure_repo_checkout() {
  validate_app_dir
  ensure_required_cmd curl "curl is required to download the Maxogram source tarball."
  ensure_required_cmd tar "tar is required to extract the Maxogram source tarball."
  ensure_required_cmd find "find is required to inspect the extracted Maxogram source tree."

  local work_root
  local archive_path
  local extract_root
  local source_root
  local venv_backup=""

  work_root="$(mktemp -d "/tmp/${APP_NAME}-source-XXXXXX")"
  archive_path="${work_root}/source.tar.gz"
  extract_root="${work_root}/extract"

  mkdir -p "${extract_root}"

  log "Downloading ${APP_SOURCE_LABEL}..."
  curl -fsSL "${APP_SOURCE_URL}" -o "${archive_path}"
  tar -C "${extract_root}" -xzf "${archive_path}"

  source_root="$(find "${extract_root}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  [[ -n "${source_root}" && -d "${source_root}" ]] || die "Could not find the extracted Maxogram source tree."
  [[ -f "${source_root}/requirements.txt" ]] || die "The downloaded source tree is missing requirements.txt."

  if [[ -d "${APP_DIR}/.venv" ]]; then
    venv_backup="${work_root}/.venv"
    mv "${APP_DIR}/.venv" "${venv_backup}"
  fi

  rm -rf "${APP_DIR}"
  install -d -m 755 "${APP_DIR}"

  (
    cd "${source_root}"
    tar -cf - .
  ) | (
    cd "${APP_DIR}"
    tar -xf -
  )

  if [[ -n "${venv_backup}" && -d "${venv_backup}" ]]; then
    mv "${venv_backup}" "${APP_DIR}/.venv"
  fi

  chown -R "${APP_USER}:${APP_GROUP}" "${APP_DIR}"
  rm -rf "${work_root}"
}

python_version_ge() {
  local python_exec="$1"
  local major="$2"
  local minor="$3"

  "${python_exec}" - "$major" "$minor" <<'PY'
import sys

required_major = int(sys.argv[1])
required_minor = int(sys.argv[2])
sys.exit(0 if sys.version_info >= (required_major, required_minor) else 1)
PY
}

discover_python_exec() {
  local candidate
  local resolved
  local seen=" "

  for candidate in "${PYTHON_COMMAND_CANDIDATES[@]}" "python3.13" "python3" "python"; do
    if ! have_cmd "${candidate}"; then
      continue
    fi
    resolved="$(command -v "${candidate}")"
    if [[ "${seen}" == *" ${resolved} "* ]]; then
      continue
    fi
    seen="${seen}${resolved} "
    if python_version_ge "${resolved}" "${PYTHON_MIN_MAJOR}" "${PYTHON_MIN_MINOR}"; then
      printf '%s\n' "${resolved}"
      return 0
    fi
  done

  return 1
}

install_python_build_dependencies() {
  if [[ "${#BUILD_DEP_PACKAGE_GROUPS[@]}" -eq 0 ]]; then
    warn "No known build-dependency set for this distro. Python source build will rely on existing build tools."
    return 0
  fi

  install_from_candidate_groups "Python build dependencies" "${BUILD_DEP_PACKAGE_GROUPS[@]}" || true
}

ensure_python() {
  export PATH="/usr/local/bin:${PATH}"

  if PYTHON_EXEC="$(discover_python_exec)"; then
    log "Using system Python at ${PYTHON_EXEC}."
    return 0
  fi

  log "Trying to install a Python interpreter >= 3.13 from OS packages..."
  if [[ "${#PYTHON_PACKAGE_GROUPS[@]}" -gt 0 ]]; then
    install_from_candidate_groups "Python >= 3.13" "${PYTHON_PACKAGE_GROUPS[@]}" || true
    if PYTHON_EXEC="$(discover_python_exec)"; then
      log "Using packaged Python at ${PYTHON_EXEC}."
      return 0
    fi
  else
    warn "No package-manager recipe is available for Python on this distro."
  fi

  warn "A suitable Python >= 3.13 is not available from packages; building Python ${PYTHON_FALLBACK_VERSION} from source."
  install_python_build_dependencies

  local build_root="/tmp/${APP_NAME}-python-build"
  rm -rf "${build_root}"
  mkdir -p "${build_root}"

  ensure_required_cmd curl "curl is required to download the Python source tarball."
  ensure_required_cmd tar "tar is required to extract the Python source tarball."

  curl -fsSL \
    "https://www.python.org/ftp/python/${PYTHON_FALLBACK_VERSION}/Python-${PYTHON_FALLBACK_VERSION}.tgz" \
    -o "${build_root}/Python.tgz"
  tar -C "${build_root}" -xf "${build_root}/Python.tgz"

  local source_dir="${build_root}/Python-${PYTHON_FALLBACK_VERSION}"
  local jobs="2"
  if have_cmd getconf; then
    jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || printf '2')"
  fi

  (
    cd "${source_dir}"
    ./configure --enable-optimizations --with-ensurepip=install
    make -j"${jobs}"
    make altinstall
  )

  PYTHON_EXEC="$(discover_python_exec || true)"
  [[ -n "${PYTHON_EXEC}" ]] || die "Python build finished, but no interpreter >= 3.13 is available."
}

ensure_virtualenv() {
  local venv_python="${APP_DIR}/.venv/bin/python"
  local venv_pip="${APP_DIR}/.venv/bin/pip"

  if [[ -d "${APP_DIR}/.venv" ]]; then
    log "Upgrading existing virtual environment..."
    run_as_app "${PYTHON_EXEC}" -m venv --upgrade "${APP_DIR}/.venv"
  else
    log "Creating virtual environment in ${APP_DIR}/.venv..."
    run_as_app "${PYTHON_EXEC}" -m venv "${APP_DIR}/.venv"
  fi

  run_as_app "${venv_python}" -m ensurepip --upgrade
  run_as_app "${venv_pip}" install --upgrade pip setuptools wheel
  run_as_app "${venv_pip}" install --upgrade -r "${APP_DIR}/requirements.txt"
}

ensure_database_role_and_db() {
  validate_pg_identifier "${DB_NAME}" || die "Invalid database name: ${DB_NAME}"
  validate_pg_identifier "${DB_USER}" || die "Invalid database user: ${DB_USER}"
  validate_pg_identifier "${DB_SCHEMA}" || die "Invalid schema name: ${DB_SCHEMA}"
  [[ "${DB_PORT}" =~ ^[0-9]+$ ]] || die "PostgreSQL port must be an integer."

  local escaped_password
  escaped_password="$(escape_sql_literal "${DB_PASSWORD}")"

  local role_exists
  role_exists="$(run_as_postgres "${PSQL_BIN}" -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='$(escape_sql_literal "${DB_USER}")';" | tr -d '[:space:]')"
  if [[ "${role_exists}" == "1" ]]; then
    if [[ "${UPDATE_ROLE_PASSWORD}" == "true" ]]; then
      log "Updating password for existing PostgreSQL role ${DB_USER}..."
      run_as_postgres "${PSQL_BIN}" -v ON_ERROR_STOP=1 -d postgres -c \
        "ALTER ROLE \"${DB_USER}\" LOGIN PASSWORD '${escaped_password}';"
    else
      log "Reusing existing PostgreSQL role ${DB_USER}."
    fi
  else
    log "Creating PostgreSQL role ${DB_USER}..."
    run_as_postgres "${PSQL_BIN}" -v ON_ERROR_STOP=1 -d postgres -c \
      "CREATE ROLE \"${DB_USER}\" LOGIN PASSWORD '${escaped_password}';"
  fi

  local db_exists
  db_exists="$(run_as_postgres "${PSQL_BIN}" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$(escape_sql_literal "${DB_NAME}")';" | tr -d '[:space:]')"
  if [[ "${db_exists}" != "1" ]]; then
    log "Creating PostgreSQL database ${DB_NAME}..."
    run_as_postgres "${CREATEDB_BIN}" -O "${DB_USER}" "${DB_NAME}"
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

  run_as_postgres "${PSQL_BIN}" -v ON_ERROR_STOP=1 -d postgres -c \
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
MAXOGRAM_TG_BOT_TOKEN=$(quote_env_value "${TG_BOT_TOKEN}")
MAXOGRAM_MAX_BOT_TOKEN=$(quote_env_value "${MAX_BOT_TOKEN}")
MAXOGRAM_DB_DATABASE=$(quote_env_value "${DB_NAME}")
MAXOGRAM_DB_USER=$(quote_env_value "${DB_USER}")
MAXOGRAM_DB_PASSWORD=$(quote_env_value "${DB_PASSWORD}")
MAXOGRAM_DB_HOST=$(quote_env_value "${DB_HOST}")
MAXOGRAM_DB_PORT=$(quote_env_value "${DB_PORT}")
MAXOGRAM_DB_SCHEMA=$(quote_env_value "${DB_SCHEMA}")
EOF
  chmod 600 "${ENV_FILE}"
}

run_maxogram_cli() {
  run_as_app env \
    MAXOGRAM_TG_BOT_TOKEN="${TG_BOT_TOKEN}" \
    MAXOGRAM_MAX_BOT_TOKEN="${MAX_BOT_TOKEN}" \
    MAXOGRAM_DB_DATABASE="${DB_NAME}" \
    MAXOGRAM_DB_USER="${DB_USER}" \
    MAXOGRAM_DB_PASSWORD="${DB_PASSWORD}" \
    MAXOGRAM_DB_HOST="${DB_HOST}" \
    MAXOGRAM_DB_PORT="${DB_PORT}" \
    MAXOGRAM_DB_SCHEMA="${DB_SCHEMA}" \
    "${APP_DIR}/.venv/bin/python" -m maxogram --root "${APP_DIR}" "$@"
}

verify_database_connection() {
  log "Checking database connectivity..."
  run_as_app env \
    MAXOGRAM_DB_DATABASE="${DB_NAME}" \
    MAXOGRAM_DB_USER="${DB_USER}" \
    MAXOGRAM_DB_PASSWORD="${DB_PASSWORD}" \
    MAXOGRAM_DB_HOST="${DB_HOST}" \
    MAXOGRAM_DB_PORT="${DB_PORT}" \
    "${APP_DIR}/.venv/bin/python" - <<'PY'
import asyncio
import os

import asyncpg


async def main() -> None:
    conn = await asyncpg.connect(
        database=os.environ["MAXOGRAM_DB_DATABASE"],
        user=os.environ["MAXOGRAM_DB_USER"],
        password=os.environ["MAXOGRAM_DB_PASSWORD"],
        host=os.environ["MAXOGRAM_DB_HOST"],
        port=int(os.environ["MAXOGRAM_DB_PORT"]),
    )
    await conn.close()


asyncio.run(main())
PY
}

write_systemd_unit() {
  local after_line="After=network-online.target"
  if [[ "${LOCAL_DB_TARGET}" == "true" && -n "${POSTGRES_SERVICE_UNIT}" ]]; then
    after_line="After=network-online.target ${POSTGRES_SERVICE_UNIT}"
  fi

  cat > "${SYSTEMD_UNIT}" <<EOF
[Unit]
Description=Maxogram bridge bot
Wants=network-online.target
${after_line}

[Service]
Type=simple
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${APP_DIR}
EnvironmentFile=${ENV_FILE}
ExecStart=${APP_DIR}/.venv/bin/python -m maxogram --root ${APP_DIR} run
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

  "${SYSTEMCTL_BIN}" daemon-reload
  "${SYSTEMCTL_BIN}" enable --now "${APP_NAME}.service"
}

write_cron_restart() {
  if [[ "${#CRON_PACKAGE_GROUPS[@]}" -gt 0 ]]; then
    install_from_candidate_groups "cron watchdog packages" "${CRON_PACKAGE_GROUPS[@]}" || true
  fi

  if CRON_SERVICE_UNIT="$(service_enable_now_candidates "${CRON_SERVICE_CANDIDATES[@]}")"; then
    "${SYSTEMCTL_BIN}" disable --now "maxogram-restart.timer" >/dev/null 2>&1 || true
    rm -f "${RESTART_TIMER_SERVICE}" "${RESTART_TIMER_UNIT}"
    cat > "${CRON_FILE}" <<EOF
# Managed by ${APP_NAME} install.sh
0 */4 * * * root ${SYSTEMCTL_BIN} restart ${APP_NAME}.service >/dev/null 2>&1
EOF
    chmod 644 "${CRON_FILE}"
    WATCHDOG_MODE="cron"
    log "Using cron watchdog via ${CRON_SERVICE_UNIT}."
    return 0
  fi

  return 1
}

write_restart_timer() {
  rm -f "${CRON_FILE}"

  cat > "${RESTART_TIMER_SERVICE}" <<EOF
[Unit]
Description=Restart Maxogram service

[Service]
Type=oneshot
ExecStart=${SYSTEMCTL_BIN} restart ${APP_NAME}.service
EOF

  cat > "${RESTART_TIMER_UNIT}" <<EOF
[Unit]
Description=Restart Maxogram every 4 hours

[Timer]
OnBootSec=4h
OnUnitActiveSec=4h
Persistent=true
Unit=maxogram-restart.service

[Install]
WantedBy=timers.target
EOF

  "${SYSTEMCTL_BIN}" daemon-reload
  "${SYSTEMCTL_BIN}" enable --now "maxogram-restart.timer"
  WATCHDOG_MODE="timer"
  log "Using systemd timer watchdog."
}

write_restart_watchdog() {
  if write_cron_restart; then
    return 0
  fi

  warn "Cron watchdog is unavailable; falling back to a systemd timer."
  write_restart_timer
}

main() {
  require_root
  ensure_systemd_available
  detect_nologin_shell

  if [[ "${MODE}" != "auto" && "${MODE}" != "manual" && "${MODE}" != "update" ]]; then
    usage
    exit 1
  fi

  detect_platform
  load_existing_env
  ensure_base_packages
  ensure_required_cmd sed "sed is required by the installer."
  ensure_required_cmd grep "grep is required by the installer."
  ensure_required_cmd curl "curl is required by the installer."
  ensure_required_cmd tar "tar is required by the installer."

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

  ensure_app_user
  ensure_repo_checkout
  ensure_python
  ensure_virtualenv

  if [[ "${LOCAL_DB_TARGET}" == "true" && "${MODE}" != "update" ]]; then
    ensure_database_role_and_db
    create_schema_and_search_path
  fi

  write_env_file
  verify_database_connection

  log "Validating runtime configuration..."
  run_maxogram_cli check-config >/dev/null

  log "Applying database migrations..."
  run_maxogram_cli db-upgrade

  write_systemd_unit
  write_restart_watchdog

  "${SYSTEMCTL_BIN}" restart "${APP_NAME}.service"

  cat <<EOF

Installation complete.

Mode:            ${MODE}
Source:          ${APP_SOURCE_LABEL}
App directory:   ${APP_DIR}
Config file:     ${ENV_FILE}
Service:         ${APP_NAME}.service
Database:        ${DB_NAME} on ${DB_HOST}:${DB_PORT}
Schema:          ${DB_SCHEMA}
Watchdog:        ${WATCHDOG_MODE}

Useful commands:
  systemctl status ${APP_NAME}
  journalctl -u ${APP_NAME} -f
  systemctl restart ${APP_NAME}
EOF
}

main "$@"
