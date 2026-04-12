from __future__ import annotations

import os
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def find_bash() -> str | None:
    candidates = [
        shutil.which("bash"),
        r"C:\Program Files\Git\bin\bash.exe",
        r"C:\Program Files\Git\usr\bin\bash.exe",
        "/bin/bash",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return None


def bash_path() -> str:
    resolved = find_bash()
    if not resolved:
        pytest.skip("bash is required for installer behavior tests")
    return resolved


def write_executable(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content), encoding="utf-8")
    path.chmod(0o755)


def write_postmaster_pid(data_dir: Path, port: int, socket_dir: Path, pid: int) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    socket_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "postmaster.pid").write_text(
        "\n".join(
            [
                str(pid),
                data_dir.as_posix(),
                "0",
                str(port),
                socket_dir.as_posix(),
                "*",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def write_fake_createdb(path: Path) -> None:
    write_executable(
        path,
        """#!/usr/bin/env bash
        set -euo pipefail
        exit 0
        """,
    )


def write_fake_psql(path: Path, responses: dict[str, str]) -> None:
    checks = "\n".join(
        f"""if [[ "${{port}}|${{sql}}" == {response_key!r} ]]; then
  printf '%s\\n' {response_value!r}
  exit 0
fi"""
        for response_key, response_value in responses.items()
    )
    write_executable(
        path,
        f"""#!/usr/bin/env bash
        set -euo pipefail

        port="${{PGPORT:-}}"
        sql=""

        while (($#)); do
          case "$1" in
            -p)
              port="$2"
              shift 2
              ;;
            -tAc|-c)
              sql="$2"
              shift 2
              ;;
            *)
              shift
              ;;
          esac
        done

        {checks}
        exit 0
        """,
    )


def run_bash(script: str, *, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [bash_path(), "-lc", script],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
    )


def source_install_and_run(script_body: str) -> subprocess.CompletedProcess[str]:
    install_path = (ROOT / "install.sh").resolve().as_posix()
    script = textwrap.dedent(
        f"""
        set -euo pipefail
        source "{install_path}"
        {script_body}
        """
    )
    return run_bash(script)


def make_versioned_binaries(tmp_path: Path, version: str, psql_responses: dict[str, str]) -> None:
    bin_dir = tmp_path / "lib" / "postgresql" / version / "bin"
    write_fake_psql(bin_dir / "psql", psql_responses)
    write_fake_createdb(bin_dir / "createdb")


def test_debian_resolve_uses_live_cluster_and_versioned_binary(tmp_path: Path) -> None:
    etc_root = tmp_path / "etc" / "postgresql"
    data13 = tmp_path / "var" / "lib" / "postgresql" / "13" / "main"
    data17 = tmp_path / "var" / "lib" / "postgresql" / "17" / "main"
    socket17 = tmp_path / "run" / "postgresql-17"
    config13 = etc_root / "13" / "main" / "postgresql.conf"
    config17 = etc_root / "17" / "main" / "postgresql.conf"

    config13.parent.mkdir(parents=True, exist_ok=True)
    config17.parent.mkdir(parents=True, exist_ok=True)
    config13.write_text("port = 55432\n", encoding="utf-8")
    config17.write_text("port = 55432\n", encoding="utf-8")
    write_postmaster_pid(data17, 55432, socket17, pid=1717)

    make_versioned_binaries(
        tmp_path,
        "13",
        {
            "55432|SHOW config_file;": config13.as_posix(),
            "55432|SHOW data_directory;": data13.as_posix(),
            "55432|SHOW port;": "55432",
        },
    )
    make_versioned_binaries(
        tmp_path,
        "17",
        {
            "55432|SHOW config_file;": config17.as_posix(),
            "55432|SHOW data_directory;": data17.as_posix(),
            "55432|SHOW port;": "55432",
        },
    )

    result = source_install_and_run(
        f"""
        POSTGRES_LIB_ROOT="{(tmp_path / 'lib' / 'postgresql').as_posix()}"
        POSTGRES_ETC_ROOT="{etc_root.as_posix()}"
        DISTRO_FAMILY="debian"
        ENV_FILE_ALREADY_PRESENT="true"
        DB_HOST="127.0.0.1"
        DB_PORT="55432"
        DB_NAME="maxogram"
        DB_USER="maxogram_app"
        run_as_postgres() {{ "$@"; }}
        pg_lsclusters() {{
          cat <<'EOF'
Ver Cluster Port Status Owner Data directory Log file
13 main 55432 down postgres {data13.as_posix()} /tmp/postgresql-13.log
17 main 55432 online postgres {data17.as_posix()} /tmp/postgresql-17.log
EOF
        }}
        resolve_local_postgres_target
        printf 'version=%s\\ncluster=%s\\nport=%s\\npsql=%s\\nsource=%s\\n' \
          "$POSTGRES_MAJOR_VERSION" "$POSTGRES_CLUSTER_NAME" "$POSTGRES_PORT" "$PSQL_BIN" "$POSTGRES_SELECTION_SOURCE"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "version=17" in result.stdout
    assert "cluster=main" in result.stdout
    assert "port=55432" in result.stdout
    assert f"psql={(tmp_path / 'lib' / 'postgresql' / '17' / 'bin' / 'psql').as_posix()}" in result.stdout
    assert "source=existing env port via pg_lsclusters" in result.stdout


def test_debian_resolve_prefers_existing_maxogram_data_over_newer_cluster(tmp_path: Path) -> None:
    etc_root = tmp_path / "etc" / "postgresql"
    data15 = tmp_path / "var" / "lib" / "postgresql" / "15" / "main"
    data17 = tmp_path / "var" / "lib" / "postgresql" / "17" / "main"
    socket15 = tmp_path / "run" / "postgresql-15"
    socket17 = tmp_path / "run" / "postgresql-17"
    config15 = etc_root / "15" / "main" / "postgresql.conf"
    config17 = etc_root / "17" / "main" / "postgresql.conf"

    config15.parent.mkdir(parents=True, exist_ok=True)
    config17.parent.mkdir(parents=True, exist_ok=True)
    config15.write_text("port = 55431\n", encoding="utf-8")
    config17.write_text("port = 55432\n", encoding="utf-8")
    write_postmaster_pid(data15, 55431, socket15, pid=1515)
    write_postmaster_pid(data17, 55432, socket17, pid=1717)

    make_versioned_binaries(
        tmp_path,
        "15",
        {
            "55431|SELECT 1 FROM pg_database WHERE datname='maxogram';": "1",
            "55431|SELECT 1 FROM pg_roles WHERE rolname='maxogram_app';": "1",
            "55431|SHOW config_file;": config15.as_posix(),
            "55431|SHOW data_directory;": data15.as_posix(),
            "55431|SHOW port;": "55431",
        },
    )
    make_versioned_binaries(
        tmp_path,
        "17",
        {
            "55432|SHOW config_file;": config17.as_posix(),
            "55432|SHOW data_directory;": data17.as_posix(),
            "55432|SHOW port;": "55432",
        },
    )

    result = source_install_and_run(
        f"""
        POSTGRES_LIB_ROOT="{(tmp_path / 'lib' / 'postgresql').as_posix()}"
        POSTGRES_ETC_ROOT="{etc_root.as_posix()}"
        DISTRO_FAMILY="debian"
        DB_HOST="127.0.0.1"
        DB_PORT="5432"
        DB_NAME="maxogram"
        DB_USER="maxogram_app"
        run_as_postgres() {{ "$@"; }}
        pg_lsclusters() {{
          cat <<'EOF'
Ver Cluster Port Status Owner Data directory Log file
15 main 55431 online postgres {data15.as_posix()} /tmp/postgresql-15.log
17 main 55432 online postgres {data17.as_posix()} /tmp/postgresql-17.log
EOF
        }}
        resolve_local_postgres_target
        printf 'version=%s\\nport=%s\\nsource=%s\\n' "$POSTGRES_MAJOR_VERSION" "$POSTGRES_PORT" "$POSTGRES_SELECTION_SOURCE"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "version=15" in result.stdout
    assert "port=55431" in result.stdout
    assert "source=existing Maxogram role/database via pg_lsclusters" in result.stdout


def test_debian_resolve_prefers_highest_live_major_when_no_existing_data(tmp_path: Path) -> None:
    etc_root = tmp_path / "etc" / "postgresql"
    data15 = tmp_path / "var" / "lib" / "postgresql" / "15" / "main"
    data17 = tmp_path / "var" / "lib" / "postgresql" / "17" / "main"
    socket15 = tmp_path / "run" / "postgresql-15"
    socket17 = tmp_path / "run" / "postgresql-17"
    config15 = etc_root / "15" / "main" / "postgresql.conf"
    config17 = etc_root / "17" / "main" / "postgresql.conf"

    config15.parent.mkdir(parents=True, exist_ok=True)
    config17.parent.mkdir(parents=True, exist_ok=True)
    config15.write_text("port = 55431\n", encoding="utf-8")
    config17.write_text("port = 55432\n", encoding="utf-8")
    write_postmaster_pid(data15, 55431, socket15, pid=1515)
    write_postmaster_pid(data17, 55432, socket17, pid=1717)

    make_versioned_binaries(
        tmp_path,
        "15",
        {
            "55431|SHOW config_file;": config15.as_posix(),
            "55431|SHOW data_directory;": data15.as_posix(),
            "55431|SHOW port;": "55431",
        },
    )
    make_versioned_binaries(
        tmp_path,
        "17",
        {
            "55432|SHOW config_file;": config17.as_posix(),
            "55432|SHOW data_directory;": data17.as_posix(),
            "55432|SHOW port;": "55432",
        },
    )

    result = source_install_and_run(
        f"""
        POSTGRES_LIB_ROOT="{(tmp_path / 'lib' / 'postgresql').as_posix()}"
        POSTGRES_ETC_ROOT="{etc_root.as_posix()}"
        DISTRO_FAMILY="debian"
        DB_HOST="127.0.0.1"
        DB_PORT="5432"
        DB_NAME="maxogram"
        DB_USER="maxogram_app"
        run_as_postgres() {{ "$@"; }}
        pg_lsclusters() {{
          cat <<'EOF'
Ver Cluster Port Status Owner Data directory Log file
15 main 55431 online postgres {data15.as_posix()} /tmp/postgresql-15.log
17 main 55432 online postgres {data17.as_posix()} /tmp/postgresql-17.log
EOF
        }}
        resolve_local_postgres_target
        printf 'version=%s\\nport=%s\\nsource=%s\\n' "$POSTGRES_MAJOR_VERSION" "$POSTGRES_PORT" "$POSTGRES_SELECTION_SOURCE"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "version=17" in result.stdout
    assert "port=55432" in result.stdout
    assert "source=highest live major version via pg_lsclusters" in result.stdout


def test_debian_resolve_fails_on_same_major_ambiguity(tmp_path: Path) -> None:
    etc_root = tmp_path / "etc" / "postgresql"
    data_main = tmp_path / "var" / "lib" / "postgresql" / "17" / "main"
    data_alt = tmp_path / "var" / "lib" / "postgresql" / "17" / "alt"
    socket_main = tmp_path / "run" / "postgresql-main"
    socket_alt = tmp_path / "run" / "postgresql-alt"
    config_main = etc_root / "17" / "main" / "postgresql.conf"
    config_alt = etc_root / "17" / "alt" / "postgresql.conf"

    config_main.parent.mkdir(parents=True, exist_ok=True)
    config_alt.parent.mkdir(parents=True, exist_ok=True)
    config_main.write_text("port = 55432\n", encoding="utf-8")
    config_alt.write_text("port = 55433\n", encoding="utf-8")
    write_postmaster_pid(data_main, 55432, socket_main, pid=1717)
    write_postmaster_pid(data_alt, 55433, socket_alt, pid=1818)

    make_versioned_binaries(
        tmp_path,
        "17",
        {
            "55432|SHOW config_file;": config_main.as_posix(),
            "55432|SHOW data_directory;": data_main.as_posix(),
            "55432|SHOW port;": "55432",
            "55433|SHOW config_file;": config_alt.as_posix(),
            "55433|SHOW data_directory;": data_alt.as_posix(),
            "55433|SHOW port;": "55433",
        },
    )

    result = source_install_and_run(
        f"""
        POSTGRES_LIB_ROOT="{(tmp_path / 'lib' / 'postgresql').as_posix()}"
        POSTGRES_ETC_ROOT="{etc_root.as_posix()}"
        DISTRO_FAMILY="debian"
        DB_HOST="127.0.0.1"
        DB_PORT="5432"
        DB_NAME="maxogram"
        DB_USER="maxogram_app"
        run_as_postgres() {{ "$@"; }}
        pg_lsclusters() {{
          cat <<'EOF'
Ver Cluster Port Status Owner Data directory Log file
17 main 55432 online postgres {data_main.as_posix()} /tmp/postgresql-main.log
17 alt 55433 online postgres {data_alt.as_posix()} /tmp/postgresql-alt.log
EOF
        }}
        resolve_local_postgres_target
        """
    )

    assert result.returncode != 0
    assert "Multiple live PostgreSQL targets share the highest major version 17." in result.stderr


def test_generic_resolve_uses_postmaster_pid_metadata(tmp_path: Path) -> None:
    data16 = tmp_path / "var" / "lib" / "pgsql" / "16" / "data"
    socket16 = tmp_path / "run" / "pgsql-16"
    config16 = tmp_path / "etc" / "pgsql-16.conf"

    config16.parent.mkdir(parents=True, exist_ok=True)
    config16.write_text("port = 6543\n", encoding="utf-8")
    (data16 / "PG_VERSION").parent.mkdir(parents=True, exist_ok=True)
    (data16 / "PG_VERSION").write_text("16\n", encoding="utf-8")
    write_postmaster_pid(data16, 6543, socket16, pid=2160)

    make_versioned_binaries(
        tmp_path,
        "16",
        {
            "6543|SHOW config_file;": config16.as_posix(),
            "6543|SHOW data_directory;": data16.as_posix(),
            "6543|SHOW port;": "6543",
        },
    )

    result = source_install_and_run(
        f"""
        POSTGRES_LIB_ROOT="{(tmp_path / 'lib' / 'postgresql').as_posix()}"
        DISTRO_FAMILY="generic"
        SYSTEMCTL_BIN="systemctl"
        DB_HOST="127.0.0.1"
        DB_PORT="5432"
        DB_NAME="maxogram"
        DB_USER="maxogram_app"
        run_as_postgres() {{ "$@"; }}
        ps() {{
          printf '2160 /usr/lib/postgresql/16/bin/postgres -D {data16.as_posix()} -c config_file={config16.as_posix()}\\n'
        }}
        systemctl() {{
          if [[ "$1" == "list-units" ]]; then
            cat <<'EOF'
postgresql-16.service loaded active running PostgreSQL 16
EOF
            return 0
          fi
          if [[ "$1" == "show" && "$2" == "-p" && "$3" == "MainPID" ]]; then
            printf '2160\\n'
            return 0
          fi
          if [[ "$1" == "show" && "$2" == "-p" && "$3" == "ActiveState" ]]; then
            printf 'active\\n'
            return 0
          fi
          return 0
        }}
        resolve_local_postgres_target
        printf 'port=%s\\nservice=%s\\nsource=%s\\n' "$POSTGRES_PORT" "$POSTGRES_SERVICE_UNIT" "$POSTGRES_SELECTION_SOURCE"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "port=6543" in result.stdout
    assert "service=postgresql-16.service" in result.stdout
    assert "source=highest live major version via postmaster.pid" in result.stdout


def test_generic_resolve_prefers_highest_live_major(tmp_path: Path) -> None:
    data14 = tmp_path / "var" / "lib" / "pgsql" / "14" / "data"
    data16 = tmp_path / "var" / "lib" / "pgsql" / "16" / "data"
    socket14 = tmp_path / "run" / "pgsql-14"
    socket16 = tmp_path / "run" / "pgsql-16"
    config14 = tmp_path / "etc" / "pgsql-14.conf"
    config16 = tmp_path / "etc" / "pgsql-16.conf"

    config14.parent.mkdir(parents=True, exist_ok=True)
    config16.parent.mkdir(parents=True, exist_ok=True)
    config14.write_text("port = 5434\n", encoding="utf-8")
    config16.write_text("port = 6543\n", encoding="utf-8")
    (data14 / "PG_VERSION").parent.mkdir(parents=True, exist_ok=True)
    (data16 / "PG_VERSION").parent.mkdir(parents=True, exist_ok=True)
    (data14 / "PG_VERSION").write_text("14\n", encoding="utf-8")
    (data16 / "PG_VERSION").write_text("16\n", encoding="utf-8")
    write_postmaster_pid(data14, 5434, socket14, pid=1414)
    write_postmaster_pid(data16, 6543, socket16, pid=1616)

    make_versioned_binaries(
        tmp_path,
        "14",
        {
            "5434|SHOW config_file;": config14.as_posix(),
            "5434|SHOW data_directory;": data14.as_posix(),
            "5434|SHOW port;": "5434",
        },
    )
    make_versioned_binaries(
        tmp_path,
        "16",
        {
            "6543|SHOW config_file;": config16.as_posix(),
            "6543|SHOW data_directory;": data16.as_posix(),
            "6543|SHOW port;": "6543",
        },
    )

    result = source_install_and_run(
        f"""
        POSTGRES_LIB_ROOT="{(tmp_path / 'lib' / 'postgresql').as_posix()}"
        DISTRO_FAMILY="generic"
        SYSTEMCTL_BIN="systemctl"
        DB_HOST="127.0.0.1"
        DB_PORT="5432"
        DB_NAME="maxogram"
        DB_USER="maxogram_app"
        run_as_postgres() {{ "$@"; }}
        ps() {{
          printf '1414 /usr/lib/postgresql/14/bin/postgres -D {data14.as_posix()} -c config_file={config14.as_posix()}\\n'
          printf '1616 /usr/lib/postgresql/16/bin/postgres -D {data16.as_posix()} -c config_file={config16.as_posix()}\\n'
        }}
        systemctl() {{
          if [[ "$1" == "list-units" ]]; then
            cat <<'EOF'
postgresql-14.service loaded active running PostgreSQL 14
postgresql-16.service loaded active running PostgreSQL 16
EOF
            return 0
          fi
          if [[ "$1" == "show" && "$2" == "-p" && "$3" == "MainPID" ]]; then
            case "$5" in
              postgresql-14.service) printf '1414\\n' ;;
              postgresql-16.service) printf '1616\\n' ;;
            esac
            return 0
          fi
          if [[ "$1" == "show" && "$2" == "-p" && "$3" == "ActiveState" ]]; then
            printf 'active\\n'
            return 0
          fi
          return 0
        }}
        resolve_local_postgres_target
        printf 'version=%s\\nport=%s\\n' "$POSTGRES_MAJOR_VERSION" "$POSTGRES_PORT"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "version=16" in result.stdout
    assert "port=6543" in result.stdout


def test_set_postgres_port_rewrites_selected_config_and_reselects_target(tmp_path: Path) -> None:
    etc_root = tmp_path / "etc" / "postgresql"
    data17 = tmp_path / "var" / "lib" / "postgresql" / "17" / "main"
    socket17 = tmp_path / "run" / "postgresql-17"
    config17 = etc_root / "17" / "main" / "postgresql.conf"

    config17.parent.mkdir(parents=True, exist_ok=True)
    config17.write_text("port = 55432\n", encoding="utf-8")
    write_postmaster_pid(data17, 55432, socket17, pid=1717)

    write_fake_createdb(tmp_path / "lib" / "postgresql" / "17" / "bin" / "createdb")
    write_executable(
        tmp_path / "lib" / "postgresql" / "17" / "bin" / "psql",
        f"""#!/usr/bin/env bash
        set -euo pipefail
        sql=""
        while (($#)); do
          case "$1" in
            -tAc|-c)
              sql="$2"
              shift 2
              ;;
            *)
              shift
              ;;
          esac
        done
        current_port="$(grep -E '^[[:space:]]*port[[:space:]]*=' {config17.as_posix()!r} | awk '{{print $3}}')"
        case "$sql" in
          'SHOW config_file;')
            printf '%s\\n' {config17.as_posix()!r}
            ;;
          'SHOW data_directory;')
            printf '%s\\n' {data17.as_posix()!r}
            ;;
          'SHOW port;')
            printf '%s\\n' "$current_port"
            ;;
        esac
        """,
    )

    result = source_install_and_run(
        f"""
        POSTGRES_LIB_ROOT="{(tmp_path / 'lib' / 'postgresql').as_posix()}"
        POSTGRES_ETC_ROOT="{etc_root.as_posix()}"
        DISTRO_FAMILY="debian"
        DB_HOST="127.0.0.1"
        DB_PORT="5432"
        DB_NAME="maxogram"
        DB_USER="maxogram_app"
        run_as_postgres() {{ "$@"; }}
        pg_lsclusters() {{
          current_port="$(grep -E '^[[:space:]]*port[[:space:]]*=' {config17.as_posix()!r} | awk '{{print $3}}')"
          cat <<EOF
Ver Cluster Port Status Owner Data directory Log file
17 main $current_port online postgres {data17.as_posix()} /tmp/postgresql-17.log
EOF
        }}
        service_restart_candidates() {{ return 0; }}
        resolve_local_postgres_target
        set_postgres_port "60000" "$POSTGRES_PORT"
        printf 'port=%s\\nconfig=%s\\n' "$POSTGRES_PORT" "$(grep -E '^[[:space:]]*port[[:space:]]*=' {config17.as_posix()!r} | awk '{{print $3}}')"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "port=60000" in result.stdout
    assert "config=60000" in result.stdout


def test_collect_update_inputs_skips_local_postgres_for_remote_host() -> None:
    result = source_install_and_run(
        """
        ENV_FILE_ALREADY_PRESENT="true"
        TG_BOT_TOKEN="telegram-token"
        MAX_BOT_TOKEN="max-token"
        DB_HOST="db.example.com"
        DB_PORT="55432"
        DB_NAME="maxogram"
        DB_USER="maxogram_app"
        DB_PASSWORD="secret"
        DB_SCHEMA="maxogram"
        ensure_postgres_installed() { echo "should-not-run"; exit 99; }
        resolve_local_postgres_target() { echo "should-not-run"; exit 98; }
        collect_update_inputs
        printf 'local=%s\\nport=%s\\n' "$LOCAL_DB_TARGET" "$DB_PORT"
        """
    )

    assert result.returncode == 0, result.stderr
    assert "should-not-run" not in result.stdout
    assert "local=false" in result.stdout
    assert "port=55432" in result.stdout
