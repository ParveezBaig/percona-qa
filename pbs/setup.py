"""Scenario setup/config layer for the binlog_server fetch/pull streaming
tests.

Lives in pbs/ (not tests/) alongside servers.py and util.py, the other
two shared framework modules -- tests/ holds only the test scripts
themselves and their own optional *_test.config files. Each test script
adds pbs/ to sys.path (see any test's own top) before importing this.

Test config loading and validation, generating the binlog_server
main_config.json, the shared ./logs layout, and the argparse flags common
to every test all live here, on top of servers.py's process wrapper
classes. This is also where ScenarioRunner lives: the base class that
owns the setup/teardown lifecycle every test shares (load config, start
the server, prepare sysbench data, and -- no matter how the scenario
exits -- stop any still-running background sysbench/pull process, then
the server, then log where the logs were preserved). Each test's own
scenario class overrides just `scenario()` with its test-specific steps;
see each script's own module docstring for what it tests:
gtid_rewrite_test.py, nogtid_norewrite_test.py, rewrite_mode_transition_test.py,
pull_restart_resilience_test.py, pull_purge_resume_test.py,
binlog_event_diversity_test.py.

basedir, binlog_server_bin, and storage backend/S3 settings come from a
JSON test config file (--config, default: config.json in pbs/), not from
command-line flags; when its "s3" section has no
"bucket" set, storage defaults to local file-backed storage. "encryption"
(unset/""/0/false/"0" all mean disabled) turns on storage.encryption, with
"encryption_kek_id"/"encryption_cipher" as optional overrides for
generate_config.py's own --encryption defaults; a keyring file is
auto-created under ./logs the first time it's needed. Test logs, the
generated main_config.json, binsrv.log, the mysqld datadir, and storage
all live under ./logs (relative to the directory the test is run from) --
specifically ./logs/binsrv_data (local storage), ./logs/binsrv_tmp
(fs_buffer_directory, used for the S3 backend), and
./logs/keyring_data.json. ./logs is cleared and recreated at the start of
each run.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Optional

from servers import BinlogServer, ExistingServer, MysqldServer, Sysbench, wait_for_pull_catch_up
from util import LOG, TestFailure, assert_response_ok, configure_logging, find_free_port, run, tail_log_file, total_bytes

REPL_USER = "repl_test"
REPL_PASSWORD = "ReplTestPw_1!"
SBTEST_USER = "sbtest_test"
SBTEST_PASSWORD = "SbtestTestPw_1!"
SBTEST_DB = "sbtest"

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "config.json"
DEFAULT_GENERATE_CONFIG_SCRIPT = Path(__file__).resolve().parent / "scripts" / "generate_config.py"

# Kept in sync with generate_config.py's own --encryption default: used here
# only to name the keyring entry we auto-generate when the test config's
# 'encryption_kek_id' is left unset, matching whatever generate_config.py
# will default storage.encryption.kek_id to.
DEFAULT_ENCRYPTION_KEK_ID = "alpha"

SKIP_SERVER_SETUP_ENV = "SKIP_SERVER_SETUP"


def skip_server_setup_enabled() -> bool:
    return os.environ.get(SKIP_SERVER_SETUP_ENV, "0") == "1"


def load_test_config(path: Path) -> dict:
    if not path.is_file():
        raise TestFailure(
            f"test config file not found: {path}\n"
            f"Copy/edit it from the template at {DEFAULT_CONFIG_PATH} and fill in at least "
            f"'basedir' and 'binlog_server_bin'."
        )
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise TestFailure(f"{path}: invalid JSON: {exc}") from exc

    binlog_server_bin = data.get("binlog_server_bin") or ""
    if not binlog_server_bin:
        raise TestFailure(f"{path}: 'binlog_server_bin' must be set to a built binlog_server binary")

    # empty/unset means "look up 'sysbench' on PATH", same as generate_config.py's
    # env-var defaults do for other tools.
    sysbench_bin = data.get("sysbench_bin") or "sysbench"
    sysbench_table_count = int(data.get("sysbench_table_count") or 4)
    sysbench_threads_count = int(data.get("sysbench_threads_count") or 4)
    sysbench_table_size = int(data.get("sysbench_table_size") or 10000)
    sysbench_script = data.get("sysbench_script") or "oltp_read_write"
    # Unset/""/0 means "no override" -- every scenario that runs sysbench
    # in the background keeps using its own --background-duration
    # (default/CLI) unchanged; see ScenarioRunner.run().
    sysbench_run_time = int(data.get("sysbench_run_time") or 0)
    fetch_timeout = int(data.get("fetch_timeout") or 300)

    basedir = data.get("basedir") or ""

    connection = data.get("connection") or {}
    conn = {
        "host": connection.get("host") or "",
        "port": int(connection.get("port") or 3306),
        "user": connection.get("user") or "",
        "password": connection.get("password") or "",
    }

    s3 = data.get("s3") or {}
    # S3 is only used when it has actually been configured; otherwise storage
    # defaults to local file-backed storage under logs/binsrv_data.
    backend = "s3" if s3.get("bucket") else "file"

    # Unset, "", 0, false, and the string "0" all mean "no encryption" --
    # Python already treats the first four as falsy, "0" needs calling out
    # explicitly since a non-empty string is otherwise truthy.
    encryption_enabled = data.get("encryption") not in (None, "", 0, False, "0")
    encryption_kek_id = data.get("encryption_kek_id") or ""
    encryption_cipher = data.get("encryption_cipher") or ""

    return {
        "basedir": Path(basedir) if basedir else None,
        "binlog_server_bin": binlog_server_bin,
        "sysbench_bin": sysbench_bin,
        "sysbench_table_count": sysbench_table_count,
        "sysbench_threads_count": sysbench_threads_count,
        "sysbench_table_size": sysbench_table_size,
        "sysbench_script": sysbench_script,
        "sysbench_run_time": sysbench_run_time,
        "fetch_timeout": fetch_timeout,
        "encryption_enabled": encryption_enabled,
        "encryption_kek_id": encryption_kek_id,
        "encryption_cipher": encryption_cipher,
        "connection": conn,
        "backend": backend,
        "s3": s3,
    }


def validate_environment(test_config: dict, skip_server_setup: bool) -> None:
    if skip_server_setup:
        conn = test_config["connection"]
        if not conn["host"] or not conn["user"]:
            raise TestFailure(
                f"{SKIP_SERVER_SETUP_ENV}=1 requires 'connection.host' and 'connection.user' "
                "to be set in the test config"
            )
        if not shutil.which("mysql"):
            raise TestFailure(f"{SKIP_SERVER_SETUP_ENV}=1 requires the 'mysql' client binary on PATH")
    else:
        if test_config["basedir"] is None:
            raise TestFailure("'basedir' must be set to a MySQL/Percona Server basedir")
        if not (test_config["basedir"] / "bin" / "mysqld").exists():
            raise TestFailure(f"mysqld not found under basedir: {test_config['basedir']}")
        if not (test_config["basedir"] / "bin" / "mysql").exists():
            raise TestFailure(f"mysql client not found under basedir: {test_config['basedir']}")
    if not Path(test_config["binlog_server_bin"]).is_file():
        raise TestFailure(f"binlog_server_bin not found: {test_config['binlog_server_bin']}")
    sysbench_bin = test_config["sysbench_bin"]
    if not shutil.which(sysbench_bin) and not Path(sysbench_bin).is_file():
        raise TestFailure(f"sysbench binary not found: {sysbench_bin}")
    if test_config["backend"] == "s3" and not test_config["s3"].get("access_key_id"):
        raise TestFailure("'s3.bucket' is set but 's3.access_key_id' is missing in the test config")


def ensure_keyring_file(keyring_path: Path, kek_id: str) -> None:
    """Create a keyring file with a single random AES-128-ECB key under
    kek_id, unless one is already there. Matches the format and the
    kek/cipher combination documented as working in the
    percona-binlog-server README's own example config."""
    if keyring_path.exists():
        return
    keyring_path.parent.mkdir(parents=True, exist_ok=True)
    key_hex = os.urandom(16).hex()
    keyring_path.write_text(json.dumps({
        "version": 1,
        "keys": [{"id": kek_id, "cipher": "AES-128-ECB", "data_hex": key_hex}],
    }, indent=2))
    os.chmod(keyring_path, 0o600)


def generate_binsrv_config(
    args: argparse.Namespace, test_config: dict, *, output_path: Path, log_path: Path,
    replication_mode: str, use_rewrite: bool, storage_dir: Path, buffer_dir: Path, keyring_path: Path,
    host: str, port: int, user: str, password: str,
) -> None:
    backend = test_config["backend"]
    cmd = [
        sys.executable, str(args.generate_config_script),
        "--non-interactive", "--force",
        "--host", host, "--port", str(port),
        "--user", user, "--password", password,
        "--replication-mode", replication_mode,
        "--checkpoint-size", args.checkpoint_size,
        "--checkpoint-interval", args.checkpoint_interval,
        "--read-timeout", str(args.read_timeout),
        "--log-level", "debug", "--log-file", str(log_path),
        "--storage-backend", backend,
        "-o", str(output_path),
    ]
    if use_rewrite:
        cmd += ["--rewrite-base-file-name", "bnlg", "--rewrite-file-size", args.rewrite_file_size]
    if backend == "file":
        cmd += ["--storage-path", str(storage_dir)]
    else:
        s3 = test_config["s3"]
        cmd += ["--fs-buffer-directory", str(buffer_dir)]
        for flag, key in (
            ("--s3-access-key-id", "access_key_id"),
            ("--s3-secret-access-key", "secret_access_key"),
            ("--s3-secret-access-key-env", "secret_access_key_env"),
            ("--s3-bucket", "bucket"),
            ("--s3-region", "region"),
            ("--s3-endpoint", "endpoint"),
            ("--s3-path", "path"),
        ):
            value = s3.get(key)
            if value:
                cmd += [flag, value]

    if test_config["encryption_enabled"]:
        kek_id = test_config["encryption_kek_id"] or DEFAULT_ENCRYPTION_KEK_ID
        ensure_keyring_file(keyring_path, kek_id)
        cmd += ["--keyring-path", str(keyring_path), "--encryption"]
        if test_config["encryption_kek_id"]:
            cmd += ["--encryption-kek-id", test_config["encryption_kek_id"]]
        if test_config["encryption_cipher"]:
            cmd += ["--encryption-cipher", test_config["encryption_cipher"]]

    LOG.info("generating config at %s (backend=%s)", output_path, backend)
    run(cmd, capture=True, timeout=30)


def build_common_parser(description: str) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=description, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--config", default=str(DEFAULT_CONFIG_PATH),
        help=(
            "path to the test config file (basedir, binlog_server_bin, sysbench_bin/"
            "sysbench_table_count/sysbench_threads_count/sysbench_table_size/sysbench_script/"
            "sysbench_run_time, fetch_timeout, s3 settings) (default: %(default)s)"
        ),
    )
    p.add_argument("--generate-config-script", default=str(DEFAULT_GENERATE_CONFIG_SCRIPT))
    p.add_argument("--port", type=int, default=0, help="fixed mysqld port (default: pick a free one)")

    p.add_argument(
        "--short-run-duration", type=int, default=20,
        help="seconds for the bounded sysbench run (default: %(default)s)",
    )

    p.add_argument(
        "--max-binlog-size", default="4M",
        help="mysqld --max-binlog-size, kept small to force rotation during the test (default: %(default)s)",
    )
    p.add_argument("--checkpoint-size", default="1M")
    p.add_argument("--checkpoint-interval", default="5s")
    p.add_argument(
        "--read-timeout", type=int, default=10,
        help=(
            "connection.read_timeout in the generated config; kept short so pull unblocks "
            "and notices SIGTERM quickly once idle, instead of blocking on the network read "
            "for as long as the default (60s) (default: %(default)s)"
        ),
    )

    p.add_argument("--verbose", action="store_true")
    return p


def add_background_duration_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--background-duration", type=int, default=300,
        help="seconds for the background sysbench run + concurrent pull (default: %(default)s = 5 minutes)",
    )


def add_catch_up_args(p: argparse.ArgumentParser) -> None:
    """Flags for scenarios that poll for `pull` to catch up before stopping
    it gracefully."""
    p.add_argument(
        "--catch-up-timeout", type=int, default=120,
        help=(
            "max seconds to wait for pull to actually finish downloading everything the "
            "server has before stopping it (default: %(default)s)"
        ),
    )
    p.add_argument("--catch-up-poll-interval", type=int, default=3, help="seconds between catch-up checks (default: %(default)s)")
    p.add_argument(
        "--pull-stop-timeout", type=int, default=60,
        help="max seconds to wait for pull to exit gracefully after SIGTERM before killing it (default: %(default)s)",
    )


def add_pull_test_args(p: argparse.ArgumentParser) -> None:
    """Extra flags for scenarios that also drive `pull` (not used by the
    fetch-only rewrite-mode-transition test)."""
    add_background_duration_arg(p)
    add_catch_up_args(p)


@dataclasses.dataclass
class ScenarioSetup:
    test_config: dict
    work_dir: Path
    storage_dir: Path
    buffer_dir: Path
    keyring_path: Path
    config_path: Path
    binsrv_log_path: Path
    host: str
    port: int
    server: Any
    sysbench: "Sysbench"


def setup_scenario(args: argparse.Namespace, *, gtid_mode: bool) -> Optional[ScenarioSetup]:
    """Prologue shared by every scenario runner: load/validate the test
    config, recreate the ./logs layout, and construct the server (self-managed
    mysqld, or an adapter for an already-running one under SKIP_SERVER_SETUP=1)
    and sysbench handles. Returns None (after logging the error) on failure."""
    skip_server_setup = skip_server_setup_enabled()
    try:
        test_config = load_test_config(Path(args.config))
        validate_environment(test_config, skip_server_setup)
    except TestFailure as exc:
        LOG.error("%s", exc)
        return None

    work_dir = Path.cwd() / "logs"
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True)
    LOG.info("test logs: %s", work_dir)

    storage_dir = work_dir / "binsrv_data"
    buffer_dir = work_dir / "binsrv_tmp"
    keyring_path = work_dir / "keyring_data.json"
    storage_dir.mkdir(parents=True, exist_ok=True)
    buffer_dir.mkdir(parents=True, exist_ok=True)

    config_path = work_dir / "main_config.json"
    binsrv_log_path = work_dir / "binsrv.log"

    if skip_server_setup:
        conn = test_config["connection"]
        host, port = conn["host"], conn["port"]
        server: Any = ExistingServer(shutil.which("mysql"), host, port, conn["user"], conn["password"])
    else:
        host = "127.0.0.1"
        port = args.port or find_free_port()
        server = MysqldServer(test_config["basedir"], work_dir / "data", port, gtid_mode, args.max_binlog_size)

    sysbench = Sysbench(
        test_config["sysbench_bin"], host, port, SBTEST_USER, SBTEST_PASSWORD, SBTEST_DB,
        test_config["sysbench_table_count"], test_config["sysbench_table_size"],
        test_config["sysbench_threads_count"], test_config["sysbench_script"],
    )

    return ScenarioSetup(
        test_config, work_dir, storage_dir, buffer_dir, keyring_path,
        config_path, binsrv_log_path, host, port, server, sysbench,
    )


class ScenarioRunner:
    """Base class for a test scenario.

    Owns the setup/teardown lifecycle every test shares: load the test
    config, build+start the server and sysbench, create the test's
    accounts, prepare the initial sysbench load's data on disk -- then run
    the subclass's `scenario()` -- and, no matter how `scenario()` exits,
    always (in this order) stop any still-running background sysbench run,
    stop `pull` if it's still running, stop the server, and log where the
    logs were preserved.

    A subclass only needs to override `scenario()` with its test-specific
    steps; `self.server`, `self.sysbench`, `self.binlog_srv`,
    `self.config_path`, etc. (see `run()` below for the full set) are all
    ready by the time `scenario()` runs. If the scenario drives `pull` or a
    background sysbench run, assign the Popen to `self.pull_process`/
    `self.sysbench_bg` so the shared teardown can stop it; a scenario that
    never touches one of those (e.g. a fetch-only test has no
    pull_process) leaves it None, which the teardown treats as a no-op.
    Raise TestFailure from `scenario()` for any failed assertion or
    subprocess call; `run()` catches it, logs it, and returns False.
    """

    #: overridden by a subclass whose test needs server gtid_mode=OFF
    gtid_mode: bool = True

    def __init__(self, args: argparse.Namespace, *, name: str):
        self.args = args
        self.name = name
        self.pull_process: Optional[subprocess.Popen] = None
        self.sysbench_bg: Optional[subprocess.Popen] = None

    def scenario(self) -> None:
        raise NotImplementedError

    def run(self) -> bool:
        configure_logging(self.args.verbose)

        setup = setup_scenario(self.args, gtid_mode=self.gtid_mode)
        if setup is None:
            return False
        self.setup = setup
        self.test_config = setup.test_config
        self.work_dir = setup.work_dir
        self.storage_dir = setup.storage_dir
        self.buffer_dir = setup.buffer_dir
        self.keyring_path = setup.keyring_path
        self.config_path = setup.config_path
        self.binsrv_log_path = setup.binsrv_log_path
        self.host = setup.host
        self.port = setup.port
        self.server = setup.server
        self.sysbench = setup.sysbench
        self.binlog_srv = BinlogServer(self.test_config["binlog_server_bin"])

        # config.json's sysbench_run_time, when set, overrides the
        # background/soak sysbench run's duration wherever a scenario uses
        # one (--background-duration) -- it never affects the short
        # blocking bursts (--short-run-duration), which stay CLI-only.
        # Applied once here, before scenario() runs, so it flows through
        # unchanged to every downstream use (including the
        # required_duration floor some scenarios compute on top of it).
        sysbench_run_time = self.test_config.get("sysbench_run_time")
        if sysbench_run_time and hasattr(self.args, "background_duration"):
            LOG.info(
                "[%s] sysbench_run_time=%ss (test config) overrides --background-duration (was %ss)",
                self.name, sysbench_run_time, self.args.background_duration,
            )
            self.args.background_duration = sysbench_run_time

        ok = False
        try:
            self.server.initialize()
            self.server.start()
            self.server.setup_test_accounts()

            self.scenario()
            ok = True
        except TestFailure as exc:
            LOG.error("[%s] FAILED: %s", self.name, exc)
            ok = False
        finally:
            if self.sysbench_bg is not None and self.sysbench_bg.poll() is None:
                self.sysbench_bg.terminate()
            if self.pull_process is not None:
                self.binlog_srv.stop_pull(self.pull_process)
            self.server.stop()
            LOG.info("[%s] test logs preserved at %s", self.name, self.work_dir)
        return ok


class FetchPullStreamingScenario(ScenarioRunner):
    """Shared scenario used by gtid_rewrite_test.py and
    nogtid_norewrite_test.py, against a throwaway mysqld (or an existing
    server under SKIP_SERVER_SETUP=1):

      1. sysbench prepare ("load"), then `fetch` once.
      2. A bounded sysbench run for new transactions, then `fetch` again.
      3. sysbench in the background for --background-duration while `pull`
         streams concurrently. Once sysbench finishes, poll until storage
         actually has everything the server has -- exact binlog file name
         match for the non-rewrite scenario, full GTID coverage via
         search_by_gtid_set for the rewrite one (names don't match 1:1
         after rewriting) -- before stopping `pull` (SIGTERM, so it shuts
         down gracefully and flushes), then re-verify completeness once
         more.
    """

    def __init__(
        self, args: argparse.Namespace, *, name: str, gtid_mode: bool, replication_mode: str, use_rewrite: bool,
    ):
        super().__init__(args, name=name)
        self.gtid_mode = gtid_mode
        self.replication_mode = replication_mode
        self.use_rewrite = use_rewrite

    def scenario(self) -> None:
        args, test_config, name = self.args, self.test_config, self.name

        LOG.info("[%s] preparing sysbench data (initial load)", name)
        self.sysbench.prepare()

        generate_binsrv_config(
            args, test_config, output_path=self.config_path, log_path=self.binsrv_log_path,
            replication_mode=self.replication_mode, use_rewrite=self.use_rewrite,
            storage_dir=self.storage_dir, buffer_dir=self.buffer_dir, keyring_path=self.keyring_path,
            host=self.host, port=self.port, user=REPL_USER, password=REPL_PASSWORD,
        )

        LOG.info("[%s] fetch #1 (initial load)", name)
        self.binlog_srv.fetch(self.config_path, timeout=test_config["fetch_timeout"])
        listing_1 = self.binlog_srv.list(self.config_path)
        assert_response_ok(listing_1, "list after fetch #1")
        bytes_1 = total_bytes(listing_1)
        if bytes_1 <= 0:
            raise TestFailure("fetch #1 stored no data")
        LOG.info("[%s] fetch #1 stored %d bytes across %d file(s)", name, bytes_1, len(listing_1.get("result", [])))

        LOG.info("[%s] sysbench run (%ss) for new transactions", name, args.short_run_duration)
        self.sysbench.run_blocking(args.short_run_duration)

        LOG.info("[%s] fetch #2 (new transactions)", name)
        self.binlog_srv.fetch(self.config_path, timeout=test_config["fetch_timeout"])
        listing_2 = self.binlog_srv.list(self.config_path)
        assert_response_ok(listing_2, "list after fetch #2")
        bytes_2 = total_bytes(listing_2)
        LOG.info("[%s] fetch #2 stored %d bytes across %d file(s)", name, bytes_2, len(listing_2.get("result", [])))
        if bytes_2 <= bytes_1:
            raise TestFailure(f"fetch #2 did not grow storage ({bytes_2} <= {bytes_1} bytes)")

        LOG.info("[%s] sysbench run in background for %ss with concurrent pull", name, args.background_duration)
        self.sysbench_bg = self.sysbench.run_background(args.background_duration)
        self.pull_process = self.binlog_srv.start_pull(self.config_path)

        time.sleep(2)
        if self.pull_process.poll() is not None:
            raise TestFailure(
                f"pull exited immediately (code {self.pull_process.returncode}); "
                f"last lines of {self.binsrv_log_path}:\n{tail_log_file(self.binsrv_log_path)}"
            )

        try:
            self.sysbench_bg.wait(timeout=args.background_duration + 120)
        except subprocess.TimeoutExpired as exc:
            raise TestFailure(
                f"background sysbench run did not finish within {args.background_duration + 120}s"
            ) from exc
        if self.sysbench_bg.returncode != 0:
            raise TestFailure(f"background sysbench run failed (exit {self.sysbench_bg.returncode})")
        self.sysbench_bg = None

        gtid_after_load = self.server.gtid_executed()
        server_binlogs = self.server.show_binary_logs()

        LOG.info("[%s] waiting up to %ss for pull to catch up with the server", name, args.catch_up_timeout)
        caught_up = wait_for_pull_catch_up(
            self.binlog_srv, self.config_path, use_rewrite=self.use_rewrite,
            gtid_target=gtid_after_load, server_binlogs=server_binlogs,
            timeout=args.catch_up_timeout, poll_interval=args.catch_up_poll_interval,
        )
        if not caught_up:
            raise TestFailure(
                f"pull did not catch up with the server within {args.catch_up_timeout}s; "
                f"see {self.binsrv_log_path}"
            )
        LOG.info("[%s] pull has downloaded everything the server has; stopping it", name)

        rc = self.binlog_srv.stop_pull(self.pull_process, timeout=args.pull_stop_timeout)
        self.pull_process = None
        if rc != 0:
            raise TestFailure(
                f"pull did not shut down cleanly (exit {rc}); "
                f"last lines of {self.binsrv_log_path}:\n{tail_log_file(self.binsrv_log_path)}"
            )

        final_listing = self.binlog_srv.list(self.config_path)
        assert_response_ok(final_listing, "final list after pull")
        LOG.info(
            "[%s] storage now has %d file(s), %d bytes",
            name, len(final_listing.get("result", [])), total_bytes(final_listing),
        )

        if self.use_rewrite:
            LOG.info("[%s] verifying full GTID coverage via search_by_gtid_set", name)
            search_result = self.binlog_srv.search_by_gtid_set(self.config_path, gtid_after_load)
            assert_response_ok(search_result, "search_by_gtid_set for the fully executed GTID set")
        else:
            stored_names = {r["name"] for r in final_listing.get("result", [])}
            missing = set(server_binlogs) - stored_names
            if missing:
                raise TestFailure(
                    f"binlog files present on the server but missing from storage: {sorted(missing)}"
                )
            LOG.info("[%s] all %d server binlog file(s) are present in storage", name, len(server_binlogs))

        LOG.info("[%s] PASSED", name)
