#!/usr/bin/env python3
"""
NyoSig Analysator — Analytics Log Engine
Comprehensive operation profiling with timing data in a SEPARATE database.

Purpose: Record every operation with start/end timestamps, duration,
parameters, results, and errors. Enables:
  - Performance analysis (which layers are slow, which APIs timeout)
  - System tuning (optimal rate limits, cache TTLs, batch sizes)
  - Audit trail (complete history of what the system did and when)
  - Bottleneck identification (where time is spent per run)

Separate DB: analytics_log.db (does NOT pollute the main data DB)
"""
import os
import sys
import time
import json
import sqlite3
import threading
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

# =====================================================================
# ANALYTICS DB (separate from main DB)
# =====================================================================

_LOG_DB_NAME = "analytics_log.db"
_log_lock = threading.Lock()


def get_analytics_db_path(project_root: str) -> str:
    return os.path.join(project_root, "db", _LOG_DB_NAME)


def _connect_analytics(project_root: str) -> sqlite3.Connection:
    path = get_analytics_db_path(project_root)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    return con


def ensure_analytics_schema(con: sqlite3.Connection):
    """Create all analytics tables. Idempotent."""

    # ---- run_profile: one row per complete pipeline run ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS run_profile (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            app_version     TEXT,
            scope           TEXT,
            started_utc     TEXT    NOT NULL,
            finished_utc    TEXT,
            duration_s      REAL,
            status          TEXT    NOT NULL DEFAULT 'running',
            total_api_calls INTEGER DEFAULT 0,
            total_db_writes INTEGER DEFAULT 0,
            candidates_n    INTEGER DEFAULT 0,
            layers_run      INTEGER DEFAULT 0,
            errors_count    INTEGER DEFAULT 0,
            config_json     TEXT,
            summary_json    TEXT
        );
    """)

    # ---- operation_log: every individual operation with timing ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS operation_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER,
            profile_id      INTEGER,
            operation       TEXT    NOT NULL,
            category        TEXT    NOT NULL,
            started_utc     TEXT    NOT NULL,
            finished_utc    TEXT,
            duration_ms     REAL,
            status          TEXT    NOT NULL DEFAULT 'running',
            input_params    TEXT,
            output_summary  TEXT,
            error_message   TEXT,
            api_calls       INTEGER DEFAULT 0,
            db_writes       INTEGER DEFAULT 0,
            items_processed INTEGER DEFAULT 0,
            bytes_received  INTEGER DEFAULT 0,
            FOREIGN KEY (profile_id) REFERENCES run_profile(id)
        );
    """)

    # ---- api_call_log: every external API call ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS api_call_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER,
            operation_id    INTEGER,
            provider        TEXT    NOT NULL,
            endpoint        TEXT,
            method          TEXT    DEFAULT 'GET',
            started_utc     TEXT    NOT NULL,
            finished_utc    TEXT,
            duration_ms     REAL,
            http_status     INTEGER,
            response_bytes  INTEGER,
            was_cached      INTEGER DEFAULT 0,
            was_rate_limited INTEGER DEFAULT 0,
            wait_time_ms    REAL    DEFAULT 0,
            error           TEXT,
            FOREIGN KEY (operation_id) REFERENCES operation_log(id)
        );
    """)

    # ---- layer_timing: per-layer execution summary ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS layer_timing (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            profile_id      INTEGER,
            layer_name      TEXT    NOT NULL,
            scope_key       TEXT    NOT NULL,
            started_utc     TEXT    NOT NULL,
            finished_utc    TEXT,
            duration_ms     REAL,
            status          TEXT,
            symbols_input   INTEGER DEFAULT 0,
            symbols_scored  INTEGER DEFAULT 0,
            api_calls       INTEGER DEFAULT 0,
            cache_hits      INTEGER DEFAULT 0,
            rate_limit_waits INTEGER DEFAULT 0,
            total_wait_ms   REAL    DEFAULT 0,
            error           TEXT,
            FOREIGN KEY (profile_id) REFERENCES run_profile(id)
        );
    """)

    # ---- performance_summary: daily aggregated metrics ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS performance_summary (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date_utc        TEXT    NOT NULL,
            total_runs      INTEGER DEFAULT 0,
            avg_run_duration_s REAL,
            total_api_calls INTEGER DEFAULT 0,
            total_rate_limit_waits INTEGER DEFAULT 0,
            total_errors    INTEGER DEFAULT 0,
            slowest_layer   TEXT,
            slowest_layer_ms REAL,
            fastest_layer   TEXT,
            fastest_layer_ms REAL,
            cache_hit_rate  REAL,
            computed_utc    TEXT    NOT NULL
        );
    """)

    con.execute("CREATE INDEX IF NOT EXISTS idx_oplog_run ON operation_log(run_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_oplog_cat ON operation_log(category);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_api_run ON api_call_log(run_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_layer_run ON layer_timing(run_id);")
    con.commit()


# =====================================================================
# ANALYTICS LOGGER (thread-safe, context-manager based)
# =====================================================================

class AnalyticsLogger:
    """
    Thread-safe logger for operation profiling.
    Use as context manager for automatic timing:

        with logger.operation("fetch_spot", "pipeline", run_id=42):
            ... do work ...

    Or manually:
        op_id = logger.start_operation("fetch_spot", "pipeline", run_id=42)
        ... do work ...
        logger.end_operation(op_id, status="ok", items_processed=250)
    """

    def __init__(self, project_root: str):
        self._root = project_root
        self._con = _connect_analytics(project_root)
        ensure_analytics_schema(self._con)
        self._current_profile_id = None

    def close(self):
        try:
            self._con.close()
        except Exception:
            pass

    # --- Run Profile ---

    def start_run_profile(self, run_id, app_version="", scope="", config=None):
        """Start tracking a complete pipeline run."""
        with _log_lock:
            cur = self._con.execute(
                "INSERT INTO run_profile "
                "(run_id, app_version, scope, started_utc, config_json) "
                "VALUES (?, ?, ?, ?, ?);",
                (run_id, app_version, scope,
                 _utc_now(), json.dumps(config or {})))
            self._con.commit()
            self._current_profile_id = cur.lastrowid
            return cur.lastrowid

    def end_run_profile(self, profile_id=None, status="completed",
                         candidates_n=0, summary=None):
        """Finish tracking a pipeline run. Computes total duration."""
        pid = profile_id or self._current_profile_id
        if not pid:
            return
        with _log_lock:
            row = self._con.execute(
                "SELECT started_utc FROM run_profile WHERE id=?;", (pid,)).fetchone()
            dur = _duration_since(row[0]) if row else 0
            # Aggregate stats from operation_log
            stats = self._con.execute(
                "SELECT COUNT(*), SUM(api_calls), SUM(db_writes), "
                "SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) "
                "FROM operation_log WHERE profile_id=?;", (pid,)).fetchone()
            layers = self._con.execute(
                "SELECT COUNT(*) FROM layer_timing WHERE profile_id=?;",
                (pid,)).fetchone()
            self._con.execute(
                "UPDATE run_profile SET finished_utc=?, duration_s=?, status=?, "
                "total_api_calls=?, total_db_writes=?, candidates_n=?, "
                "layers_run=?, errors_count=?, summary_json=? WHERE id=?;",
                (_utc_now(), round(dur, 2), status,
                 stats[1] or 0, stats[2] or 0, candidates_n,
                 layers[0] or 0, stats[3] or 0,
                 json.dumps(summary or {}), pid))
            self._con.commit()

    # --- Operation Log ---

    def start_operation(self, operation: str, category: str, run_id=None,
                         input_params=None) -> int:
        """Start tracking an individual operation. Returns operation ID."""
        with _log_lock:
            cur = self._con.execute(
                "INSERT INTO operation_log "
                "(run_id, profile_id, operation, category, started_utc, input_params) "
                "VALUES (?, ?, ?, ?, ?, ?);",
                (run_id, self._current_profile_id, operation, category,
                 _utc_now(), json.dumps(input_params or {})))
            self._con.commit()
            return cur.lastrowid

    def end_operation(self, op_id: int, status="ok", output_summary=None,
                       error_message=None, api_calls=0, db_writes=0,
                       items_processed=0, bytes_received=0):
        """Finish tracking an operation. Computes duration."""
        with _log_lock:
            row = self._con.execute(
                "SELECT started_utc FROM operation_log WHERE id=?;", (op_id,)).fetchone()
            dur_ms = _duration_since(row[0]) * 1000 if row else 0
            self._con.execute(
                "UPDATE operation_log SET finished_utc=?, duration_ms=?, status=?, "
                "output_summary=?, error_message=?, api_calls=?, db_writes=?, "
                "items_processed=?, bytes_received=? WHERE id=?;",
                (_utc_now(), round(dur_ms, 1), status,
                 json.dumps(output_summary) if output_summary else None,
                 error_message, api_calls, db_writes,
                 items_processed, bytes_received, op_id))
            self._con.commit()

    @contextmanager
    def operation(self, name: str, category: str, run_id=None, input_params=None):
        """Context manager for automatic operation timing."""
        op_id = self.start_operation(name, category, run_id, input_params)
        result = {"op_id": op_id, "api_calls": 0, "db_writes": 0, "items": 0}
        try:
            yield result
            self.end_operation(op_id, status="ok",
                                api_calls=result.get("api_calls", 0),
                                db_writes=result.get("db_writes", 0),
                                items_processed=result.get("items", 0))
        except Exception as e:
            self.end_operation(op_id, status="error", error_message=str(e)[:300])
            raise

    # --- API Call Log ---

    def log_api_call(self, run_id=None, operation_id=None, provider="",
                      endpoint="", duration_ms=0, http_status=200,
                      response_bytes=0, was_cached=False,
                      was_rate_limited=False, wait_time_ms=0, error=None):
        """Log a single external API call."""
        with _log_lock:
            self._con.execute(
                "INSERT INTO api_call_log "
                "(run_id, operation_id, provider, endpoint, started_utc, "
                "finished_utc, duration_ms, http_status, response_bytes, "
                "was_cached, was_rate_limited, wait_time_ms, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (run_id, operation_id, provider, endpoint,
                 _utc_now(), _utc_now(), round(duration_ms, 1),
                 http_status, response_bytes,
                 1 if was_cached else 0,
                 1 if was_rate_limited else 0,
                 round(wait_time_ms, 1),
                 error))
            self._con.commit()

    # --- Layer Timing ---

    def start_layer(self, run_id, layer_name, scope_key, symbols_input=0):
        """Start timing a layer execution."""
        with _log_lock:
            cur = self._con.execute(
                "INSERT INTO layer_timing "
                "(run_id, profile_id, layer_name, scope_key, started_utc, symbols_input) "
                "VALUES (?, ?, ?, ?, ?, ?);",
                (run_id, self._current_profile_id, layer_name, scope_key,
                 _utc_now(), symbols_input))
            self._con.commit()
            return cur.lastrowid

    def end_layer(self, layer_id, status="ok", symbols_scored=0,
                   api_calls=0, cache_hits=0, rate_limit_waits=0,
                   total_wait_ms=0, error=None):
        """Finish timing a layer."""
        with _log_lock:
            row = self._con.execute(
                "SELECT started_utc FROM layer_timing WHERE id=?;",
                (layer_id,)).fetchone()
            dur_ms = _duration_since(row[0]) * 1000 if row else 0
            self._con.execute(
                "UPDATE layer_timing SET finished_utc=?, duration_ms=?, "
                "status=?, symbols_scored=?, api_calls=?, cache_hits=?, "
                "rate_limit_waits=?, total_wait_ms=?, error=? WHERE id=?;",
                (_utc_now(), round(dur_ms, 1), status, symbols_scored,
                 api_calls, cache_hits, rate_limit_waits,
                 round(total_wait_ms, 1), error, layer_id))
            self._con.commit()

    # --- Query functions for dashboard ---

    def get_run_profiles(self, limit=30):
        """Get recent run profiles with timing."""
        rows = self._con.execute(
            "SELECT id, run_id, app_version, scope, started_utc, finished_utc, "
            "duration_s, status, total_api_calls, total_db_writes, "
            "candidates_n, layers_run, errors_count "
            "FROM run_profile ORDER BY id DESC LIMIT ?;", (limit,)).fetchall()
        return [{"profile_id": r[0], "run_id": r[1], "version": r[2],
                 "scope": r[3], "started": r[4], "finished": r[5],
                 "duration_s": r[6], "status": r[7], "api_calls": r[8],
                 "db_writes": r[9], "candidates": r[10], "layers": r[11],
                 "errors": r[12]} for r in rows]

    def get_operations_for_run(self, run_id):
        """Get all operations for a specific run, ordered by start time."""
        rows = self._con.execute(
            "SELECT id, operation, category, started_utc, finished_utc, "
            "duration_ms, status, api_calls, db_writes, items_processed, "
            "error_message FROM operation_log WHERE run_id=? "
            "ORDER BY id ASC;", (run_id,)).fetchall()
        return [{"op_id": r[0], "operation": r[1], "category": r[2],
                 "started": r[3], "finished": r[4], "duration_ms": r[5],
                 "status": r[6], "api_calls": r[7], "db_writes": r[8],
                 "items": r[9], "error": r[10]} for r in rows]

    def get_layer_timings_for_run(self, run_id):
        """Get layer timing breakdown for a run."""
        rows = self._con.execute(
            "SELECT layer_name, scope_key, started_utc, finished_utc, "
            "duration_ms, status, symbols_input, symbols_scored, "
            "api_calls, cache_hits, rate_limit_waits, total_wait_ms, error "
            "FROM layer_timing WHERE run_id=? ORDER BY id ASC;",
            (run_id,)).fetchall()
        return [{"layer": r[0], "scope_key": r[1], "started": r[2],
                 "finished": r[3], "duration_ms": r[4], "status": r[5],
                 "symbols_in": r[6], "symbols_scored": r[7],
                 "api_calls": r[8], "cache_hits": r[9],
                 "rate_waits": r[10], "wait_ms": r[11], "error": r[12]}
                for r in rows]

    def get_api_calls_for_run(self, run_id, limit=200):
        """Get all API calls for a run."""
        rows = self._con.execute(
            "SELECT provider, endpoint, duration_ms, http_status, "
            "response_bytes, was_cached, was_rate_limited, wait_time_ms, error "
            "FROM api_call_log WHERE run_id=? ORDER BY id ASC LIMIT ?;",
            (run_id, limit)).fetchall()
        return [{"provider": r[0], "endpoint": r[1], "duration_ms": r[2],
                 "http_status": r[3], "bytes": r[4], "cached": bool(r[5]),
                 "rate_limited": bool(r[6]), "wait_ms": r[7], "error": r[8]}
                for r in rows]

    def get_performance_overview(self):
        """Get aggregate performance metrics."""
        result = {}
        # Average run duration
        row = self._con.execute(
            "SELECT AVG(duration_s), MIN(duration_s), MAX(duration_s), COUNT(*) "
            "FROM run_profile WHERE status='completed';").fetchone()
        result["runs"] = {
            "avg_duration_s": round(row[0] or 0, 1),
            "min_duration_s": round(row[1] or 0, 1),
            "max_duration_s": round(row[2] or 0, 1),
            "total_completed": row[3] or 0,
        }
        # Slowest/fastest layers
        layers = self._con.execute(
            "SELECT layer_name, AVG(duration_ms), COUNT(*) "
            "FROM layer_timing WHERE status IN ('ok', 'degraded') "
            "GROUP BY layer_name ORDER BY AVG(duration_ms) DESC;").fetchall()
        result["layers"] = [{"layer": r[0], "avg_ms": round(r[1] or 0, 0),
                              "count": r[2]} for r in layers]
        # API provider stats
        providers = self._con.execute(
            "SELECT provider, COUNT(*), AVG(duration_ms), "
            "SUM(was_cached), SUM(was_rate_limited), SUM(wait_time_ms) "
            "FROM api_call_log GROUP BY provider ORDER BY COUNT(*) DESC;").fetchall()
        result["providers"] = [{"provider": r[0], "calls": r[1],
                                 "avg_ms": round(r[2] or 0, 0),
                                 "cache_hits": r[3] or 0,
                                 "rate_limits": r[4] or 0,
                                 "total_wait_ms": round(r[5] or 0, 0)}
                                for r in providers]
        # Error rate
        total_ops = self._con.execute(
            "SELECT COUNT(*), SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) "
            "FROM operation_log;").fetchone()
        result["error_rate"] = round(
            (total_ops[1] or 0) / max(1, total_ops[0]) * 100, 1)
        result["total_operations"] = total_ops[0] or 0
        return result

    def compute_daily_summary(self):
        """Compute and store daily performance summary."""
        today = time.strftime("%Y-%m-%d", time.gmtime())
        existing = self._con.execute(
            "SELECT id FROM performance_summary WHERE date_utc=?;",
            (today,)).fetchone()

        runs_today = self._con.execute(
            "SELECT COUNT(*), AVG(duration_s), SUM(total_api_calls), SUM(errors_count) "
            "FROM run_profile WHERE started_utc LIKE ?;",
            (today + "%",)).fetchone()
        rate_limits = self._con.execute(
            "SELECT SUM(was_rate_limited) FROM api_call_log "
            "WHERE started_utc LIKE ?;", (today + "%",)).fetchone()
        cache_stats = self._con.execute(
            "SELECT COUNT(*), SUM(was_cached) FROM api_call_log "
            "WHERE started_utc LIKE ?;", (today + "%",)).fetchone()
        slowest = self._con.execute(
            "SELECT layer_name, MAX(duration_ms) FROM layer_timing "
            "WHERE started_utc LIKE ? GROUP BY layer_name "
            "ORDER BY MAX(duration_ms) DESC LIMIT 1;",
            (today + "%",)).fetchone()
        fastest = self._con.execute(
            "SELECT layer_name, MIN(duration_ms) FROM layer_timing "
            "WHERE started_utc LIKE ? AND duration_ms > 0 "
            "GROUP BY layer_name ORDER BY MIN(duration_ms) ASC LIMIT 1;",
            (today + "%",)).fetchone()

        cache_rate = round((cache_stats[1] or 0) / max(1, cache_stats[0]) * 100, 1)

        if existing:
            self._con.execute(
                "UPDATE performance_summary SET total_runs=?, avg_run_duration_s=?, "
                "total_api_calls=?, total_rate_limit_waits=?, total_errors=?, "
                "slowest_layer=?, slowest_layer_ms=?, fastest_layer=?, "
                "fastest_layer_ms=?, cache_hit_rate=?, computed_utc=? "
                "WHERE date_utc=?;",
                (runs_today[0], runs_today[1], runs_today[2], rate_limits[0] or 0,
                 runs_today[3] or 0,
                 slowest[0] if slowest else None, slowest[1] if slowest else None,
                 fastest[0] if fastest else None, fastest[1] if fastest else None,
                 cache_rate, _utc_now(), today))
        else:
            self._con.execute(
                "INSERT INTO performance_summary "
                "(date_utc, total_runs, avg_run_duration_s, total_api_calls, "
                "total_rate_limit_waits, total_errors, slowest_layer, "
                "slowest_layer_ms, fastest_layer, fastest_layer_ms, "
                "cache_hit_rate, computed_utc) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);",
                (today, runs_today[0], runs_today[1], runs_today[2],
                 rate_limits[0] or 0, runs_today[3] or 0,
                 slowest[0] if slowest else None, slowest[1] if slowest else None,
                 fastest[0] if fastest else None, fastest[1] if fastest else None,
                 cache_rate, _utc_now()))
        self._con.commit()

    def get_daily_summaries(self, limit=30):
        """Get daily performance summaries."""
        rows = self._con.execute(
            "SELECT date_utc, total_runs, avg_run_duration_s, total_api_calls, "
            "total_rate_limit_waits, total_errors, slowest_layer, "
            "slowest_layer_ms, fastest_layer, fastest_layer_ms, cache_hit_rate "
            "FROM performance_summary ORDER BY date_utc DESC LIMIT ?;",
            (limit,)).fetchall()
        return [{"date": r[0], "runs": r[1], "avg_duration_s": r[2],
                 "api_calls": r[3], "rate_limits": r[4], "errors": r[5],
                 "slowest": r[6], "slowest_ms": r[7],
                 "fastest": r[8], "fastest_ms": r[9],
                 "cache_hit_rate": r[10]} for r in rows]

    def db_size_mb(self):
        """Get analytics DB file size."""
        path = get_analytics_db_path(self._root)
        if os.path.isfile(path):
            return round(os.path.getsize(path) / (1024 * 1024), 2)
        return 0


def _utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _duration_since(start_utc_str):
    """Compute seconds since a UTC timestamp string."""
    try:
        start = time.mktime(time.strptime(start_utc_str[:19], "%Y-%m-%dT%H:%M:%S"))
        return max(0, time.time() - start)
    except Exception:
        return 0
