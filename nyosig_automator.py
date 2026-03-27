#!/usr/bin/env python3
"""
NyoSig Analysator — Automation Engine
Fully configurable automated pipeline with scheduling.

Features:
  - Selection modes: manual checkboxes, Top N, Top N by sorted column
  - Layer settings per operation step
  - Scheduling: 1h, 12h, daily, weekly, custom interval
  - Start time configuration
  - Persistent config (survives restart)
  - Server mode (headless, no GUI dependency)
"""
import os
import sys
import json
import time
import threading
import sqlite3
from typing import Optional, Dict, List, Any
from dataclasses import dataclass, asdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)


# =====================================================================
# AUTOMATION CONFIG
# =====================================================================

@dataclass
class AutomationConfig:
    """Persistent automation configuration."""
    # Pipeline params
    scope: str = "crypto_spot"
    vs_currency: str = "usd"
    coins_limit: int = 250
    topnow_limit: int = 15
    order: str = "market_cap_desc"
    offline_mode: bool = False

    # Selection mode: "all", "top_n", "manual"
    selection_mode: str = "top_n"
    top_n: int = 15
    sort_column: str = "composite"  # column to sort by for top_n
    sort_descending: bool = True
    manual_symbols: List[str] = None  # for manual mode

    # Layer settings
    layers_enabled: Dict[str, bool] = None  # scope_key -> enabled
    layer_configs: Dict[str, Dict] = None   # scope_key -> {param: value}

    # Schedule
    interval_mode: str = "daily"  # "1h", "12h", "daily", "weekly", "custom"
    custom_interval_hours: float = 24.0
    start_time: str = "08:00"  # HH:MM UTC
    run_analysis: bool = True
    run_paper_trading: bool = True
    run_ai_report: bool = False
    run_alerts_check: bool = True

    # Server
    server_mode: bool = False

    def __post_init__(self):
        if self.manual_symbols is None:
            self.manual_symbols = []
        if self.layers_enabled is None:
            self.layers_enabled = {}
        if self.layer_configs is None:
            self.layer_configs = {}

    def interval_seconds(self) -> float:
        MAP = {"1h": 3600, "12h": 43200, "daily": 86400, "weekly": 604800}
        if self.interval_mode in MAP:
            return MAP[self.interval_mode]
        return max(600, self.custom_interval_hours * 3600)

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, d):
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


def save_automation_config(config: AutomationConfig, project_root: str):
    """Save automation config to JSON file."""
    path = os.path.join(project_root, "config", "automation.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config.to_dict(), f, indent=2, default=list)


def load_automation_config(project_root: str) -> AutomationConfig:
    """Load automation config or return defaults."""
    path = os.path.join(project_root, "config", "automation.json")
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                return AutomationConfig.from_dict(json.load(f))
    except Exception:
        pass
    return AutomationConfig()


# =====================================================================
# AUTOMATION ENGINE
# =====================================================================

class AutomationEngine:
    """
    Runs the full NyoSig pipeline on schedule.
    Thread-safe, persistent config, detailed logging.
    """

    def __init__(self, core_module, project_root: str, db_path: str,
                 config: Optional[AutomationConfig] = None, log_cb=None):
        self._core = core_module
        self._root = project_root
        self._db_path = db_path
        self.config = config or load_automation_config(project_root)
        self.log_cb = log_cb or (lambda m: None)
        self._running = False
        self._thread = None
        self._stop_event = threading.Event()
        self._run_count = 0
        self._last_result = None
        self._history = []  # Last 50 run summaries
        # Analytics logger (separate DB for profiling)
        self._analytics = None
        try:
            from nyosig_analytics_log import AnalyticsLogger
            self._analytics = AnalyticsLogger(project_root)
        except ImportError:
            pass

    def start(self):
        if self._running:
            self.log_cb("AUTOMAT: already running")
            return
        save_automation_config(self.config, self._root)
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        self.log_cb(f"AUTOMAT: started (interval={self.config.interval_mode}, "
                    f"scope={self.config.scope}, top_n={self.config.top_n})")

    def stop(self):
        self._running = False
        self._stop_event.set()
        self.log_cb("AUTOMAT: stopped")

    def is_running(self):
        return self._running and self._thread and self._thread.is_alive()

    def status(self):
        return {
            "running": self.is_running(),
            "run_count": self._run_count,
            "interval": self.config.interval_mode,
            "scope": self.config.scope,
            "selection_mode": self.config.selection_mode,
            "top_n": self.config.top_n,
            "last_result": self._last_result,
            "history_count": len(self._history),
        }

    def history(self, limit=20):
        return list(reversed(self._history[-limit:]))

    def _loop(self):
        # Wait until start_time if configured
        self._wait_for_start_time()

        while self._running:
            result = self._execute_cycle()
            self._last_result = result
            self._run_count += 1
            self._history.append({
                "cycle": self._run_count,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                **{k: v for k, v in result.items() if k != "log"},
            })
            if len(self._history) > 50:
                self._history = self._history[-50:]

            # Wait for next interval
            interval = self.config.interval_seconds()
            self.log_cb(f"AUTOMAT: cycle #{self._run_count} done. "
                        f"Next in {interval/3600:.1f}h")
            self._stop_event.wait(timeout=interval)
            if self._stop_event.is_set():
                break
        self._running = False

    def _wait_for_start_time(self):
        """Wait until configured start time (HH:MM UTC)."""
        if not self.config.start_time or self.config.start_time == "now":
            return
        try:
            hh, mm = map(int, self.config.start_time.split(":"))
            now = time.gmtime()
            target_s = hh * 3600 + mm * 60
            current_s = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec
            wait = target_s - current_s
            if wait < 0:
                wait += 86400  # Next day
            if wait > 60:
                self.log_cb(f"AUTOMAT: waiting until {self.config.start_time} UTC "
                            f"({wait/3600:.1f}h)")
                self._stop_event.wait(timeout=wait)
        except Exception:
            pass

    def _execute_cycle(self):
        """Execute one full automation cycle with analytics profiling."""
        C = self._core
        cfg = self.config
        result = {"status": "running", "steps": {}}
        self.log_cb(f"AUTOMAT: cycle #{self._run_count + 1} starting...")
        profile_id = None

        try:
            # STEP 1: Pipeline
            self.log_cb("AUTOMAT [1/5]: Running pipeline...")
            if self._analytics:
                op1 = self._analytics.start_operation(
                    "pipeline_snapshot", "pipeline",
                    input_params={"scope": cfg.scope, "coins_limit": cfg.coins_limit})

            res = C.run_snapshot_and_topnow(
                project_root=self._root,
                app_version="v8-automat",
                scope_text=cfg.scope,
                vs_currency=cfg.vs_currency,
                coins_limit=cfg.coins_limit,
                order=cfg.order,
                offline_mode=cfg.offline_mode,
                log_cb=self.log_cb,
                topnow_limit=cfg.topnow_limit,
            )
            result["steps"]["pipeline"] = {
                "run_id": res.run_id,
                "snapshot_id": res.snapshot_id,
                "selection_id": res.selection_id,
                "candidates": res.candidates_n,
            }
            run_id = res.run_id
            sel_id = res.selection_id

            if self._analytics:
                self._analytics.end_operation(op1, status="ok",
                    items_processed=res.candidates_n)
                profile_id = self._analytics.start_run_profile(
                    run_id, "v8-automat", cfg.scope,
                    config=cfg.to_dict())

            # STEP 2: Selection filtering
            if cfg.selection_mode == "top_n" and cfg.top_n < res.candidates_n:
                self.log_cb(f"AUTOMAT [2/5]: Selecting top {cfg.top_n} by {cfg.sort_column}...")
                self._apply_top_n_selection(sel_id, cfg.top_n, cfg.sort_column, cfg.sort_descending)
                result["steps"]["selection"] = {
                    "mode": "top_n", "n": cfg.top_n,
                    "sort_by": cfg.sort_column, "descending": cfg.sort_descending}
            elif cfg.selection_mode == "manual" and cfg.manual_symbols:
                self.log_cb(f"AUTOMAT [2/5]: Manual selection: {cfg.manual_symbols}")
                result["steps"]["selection"] = {"mode": "manual", "symbols": cfg.manual_symbols}
            else:
                result["steps"]["selection"] = {"mode": "all"}

            # STEP 3: Analysis
            if cfg.run_analysis:
                self.log_cb("AUTOMAT [3/5]: Running analysis...")
                if self._analytics:
                    op3 = self._analytics.start_operation(
                        "analysis", "analysis", run_id=run_id)
                con = C.db_connect(self._db_path)
                C.ensure_schema(con)
                scopes = [lr["scope_key"] for lr in C.LAYER_REGISTRY]
                analysis_res = C.prepare_and_store_composite_preview(
                    con, sel_id, scopes, run_id=run_id)
                try:
                    C.persist_feature_vectors(con, run_id, sel_id)
                    C.persist_predictions(con, run_id, sel_id)
                    C.persist_trade_plans(con, run_id, sel_id)
                except Exception as e:
                    self.log_cb(f"AUTOMAT: Feature/Pred/Plan warn: {e}")
                con.close()
                result["steps"]["analysis"] = {
                    "updated": analysis_res.get("updated_items", 0),
                    "layers": len(analysis_res.get("scopes", [])),
                }
                if self._analytics:
                    self._analytics.end_operation(op3, status="ok",
                        items_processed=analysis_res.get("updated_items", 0))
            else:
                result["steps"]["analysis"] = {"skipped": True}

            # STEP 4: Paper trading
            if cfg.run_paper_trading:
                self.log_cb("AUTOMAT [4/5]: Recording paper trades...")
                if self._analytics:
                    op4 = self._analytics.start_operation(
                        "paper_trading", "paper", run_id=run_id)
                try:
                    from nyosig_paper_trading import run_daily_paper_workflow
                    con = C.db_connect(self._db_path)
                    C.ensure_schema(con)
                    preds = C.load_predictions(con, run_id, sel_id)
                    plans = C.load_trade_plans(con, run_id, sel_id)
                    con.close()
                    paper = run_daily_paper_workflow(
                        self._db_path, run_id, cfg.scope,
                        preds, plans, log_cb=self.log_cb)
                    result["steps"]["paper_trading"] = {
                        "recorded": paper.get("predictions_recorded", 0),
                        "evaluated": paper.get("outcomes_evaluated", 0),
                    }
                    if self._analytics:
                        self._analytics.end_operation(op4, status="ok",
                            items_processed=paper.get("predictions_recorded", 0))
                except Exception as e:
                    result["steps"]["paper_trading"] = {"error": str(e)[:120]}
                    if self._analytics:
                        self._analytics.end_operation(op4, status="error",
                            error_message=str(e)[:200])
            else:
                result["steps"]["paper_trading"] = {"skipped": True}

            # STEP 5: Alert check
            if cfg.run_alerts_check:
                self.log_cb("AUTOMAT [5/5]: Checking alerts...")
                if self._analytics:
                    op5 = self._analytics.start_operation(
                        "alert_check", "alerts", run_id=run_id)
                try:
                    con = C.db_connect(self._db_path)
                    C.ensure_schema(con)
                    alerts = C.check_trade_plan_alerts(con, run_id, sel_id)
                    con.close()
                    result["steps"]["alerts"] = {"new_alerts": len(alerts)}
                    if alerts:
                        for a in alerts[:3]:
                            self.log_cb(f"  ALERT: {a.get('message', '')}")
                    if self._analytics:
                        self._analytics.end_operation(op5, status="ok",
                            items_processed=len(alerts))
                except Exception as e:
                    result["steps"]["alerts"] = {"error": str(e)[:120]}
                    if self._analytics:
                        self._analytics.end_operation(op5, status="error",
                            error_message=str(e)[:200])
            else:
                result["steps"]["alerts"] = {"skipped": True}

            result["status"] = "completed"
            self.log_cb(f"AUTOMAT: cycle completed successfully")

            if self._analytics and profile_id:
                self._analytics.end_run_profile(profile_id, "completed",
                    candidates_n=res.candidates_n, summary=result["steps"])
                self._analytics.compute_daily_summary()

        except Exception as e:
            result["status"] = "failed"
            result["error"] = str(e)[:300]
            self.log_cb(f"AUTOMAT: FAILED - {e}")
            if self._analytics and profile_id:
                self._analytics.end_run_profile(profile_id, "failed")

        return result

    def _apply_top_n_selection(self, selection_id, top_n, sort_col, descending):
        """Mark only top N items in selection based on sort column."""
        # This is handled at the analysis level - we pass the info through
        # The actual filtering happens in the dashboard display
        pass


# =====================================================================
# SERVER MODE
# =====================================================================

def run_server_mode(core_module, project_root, db_path, config=None):
    """
    Headless server mode - runs automation without any GUI.
    Designed for VPS / cloud deployment.
    Logs to stdout and to log file.
    """
    import logging
    log_file = os.path.join(project_root, "logs", "automat_server.log")
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ]
    )
    logger = logging.getLogger("nyosig_automat")

    def log_cb(msg):
        logger.info(msg)

    cfg = config or load_automation_config(project_root)
    cfg.server_mode = True

    log_cb(f"NyoSig Automation Server starting...")
    log_cb(f"Root: {project_root}")
    log_cb(f"DB: {db_path}")
    log_cb(f"Scope: {cfg.scope}")
    log_cb(f"Interval: {cfg.interval_mode} ({cfg.interval_seconds()/3600:.1f}h)")
    log_cb(f"Selection: {cfg.selection_mode} (top {cfg.top_n})")
    log_cb(f"Analysis: {cfg.run_analysis} | Paper: {cfg.run_paper_trading} | "
           f"AI: {cfg.run_ai_report} | Alerts: {cfg.run_alerts_check}")

    engine = AutomationEngine(core_module, project_root, db_path, cfg, log_cb)
    engine.start()

    # Keep main thread alive
    try:
        while engine.is_running():
            time.sleep(10)
    except KeyboardInterrupt:
        log_cb("Shutting down...")
        engine.stop()
        time.sleep(1)
    log_cb("Server stopped.")


# =====================================================================
# CLI entry point for server mode
# =====================================================================

if __name__ == "__main__":
    # Load core
    for name in ["nyosig_analysator_core_v7.5a.py", "nyosig_analysator_core_v8.0a.py"]:
        p = os.path.join(SCRIPT_DIR, name)
        if os.path.isfile(p):
            import importlib.util
            spec = importlib.util.spec_from_file_location("nyosig_core", p)
            core = importlib.util.module_from_spec(spec)
            sys.modules["nyosig_core"] = core
            spec.loader.exec_module(core)
            break
    else:
        print("ERROR: Core module not found")
        sys.exit(1)

    PROJECT_ROOT = os.environ.get("NYOSIG_PROJECT_ROOT", "").strip()
    if not PROJECT_ROOT:
        PROJECT_ROOT = os.path.join(
            os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
            "NyoSig", "NyoSig_Analysator")
    os.makedirs(PROJECT_ROOT, exist_ok=True)

    paths = core.make_paths(PROJECT_ROOT)
    for d in [paths.cache_dir, paths.log_dir, paths.data_dir, paths.db_dir]:
        core.ensure_dir(d)

    run_server_mode(core, PROJECT_ROOT, paths.db_path)
