#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NyoSig Analysator core v7.5c compatibility layer.

This file keeps the full v7.5a analytical engine in place and adds the
v7.5c corrective behaviour without replacing the full core:
1. default project root uses /storage/emulated/0/Programy/analyza_trhu
2. HTTP calls use a clear NyoSig User-Agent
3. run_pipeline compatibility entry point is available
4. JSON mirrors are written to data/run_history.json and data/run_profiles.json
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Tuple

_DIR = os.path.dirname(os.path.abspath(__file__))
_CORE_PATH = os.path.join(_DIR, "nyosig_analysator_core_v7.5a.py")
if not os.path.isfile(_CORE_PATH):
    raise FileNotFoundError("Missing base core: " + _CORE_PATH)

_spec = importlib.util.spec_from_file_location("nyosig_core_v75a_base", _CORE_PATH)
_core = importlib.util.module_from_spec(_spec)
sys.modules["nyosig_core_v75a_base"] = _core
_spec.loader.exec_module(_core)

# Re-export base core symbols first.
for _k, _v in _core.__dict__.items():
    if _k.startswith("__") and _k not in ("__doc__",):
        continue
    globals()[_k] = _v

CORE_VERSION = "v7.5c"
APP_VERSION = "v7.5c"
DEFAULT_PROJECT_ROOT = "/storage/emulated/0/Programy/analyza_trhu"


def get_project_root(default: str = DEFAULT_PROJECT_ROOT) -> str:
    return os.environ.get("NYOSIG_PROJECT_ROOT", default).strip() or default


# Patch base core project-root helpers so functions executed inside the base module
# also use the corrected default path.
_core.DEFAULT_PROJECT_ROOT = DEFAULT_PROJECT_ROOT
_core.get_project_root = get_project_root


@dataclass
class RunSummary:
    run_id: Any
    started: str
    duration_s: float
    status: str
    api_calls: int
    candidates: int
    errors: int
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "started": self.started,
            "duration_s": self.duration_s,
            "status": self.status,
            "api_calls": self.api_calls,
            "candidates": self.candidates,
            "errors": self.errors,
            "message": self.message,
        }


def _v75c_load_json_list(path: str) -> list:
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _v75c_save_json_list(path: str, data: list) -> None:
    ensure_dir(os.path.dirname(path))
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=True)
    os.replace(tmp_path, path)


def _v75c_count_api_records(con, run_id: int) -> int:
    try:
        row = con.execute("SELECT COUNT(*) FROM raw_snapshots WHERE run_id=?;", (run_id,)).fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _v75c_record_run_profile(paths, run_id: Any, app_version: str, scope: str,
                             status: str, started_utc: str, duration_s: float,
                             candidates_n: int, api_calls: int, errors: int,
                             message: str = "", snapshot_id: str = "",
                             selection_id: Any = None) -> RunSummary:
    summary = RunSummary(
        run_id=run_id,
        started=started_utc,
        duration_s=round(float(duration_s or 0), 2),
        status=status,
        api_calls=int(api_calls or 0),
        candidates=int(candidates_n or 0),
        errors=int(errors or 0),
        message=str(message or "")[:1000],
    )

    history_path = os.path.join(paths.data_dir, "run_history.json")
    profiles_path = os.path.join(paths.data_dir, "run_profiles.json")

    history = _v75c_load_json_list(history_path)
    history.append({
        "run_id": run_id,
        "created": started_utc,
        "finished": utc_now_iso(),
        "duration_s": summary.duration_s,
        "version": app_version,
        "scope": scope,
        "status": status,
        "snapshot_id": snapshot_id,
        "selection_id": selection_id,
        "candidates": int(candidates_n or 0),
        "message": str(message or "")[:500],
    })
    _v75c_save_json_list(history_path, history[-500:])

    profiles = _v75c_load_json_list(profiles_path)
    profile = summary.to_dict()
    profile.update({
        "version": app_version,
        "scope": scope,
        "snapshot_id": snapshot_id,
        "selection_id": selection_id,
    })
    profiles.append(profile)
    _v75c_save_json_list(profiles_path, profiles[-500:])

    return summary


def http_get_json(url: str, headers: Dict[str, str], timeout_s: int) -> Any:
    safe_headers = {"User-Agent": "NyoSig-Analysator/7.5c"}
    if headers:
        safe_headers.update(headers)
    return _core.http_get_json(url, safe_headers, timeout_s)


def _http_get_json(url, timeout=10, headers=None):
    safe_headers = {"User-Agent": "NyoSig-Analysator/7.5c"}
    if headers:
        safe_headers.update(headers)
    return _core._http_get_json(url, timeout=timeout, headers=safe_headers)


_core.http_get_json = http_get_json
_core._http_get_json = _http_get_json


_base_run_snapshot_and_topnow = _core.run_snapshot_and_topnow


def run_snapshot_and_topnow(project_root: str, app_version: str, scope_text: str,
                            vs_currency: str, coins_limit: int, order: str,
                            offline_mode: bool, log_cb, topnow_limit: int = 100,
                            timeframe: str = "spot", parent_snapshot_key: str = ""):
    started = utc_now_iso()
    t0 = time.time()
    paths = make_paths(project_root)
    try:
        res = _base_run_snapshot_and_topnow(
            project_root=project_root,
            app_version=app_version,
            scope_text=scope_text,
            vs_currency=vs_currency,
            coins_limit=coins_limit,
            order=order,
            offline_mode=offline_mode,
            log_cb=log_cb,
            topnow_limit=topnow_limit,
            timeframe=timeframe,
            parent_snapshot_key=parent_snapshot_key,
        )
        try:
            con = db_connect(paths.db_path)
            try:
                ensure_schema(con)
                api_calls = _v75c_count_api_records(con, res.run_id)
            finally:
                con.close()
            _v75c_record_run_profile(
                paths=paths,
                run_id=res.run_id,
                app_version=app_version,
                scope=scope_text,
                status="completed",
                started_utc=started,
                duration_s=time.time() - t0,
                candidates_n=res.candidates_n,
                api_calls=api_calls,
                errors=0,
                snapshot_id=res.snapshot_id,
                selection_id=res.selection_id,
            )
        except Exception as exc:
            if log_cb:
                log_cb("run profile write WARN: " + str(exc)[:120])
        return res
    except Exception as exc:
        try:
            _v75c_record_run_profile(
                paths=paths,
                run_id="failed_" + utc_stamp_compact(),
                app_version=app_version,
                scope=scope_text,
                status="failed",
                started_utc=started,
                duration_s=time.time() - t0,
                candidates_n=0,
                api_calls=0,
                errors=1,
                message=str(exc)[:1000],
            )
        except Exception:
            pass
        raise


def run_pipeline(scope: str = "crypto_spot", vs_currency: str = "usd",
                 limit: int = 250, top_n: int = 15,
                 history_dir: str | None = None) -> Tuple[Any, RunSummary]:
    project_root = history_dir or get_project_root()
    started = utc_now_iso()
    t0 = time.time()
    try:
        res = run_snapshot_and_topnow(
            project_root=project_root,
            app_version="v7.5c",
            scope_text=scope,
            vs_currency=vs_currency,
            coins_limit=int(limit),
            order="market_cap_desc",
            offline_mode=False,
            log_cb=lambda _m: None,
            topnow_limit=int(top_n),
        )
        paths = make_paths(project_root)
        con = db_connect(paths.db_path)
        try:
            ensure_schema(con)
            api_calls = _v75c_count_api_records(con, res.run_id)
        finally:
            con.close()
        summary = _v75c_record_run_profile(
            paths=paths,
            run_id=res.run_id,
            app_version="v7.5c",
            scope=scope,
            status="completed",
            started_utc=started,
            duration_s=time.time() - t0,
            candidates_n=res.candidates_n,
            api_calls=api_calls,
            errors=0,
            snapshot_id=res.snapshot_id,
            selection_id=res.selection_id,
        )
        return res.run_id, summary
    except Exception as exc:
        paths = make_paths(project_root)
        summary = _v75c_record_run_profile(
            paths=paths,
            run_id="failed_" + utc_stamp_compact(),
            app_version="v7.5c",
            scope=scope,
            status="failed",
            started_utc=started,
            duration_s=time.time() - t0,
            candidates_n=0,
            api_calls=0,
            errors=1,
            message=str(exc)[:1000],
        )
        return summary.run_id, summary


# Ensure callers using the base module reference see the patched functions too.
_core.run_snapshot_and_topnow = run_snapshot_and_topnow
_core.run_pipeline = run_pipeline
_core.RunSummary = RunSummary

# Re-export patched symbols.
globals().update({
    "CORE_VERSION": CORE_VERSION,
    "APP_VERSION": APP_VERSION,
    "DEFAULT_PROJECT_ROOT": DEFAULT_PROJECT_ROOT,
    "get_project_root": get_project_root,
    "RunSummary": RunSummary,
    "http_get_json": http_get_json,
    "_http_get_json": _http_get_json,
    "run_snapshot_and_topnow": run_snapshot_and_topnow,
    "run_pipeline": run_pipeline,
})
