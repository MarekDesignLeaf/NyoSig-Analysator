#!/usr/bin/env python3
"""
NyoSig Analysator — Web API (FastAPI)
Wraps core v7.5c as REST endpoints for Streamlit dashboard.

Install: pip install fastapi uvicorn
Run:     python nyosig_api.py
         or: uvicorn nyosig_api:app --host 0.0.0.0 --port 8000 --reload
"""
import os, sys, json, threading, sqlite3, time
from contextlib import contextmanager

# --- Load core ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

# Find core module
_core = None
for name in ["nyosig_analysator_core_v7.5c.py", "nyosig_analysator_core_v7.5a.py", "nyosig_analysator_core_v8.0a.py"]:
    p = os.path.join(SCRIPT_DIR, name)
    if os.path.isfile(p):
        import importlib.util
        spec = importlib.util.spec_from_file_location("nyosig_core", p)
        _core = importlib.util.module_from_spec(spec)
        sys.modules["nyosig_core"] = _core
        spec.loader.exec_module(_core)
        break
if _core is None:
    raise FileNotFoundError("Core module not found in " + SCRIPT_DIR)

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

# --- Setup ---
PROJECT_ROOT = os.environ.get("NYOSIG_PROJECT_ROOT", "").strip()
if not PROJECT_ROOT:
    PROJECT_ROOT = os.path.join(
        os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
        "NyoSig", "NyoSig_Analysator")
os.makedirs(PROJECT_ROOT, exist_ok=True)

paths = _core.make_paths(PROJECT_ROOT)
for d in [paths.cache_dir, paths.log_dir, paths.data_dir, paths.db_dir]:
    _core.ensure_dir(d)

APP_VERSION = "v7.6d-web"

# --- FastAPI app ---
app = FastAPI(
    title="NyoSig Analysator API",
    version=APP_VERSION,
    description="Multi-layer market intelligence engine — REST API"
)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

# --- DB helper ---
_DB_SCHEMA_READY = False
_DB_SCHEMA_LOCK = threading.RLock()

def _db_connect_safe(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
    con.execute("PRAGMA busy_timeout=30000;")
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con

_core.db_connect = _db_connect_safe

def _is_db_locked(exc: Exception) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower()

def _ensure_schema_once(con: sqlite3.Connection) -> None:
    global _DB_SCHEMA_READY
    if _DB_SCHEMA_READY:
        return
    with _DB_SCHEMA_LOCK:
        if _DB_SCHEMA_READY:
            return
        last_exc = None
        for attempt in range(6):
            try:
                _core.ensure_schema(con)
                _DB_SCHEMA_READY = True
                return
            except sqlite3.OperationalError as exc:
                if not _is_db_locked(exc):
                    raise
                last_exc = exc
                time.sleep(min(0.5 * (attempt + 1), 3.0))
        raise last_exc or sqlite3.OperationalError("database is locked")

@contextmanager
def get_db(ensure_schema: bool = True):
    con = _core.db_connect(paths.db_path)
    if ensure_schema:
        _ensure_schema_once(con)
    try:
        yield con
    finally:
        con.close()

# --- State ---
_pipeline_state = {"running": False, "status": "idle", "log": [], "result": None}
_log_lock = threading.Lock()

def _log(msg):
    with _log_lock:
        _pipeline_state["log"].append(msg)
        if len(_pipeline_state["log"]) > 500:
            _pipeline_state["log"] = _pipeline_state["log"][-200:]


# =====================================================================
# ENDPOINTS
# =====================================================================

# --- Health ---
@app.get("/")
def root():
    return {"service": "NyoSig Analysator API", "version": APP_VERSION,
            "project_root": PROJECT_ROOT}

@app.get("/health")
def health():
    try:
        with get_db() as con:
            h = _core.system_health_check(con)
        return h
    except sqlite3.OperationalError as exc:
        if _is_db_locked(exc):
            return {
                "status": "busy",
                "version": APP_VERSION,
                "detail": "SQLite database is temporarily locked by a running pipeline. Retry shortly.",
                "project_root": PROJECT_ROOT,
            }
        raise

# --- Scopes ---
@app.get("/scopes")
def scopes():
    result = []
    for key, display_name in _core.list_scopes():
        sd = _core.get_scope(key)
        result.append({"key": key, "name": display_name,
                       "asset_class": sd.asset_class if sd else "unknown",
                       "default_limit": sd.default_limit if sd else 100})
    return result

# --- Pipeline ---
class PipelineRequest(BaseModel):
    scope: str = "crypto_spot"
    vs_currency: str = "usd"
    coins_limit: int = 250
    order: str = "market_cap_desc"
    topnow_limit: int = 15
    offline_mode: bool = False

@app.post("/pipeline/run")
def run_pipeline(req: PipelineRequest, background_tasks: BackgroundTasks):
    if _pipeline_state["running"]:
        raise HTTPException(400, "Pipeline already running")

    def _run():
        _pipeline_state["running"] = True
        _pipeline_state["status"] = "running"
        _pipeline_state["log"] = []
        _pipeline_state["result"] = None
        try:
            res = _core.run_snapshot_and_topnow(
                project_root=PROJECT_ROOT,
                app_version=APP_VERSION,
                scope_text=req.scope,
                vs_currency=req.vs_currency,
                coins_limit=req.coins_limit,
                order=req.order,
                offline_mode=req.offline_mode,
                log_cb=_log,
                topnow_limit=req.topnow_limit,
            )
            _pipeline_state["result"] = {
                "run_id": res.run_id,
                "snapshot_id": res.snapshot_id,
                "selection_id": res.selection_id,
                "candidates_n": res.candidates_n,
            }
            try:
                if _HAS_ANALYTICS and _analytics:
                    profile_id = _analytics.start_run_profile(
                        res.run_id, APP_VERSION, req.scope,
                        config=req.model_dump() if hasattr(req, "model_dump") else req.dict())
                    op_id = _analytics.start_operation(
                        "pipeline_snapshot", "pipeline", run_id=res.run_id,
                        input_params=req.model_dump() if hasattr(req, "model_dump") else req.dict())
                    _analytics.end_operation(
                        op_id, status="ok",
                        output_summary=_pipeline_state["result"],
                        items_processed=res.candidates_n,
                        db_writes=res.candidates_n)
                    _analytics.end_run_profile(
                        profile_id, "completed",
                        candidates_n=res.candidates_n,
                        summary=_pipeline_state["result"])
                    _analytics.compute_daily_summary()
            except Exception as _analytics_exc:
                _log(f"ANALYTICS WARN: {_analytics_exc}")
            _pipeline_state["status"] = "done"
            _log(f"DONE run_id={res.run_id} candidates={res.candidates_n}")
        except Exception as e:
            _pipeline_state["status"] = "failed"
            _pipeline_state["result"] = {"error": str(e)[:500]}
            _log(f"FAILED: {e}")
        finally:
            _pipeline_state["running"] = False

    background_tasks.add_task(_run)
    return {"status": "started"}

@app.get("/pipeline/status")
def pipeline_status():
    return _pipeline_state

# --- Analysis ---
class AnalyseRequest(BaseModel):
    selection_id: int
    run_id: Optional[int] = None


@app.post("/analyse")
def analyse(req: AnalyseRequest, background_tasks: BackgroundTasks):
    def _run():
        _pipeline_state["running"] = True
        _pipeline_state["status"] = "analysing"
        _pipeline_state["result"] = None
        _log(f"ANALYSE START run_id={req.run_id} selection_id={req.selection_id}")
        try:
            with get_db() as con:
                scopes = [lr["scope_key"] for lr in LAYER_REGISTRY]
                _log("ANALYSE scopes=" + ",".join(scopes))

                res = _core.prepare_and_store_composite_preview(
                    con, req.selection_id, scopes, run_id=req.run_id)
                updated = res.get("updated_items", 0) if isinstance(res, dict) else 0
                layers_n = len(res.get("scopes", [])) if isinstance(res, dict) else 0
                _log(f"ANALYSE composite updated={updated} layers={layers_n}")

                if req.run_id:
                    _log("ANALYSE storing feature vectors")
                    _core.persist_feature_vectors(con, req.run_id, req.selection_id)

                    _log("ANALYSE storing predictions")
                    _core.persist_predictions(con, req.run_id, req.selection_id)

                    _log("ANALYSE storing trade plans")
                    _core.persist_trade_plans(con, req.run_id, req.selection_id)

                pred_count = len(_core.load_predictions(con, req.run_id, req.selection_id)) if req.run_id else 0
                feature_count = len(_core.load_feature_vectors_for_view(con, req.run_id, req.selection_id)) if req.run_id else 0
                plan_count = len(_core.load_trade_plans(con, req.run_id, req.selection_id)) if req.run_id else 0

            _pipeline_state["status"] = "done"
            _pipeline_state["result"] = {
                "updated": updated,
                "layers": layers_n,
                "feature_vectors": feature_count,
                "predictions": pred_count,
                "trade_plans": plan_count,
            }
            _log(f"ANALYSE DONE features={feature_count} predictions={pred_count} trade_plans={plan_count}")

        except Exception as e:
            import traceback
            err = traceback.format_exc()
            print("ANALYSE FAILED:", err, flush=True)
            _log("ANALYSE FAILED: " + str(e)[:500])
            _pipeline_state["status"] = "failed"
            _pipeline_state["result"] = {"error": str(e)[:1000]}
        finally:
            _pipeline_state["running"] = False

    background_tasks.add_task(_run)
    return {"status": "started", "run_id": req.run_id, "selection_id": req.selection_id}


# --- Results ---
@app.get("/runs")
def list_runs(limit: int = 20):
    with get_db() as con:
        rows = con.execute(
            "SELECT run_id, created_utc, app_version, scope, status "
            "FROM runs ORDER BY run_id DESC LIMIT ?;", (limit,)).fetchall()
    return [{"run_id": r[0], "created": r[1], "version": r[2],
             "scope": r[3], "status": r[4]} for r in rows]

@app.get("/runs/{run_id}/summary")
def run_summary(run_id: int, selection_id: Optional[int] = None):
    with get_db() as con:
        return _core.run_summary(con, run_id, selection_id)

@app.get("/runs/{run_id}/predictions")
def predictions(run_id: int, selection_id: Optional[int] = None):
    with get_db() as con:
        return _core.load_predictions(con, run_id, selection_id)

@app.get("/runs/{run_id}/trade_plans")
def trade_plans(run_id: int, selection_id: Optional[int] = None):
    with get_db() as con:
        return _core.load_trade_plans(con, run_id, selection_id)

@app.get("/runs/{run_id}/features")
def features(run_id: int, selection_id: Optional[int] = None):
    with get_db() as con:
        return _core.load_feature_vectors_for_view(con, run_id, selection_id)

@app.get("/runs/{run_id}/correlations")
def correlations(run_id: int):
    with get_db() as con:
        return _core.cross_scope_correlation(con, run_id)

@app.get("/runs/{run_id}/backtest")
def backtest(run_id: int, selection_id: Optional[int] = None):
    with get_db() as con:
        return _core.backtest_from_trade_plans(con, run_id, selection_id)

# --- Selection / Candidates ---
@app.get("/selection/{selection_id}")
def selection(selection_id: int):
    with get_db() as con:
        rows = con.execute(
            "SELECT rank_in_selection, unified_symbol, composite_preview "
            "FROM topnow_selection_items WHERE selection_id=? "
            "ORDER BY rank_in_selection;", (selection_id,)).fetchall()
        # Enrich with market data
        sel_row = con.execute(
            "SELECT snapshot_id FROM topnow_selection WHERE selection_id=?;",
            (selection_id,)).fetchone()
        snap_id = sel_row[0] if sel_row else None
        market = {}
        if snap_id:
            for m in con.execute(
                "SELECT unified_symbol, price, mcap, vol24, change_24h_pct, rank "
                "FROM market_snapshots WHERE snapshot_id=? AND timeframe='spot';",
                (snap_id,)).fetchall():
                market[m[0]] = {"price": m[1], "mcap": m[2], "vol24": m[3],
                                "chg24": m[4], "rank": m[5]}
    results = []
    for r in rows:
        m = market.get(r[1], {})
        results.append({
            "rank": r[0], "symbol": r[1], "composite": r[2],
            **m
        })
    return results

# --- Alerts ---
@app.get("/alerts")
def alerts(run_id: Optional[int] = None, limit: int = 50):
    with get_db() as con:
        return _core.load_alerts(con, run_id, limit=limit)

@app.post("/alerts/{run_id}/check")
def check_alerts(run_id: int):
    with get_db() as con:
        new = _core.check_trade_plan_alerts(con, run_id)
    return {"new_alerts": len(new), "alerts": new}

# --- Watchlist ---
@app.get("/watchlist")
def watchlist():
    with get_db() as con:
        return _core.enrich_watchlist_with_plans(con)

@app.get("/watchlist/{symbol}/history")
def signal_history(symbol: str, limit: int = 15):
    with get_db() as con:
        return _core.run_history_compare(con, symbol.upper(), limit)

# --- Portfolio ---
@app.get("/portfolio")
def portfolio():
    with get_db() as con:
        return _core.portfolio_dashboard(con)

@app.get("/portfolio/risk")
def portfolio_risk():
    with get_db() as con:
        return _core.compute_portfolio_risk(con)

# --- Prediction Performance ---
@app.get("/performance")
def performance(limit: int = 100):
    with get_db() as con:
        return _core.load_prediction_performance(con, limit)

@app.post("/performance/evaluate")
def evaluate():
    with get_db() as con:
        return _core.evaluate_prediction_history(con)

# --- Quick Refresh ---
@app.post("/tracked/refresh")
def tracked_refresh():
    with get_db() as con:
        return _core.tracked_only_refresh(con, APP_VERSION)

# --- Layers info ---
LAYER_REGISTRY = [
    {"name": "SpotBasic", "scope_key": "crypto_spot",
     "description": "Spot data: base_score from rank, mcap, volume, 24h change. Source: CoinGecko."},
    {"name": "Derivatives", "scope_key": "crypto_derivatives",
     "description": "Funding rates: overbought/oversold signal. Source: Binance Futures."},
    {"name": "OnChain", "scope_key": "onchain",
     "description": "Hash rate, tx count, active addresses, fees. Source: Blockchain.com + Blockchair."},
    {"name": "Institutional", "scope_key": "institutions",
     "description": "CME futures prices, ETF fund flows. Source: Yahoo Finance."},
    {"name": "Macro", "scope_key": "macro",
     "description": "DXY, VIX, S&P500, US10Y, Fed Funds Rate. Source: Yahoo Finance + FRED."},
    {"name": "Sentiment", "scope_key": "sentiment",
     "description": "Fear & Greed Index. Source: alternative.me."},
    {"name": "Technical", "scope_key": "technical",
     "description": "RSI, MACD, EMA slope, Mean Reversion, Relative Volume. Source: local OHLCV."},
    {"name": "Community", "scope_key": "community",
     "description": "Social engagement, developer score. Source: CoinGecko /coins/{id}."},
    {"name": "OpenInterest", "scope_key": "open_interest",
     "description": "Futures open interest changes. Source: Binance, Bybit."},
    {"name": "Fundamental", "scope_key": "fundamental",
     "description": "GitHub dev activity (stars, forks, commits). Source: GitHub API."},
]

@app.get("/layers")
def layers():
    result = []
    for lr in LAYER_REGISTRY:
        adapter = _core.get_layer_adapter(lr["scope_key"])
        caps = adapter.capabilities() if adapter else None
        result.append({
            "name": lr["name"], "scope_key": lr["scope_key"],
            "description": lr.get("description", ""),
            "source": caps.primary_source if caps else "unknown",
            "fallbacks": caps.fallback_sources if caps else [],
            "configurable": dict(caps.configurable_params) if caps else {},
        })
    return result




# --- AI Commentator ---
try:
    from nyosig_ai_commentator import generate_ai_commentary, generate_multi_ai_commentary
    _HAS_AI = True
except ImportError:
    _HAS_AI = False


@app.get("/ai/report/{run_id}")
def ai_report(run_id: int, selection_id: Optional[int] = None, multi: bool = False):
    """Generate AI market intelligence report from run data.

    Defensive endpoint. Provider/package failures are returned as JSON,
    not as HTTP 500 dashboard crashes.
    """
    try:
        with get_db() as con:
            summary = _core.run_summary(con, run_id, selection_id) or {}
            preds = _core.load_predictions(con, run_id, selection_id) or []
            plans = _core.load_trade_plans(con, run_id, selection_id) or []
            feats = _core.load_feature_vectors_for_view(con, run_id, selection_id) or []
            cors = _core.cross_scope_correlation(con, run_id) or []
            try:
                risk = _core.compute_portfolio_risk(con) or {}
            except Exception as risk_exc:
                print("AI REPORT RISK WARN:", str(risk_exc), flush=True)
                risk = {"warning": str(risk_exc)[:300]}

        if not _HAS_AI:
            try:
                from nyosig_ai_commentator import _generate_fallback_report
                return {
                    "report": _generate_fallback_report(summary, preds, cors),
                    "model": "fallback_rule_based",
                    "error": "AI commentator module not loaded",
                }
            except Exception as fallback_exc:
                return {
                    "report": "AI report could not be generated. Fallback report also failed.",
                    "model": "fallback_error",
                    "error": str(fallback_exc)[:500],
                }

        scope = summary.get("scope", "crypto_spot") if isinstance(summary, dict) else "crypto_spot"

        if multi:
            result = generate_multi_ai_commentary(
                summary, preds, plans, feats, cors, risk, scope)
            if isinstance(result, dict) and "ensemble_report" in result and "report" not in result:
                result["report"] = result.get("ensemble_report", "")
            return result

        return generate_ai_commentary(
            summary, preds, plans, feats, cors, risk, scope)

    except Exception as e:
        import traceback
        err = traceback.format_exc()
        print("AI REPORT FAILED:", err, flush=True)
        try:
            from nyosig_ai_commentator import _generate_fallback_report
            fallback = _generate_fallback_report({}, [], [])
        except Exception:
            fallback = "AI report failed before structured data could be interpreted."
        return {
            "report": fallback,
            "model": "fallback_error",
            "error": str(e)[:1000],
            "trace_tail": err[-2000:],
        }


# --- Paper Trading ---
try:
    from nyosig_paper_trading import (
        run_daily_paper_workflow, evaluate_paper_outcomes,
        _compute_paper_stats, ensure_paper_schema, record_paper_predictions,
        generate_daily_report
    )
    _HAS_PAPER = True
except ImportError:
    _HAS_PAPER = False

@app.post("/paper/run/{run_id}")
def paper_trade_run(run_id: int, selection_id: Optional[int] = None):
    """Full daily paper trading workflow: record + evaluate + report."""
    if not _HAS_PAPER:
        raise HTTPException(500, "Paper trading module not loaded")
    with get_db() as con:
        preds = _core.load_predictions(con, run_id, selection_id)
        plans = _core.load_trade_plans(con, run_id, selection_id)
        summary = _core.run_summary(con, run_id, selection_id)
    ai_text = ""
    if _HAS_AI:
        try:
            with get_db() as con:
                feats = _core.load_feature_vectors_for_view(con, run_id, selection_id)
                cors = _core.cross_scope_correlation(con, run_id)
            from nyosig_ai_commentator import generate_ai_commentary
            ai_result = generate_ai_commentary(
                summary, preds, plans, feats, cors,
                scope=summary.get("scope", "crypto_spot"))
            ai_text = ai_result.get("report", "")
        except Exception:
            pass
    result = run_daily_paper_workflow(
        paths.db_path, run_id, summary.get("scope", "crypto_spot"),
        preds, plans, ai_text)
    return result

@app.get("/paper/stats")
def paper_stats():
    """Get paper trading track record statistics."""
    with get_db() as con:
        ensure_paper_schema(con)
        return _compute_paper_stats(con)

@app.post("/paper/evaluate")
def paper_evaluate():
    """Evaluate all open paper predictions against current prices."""
    with get_db() as con:
        return evaluate_paper_outcomes(con)

@app.get("/paper/reports")
def paper_reports(limit: int = 30):
    """Get daily paper trading reports."""
    with get_db() as con:
        ensure_paper_schema(con)
        rows = con.execute(
            "SELECT date_utc, run_id, scope, report_text, tweet_text, "
            "predictions_n, created_utc FROM paper_daily_reports "
            "ORDER BY id DESC LIMIT ?;", (limit,)).fetchall()
    return [{"date": r[0], "run_id": r[1], "scope": r[2],
             "report": r[3], "tweet": r[4], "predictions": r[5],
             "created": r[6]} for r in rows]

@app.get("/paper/predictions")
def paper_predictions(status: str = "all", limit: int = 50):
    """Get paper trade predictions with their outcomes."""
    with get_db() as con:
        ensure_paper_schema(con)
        where = "1=1" if status == "all" else f"p.status='{status}'"
        rows = con.execute(f"""
            SELECT p.id, p.created_utc, p.symbol, p.signal, p.confidence,
            p.price_at_call, p.direction, p.status, p.hash_sha256,
            GROUP_CONCAT(o.eval_period || ':' || o.outcome || ':' || o.pnl_pct, '; ')
            FROM paper_predictions p
            LEFT JOIN paper_outcomes o ON o.prediction_id = p.id
            WHERE {where}
            GROUP BY p.id
            ORDER BY p.id DESC LIMIT ?;
        """, (limit,)).fetchall()
    return [{"id": r[0], "created": r[1], "symbol": r[2], "signal": r[3],
             "confidence": r[4], "price_at_call": r[5], "direction": r[6],
             "status": r[7], "hash": r[8][:16], "outcomes": r[9]}
            for r in rows]


# --- API Key Management ---
try:
    import pathlib as _pathlib
except ImportError:
    _pathlib = None

_KEYS_FILE = os.path.join(PROJECT_ROOT, "config", "api_keys.json")

_SUPPORTED_PROVIDERS = {
    "anthropic": {"env_var": "ANTHROPIC_API_KEY", "label": "Claude (Anthropic)", "prefix": "sk-ant-"},
    "openai":    {"env_var": "OPENAI_API_KEY",    "label": "GPT (OpenAI)",       "prefix": "sk-"},
    "gemini":    {"env_var": "GEMINI_API_KEY",     "label": "Gemini (Google)",    "prefix": "AI"},
    "grok":      {"env_var": "GROK_API_KEY",       "label": "Grok (xAI)",         "prefix": "xai-"},
    "github":    {"env_var": "NYOSIG_GITHUB_TOKEN","label": "GitHub (dev data)",  "prefix": "ghp_"},
}

def _load_keys() -> dict:
    try:
        if os.path.isfile(_KEYS_FILE):
            with open(_KEYS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_keys(keys: dict):
    os.makedirs(os.path.dirname(_KEYS_FILE), exist_ok=True)
    with open(_KEYS_FILE, "w", encoding="utf-8") as f:
        json.dump(keys, f, indent=2)

def _apply_keys_to_env():
    """Load file fallback keys only when the real environment variable is missing.

    Railway Variables are the primary source of truth.
    config/api_keys.json is only a local fallback and must never override Railway.
    """
    keys = _load_keys()
    for provider, info in _SUPPORTED_PROVIDERS.items():
        env_var = info["env_var"]
        if not os.environ.get(env_var) and provider in keys and keys[provider]:
            os.environ[env_var] = keys[provider]

# Apply saved keys on startup
try:
    _apply_keys_to_env()
except Exception as _ake:
    print(f"WARNING: Failed to load saved API keys: {_ake}")

def _mask_key(key: str) -> str:
    if not key or len(key) < 12:
        return "****"
    return key[:6] + "..." + key[-4:]


def _get_key(provider: str):
    """Return (key, source). Source is railway_env, file_fallback, or missing."""
    provider = (provider or "").lower().strip()
    if provider not in _SUPPORTED_PROVIDERS:
        return "", "missing"
    info = _SUPPORTED_PROVIDERS[provider]
    env_value = os.environ.get(info["env_var"], "").strip()
    if env_value:
        return env_value, "railway_env"
    saved = _load_keys()
    file_value = (saved.get(provider, "") or "").strip()
    if file_value:
        return file_value, "file_fallback"
    return "", "missing"

class KeyInput(BaseModel):
    key: str

@app.get("/keys")
def list_keys():
    result = []
    for provider, info in _SUPPORTED_PROVIDERS.items():
        key, source = _get_key(provider)
        result.append({
            "provider": provider,
            "label": info["label"],
            "is_set": bool(key),
            "preview": _mask_key(key) if key else "",
            "env_var": info["env_var"],
            "expected_prefix": info["prefix"],
            "source": source,
            "persistent": source == "railway_env",
        })
    return result

@app.post("/keys/{provider}")
def set_key(provider: str, body: KeyInput):
    provider = provider.lower().strip()
    if provider not in _SUPPORTED_PROVIDERS:
        raise HTTPException(400, f"Unknown provider: {provider}. Supported: {list(_SUPPORTED_PROVIDERS.keys())}")
    key = body.key.strip()
    if not key:
        raise HTTPException(400, "Key cannot be empty")

    info = _SUPPORTED_PROVIDERS[provider]
    os.environ[info["env_var"]] = key

    keys = _load_keys()
    keys[provider] = key
    _save_keys(keys)

    return {
        "status": "saved_runtime_fallback",
        "provider": provider,
        "preview": _mask_key(key),
        "env_var": info["env_var"],
        "warning": "Saved to runtime fallback file only. For persistent Railway storage, set this key in Railway Variables.",
    }

@app.delete("/keys/{provider}")
def delete_key(provider: str):
    provider = provider.lower().strip()
    if provider not in _SUPPORTED_PROVIDERS:
        raise HTTPException(400, f"Unknown provider: {provider}")

    keys = _load_keys()
    keys.pop(provider, None)
    _save_keys(keys)

    key, source = _get_key(provider)
    return {
        "status": "file_fallback_deleted",
        "provider": provider,
        "still_available": bool(key),
        "source": source,
        "note": "Railway Variables cannot be removed from inside the app. Remove them in Railway Variables if required.",
    }

@app.post("/keys/{provider}/test")
def test_key(provider: str):
    provider = provider.lower().strip()

    if provider not in _SUPPORTED_PROVIDERS:
        return {"status": "error", "message": "Unknown provider: " + provider}

    key, source = _get_key(provider)
    if not key:
        return {"status": "no_key", "message": "No key set for " + provider, "source": source}

    try:
        import requests

        if provider == "github":
            r = requests.get(
                "https://api.github.com/rate_limit",
                headers={
                    "Authorization": f"Bearer {key}",
                    "User-Agent": "NyoSig",
                    "Accept": "application/vnd.github+json",
                },
                timeout=15,
            )
            if r.status_code == 200:
                data = r.json()
                limit = data.get("resources", {}).get("core", {}).get("limit", 0)
                remaining = data.get("resources", {}).get("core", {}).get("remaining", 0)
                return {"status": "ok", "message": f"GitHub token valid. Limit: {limit}/hr, remaining: {remaining}", "source": source}
            return {"status": "error", "message": f"GitHub test failed HTTP {r.status_code}: {r.text[:300]}", "source": source}

        if provider == "openai":
            r = requests.get("https://api.openai.com/v1/models", headers={"Authorization": f"Bearer {key}"}, timeout=20)
            if r.status_code == 200:
                return {"status": "ok", "message": "OpenAI API key valid", "source": source}
            return {"status": "error", "message": f"OpenAI test failed HTTP {r.status_code}: {r.text[:300]}", "source": source}

        if provider == "anthropic":
            r = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 8,
                    "messages": [{"role": "user", "content": "Say OK"}],
                },
                timeout=30,
            )
            if r.status_code == 200:
                return {"status": "ok", "message": "Claude API key valid", "source": source}
            return {"status": "error", "message": f"Claude test failed HTTP {r.status_code}: {r.text[:300]}", "source": source}

        if provider == "gemini":
            r = requests.get("https://generativelanguage.googleapis.com/v1beta/models", params={"key": key}, timeout=20)
            if r.status_code == 200:
                return {"status": "ok", "message": "Gemini API key valid", "source": source}
            return {"status": "error", "message": f"Gemini test failed HTTP {r.status_code}: {r.text[:300]}", "source": source}

        if provider == "grok":
            r = requests.post(
                "https://api.x.ai/v1/responses",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={"model": "grok-4.20-reasoning", "input": "Say OK"},
                timeout=30,
            )
            if r.status_code == 200:
                return {"status": "ok", "message": "Grok xAI API key valid", "source": source}
            return {"status": "error", "message": f"Grok test failed HTTP {r.status_code}: {r.text[:300]}", "source": source}

        return {"status": "unknown", "message": f"No test available for {provider}", "source": source}

    except Exception as e:
        return {"status": "error", "message": str(e)[:500], "source": source}

# --- Automation Engine ---
try:
    from nyosig_automator import (AutomationEngine, AutomationConfig,
        save_automation_config, load_automation_config, run_server_mode)
    _HAS_AUTOMAT = True
except ImportError:
    _HAS_AUTOMAT = False

_automat_engine: Optional[Any] = None

@app.get("/automat/config")
def get_automat_config():
    """Get current automation configuration."""
    if not _HAS_AUTOMAT:
        return {"error": "Automator module not loaded. Ensure nyosig_automator.py is in the same directory."}
    cfg = load_automation_config(PROJECT_ROOT)
    return cfg.to_dict()

@app.post("/automat/config")
def set_automat_config(body: Dict[str, Any]):
    """Update automation configuration."""
    if not _HAS_AUTOMAT:
        raise HTTPException(500, "Automator module not loaded")
    cfg = AutomationConfig.from_dict(body)
    save_automation_config(cfg, PROJECT_ROOT)
    return {"status": "saved", "config": cfg.to_dict()}

@app.post("/automat/start")
def start_automat():
    """Start the automation engine with saved config."""
    global _automat_engine
    if not _HAS_AUTOMAT:
        raise HTTPException(500, "Automator module not loaded")
    if _automat_engine and _automat_engine.is_running():
        raise HTTPException(400, "Automation already running")
    cfg = load_automation_config(PROJECT_ROOT)
    _automat_engine = AutomationEngine(_core, PROJECT_ROOT, paths.db_path, cfg, _log)
    _automat_engine.start()
    return {"status": "started", "config": cfg.to_dict()}

@app.post("/automat/stop")
def stop_automat():
    """Stop the automation engine."""
    global _automat_engine
    if _automat_engine:
        _automat_engine.stop()
        _automat_engine = None
    return {"status": "stopped"}

@app.get("/automat/status")
def automat_status():
    """Get automation engine status."""
    if not _HAS_AUTOMAT:
        return {"running": False, "run_count": 0, "error": "Automator not loaded"}
    if _automat_engine:
        return _automat_engine.status()
    return {"running": False, "run_count": 0}

@app.get("/automat/history")
def automat_history(limit: int = 20):
    """Get automation run history."""
    if _automat_engine:
        return _automat_engine.history(limit)
    return []

@app.get("/automat/candidates")
def automat_candidates(selection_id: Optional[int] = None):
    """Get current candidates with all columns for sorting/selection UI."""
    if not _HAS_AUTOMAT:
        return []
    try:
        with get_db() as con:
            if not selection_id:
                row = con.execute(
                    "SELECT selection_id FROM topnow_selection ORDER BY selection_id DESC LIMIT 1;").fetchone()
                if not row:
                    return []
                selection_id = row[0]
            snap = con.execute(
                "SELECT snapshot_id FROM topnow_selection WHERE selection_id=?;",
                (selection_id,)).fetchone()
            snap_id = snap[0] if snap else None
            items = con.execute(
                "SELECT rank_in_selection, unified_symbol, composite_preview "
                "FROM topnow_selection_items WHERE selection_id=? "
                "ORDER BY rank_in_selection;", (selection_id,)).fetchall()
            market = {}
            if snap_id:
                for m in con.execute(
                    "SELECT unified_symbol, price, mcap, vol24, change_24h_pct, rank, base_score "
                    "FROM market_snapshots WHERE snapshot_id=? AND timeframe='spot';",
                    (snap_id,)).fetchall():
                    market[m[0]] = {"price": m[1], "mcap": m[2], "vol24": m[3],
                                    "chg24": m[4], "mkt_rank": m[5], "base_score": m[6]}
        result = []
        for rank, sym, comp in items:
            m = market.get(sym, {})
            result.append({
                "rank": rank, "symbol": sym, "composite": comp or 0,
                "price": m.get("price", 0), "mcap": m.get("mcap", 0),
                "vol24": m.get("vol24", 0), "chg24": m.get("chg24", 0),
                "mkt_rank": m.get("mkt_rank", 0), "base_score": m.get("base_score", 0),
            })
        return result
    except sqlite3.OperationalError as exc:
        if _is_db_locked(exc):
            _log("AUTOMAT candidates skipped: database locked")
            return []
        raise
    except Exception as exc:
        _log("AUTOMAT candidates error: " + str(exc)[:200])
        return []



# --- Analytics Log ---
try:
    from nyosig_analytics_log import AnalyticsLogger, get_analytics_db_path
    _analytics = AnalyticsLogger(PROJECT_ROOT)
    _HAS_ANALYTICS = True
except ImportError:
    _analytics = None
    _HAS_ANALYTICS = False

@app.get("/analytics/overview")
def analytics_overview():
    """Get aggregate performance metrics across all runs."""
    if not _HAS_ANALYTICS:
        raise HTTPException(500, "Analytics module not loaded")
    return _analytics.get_performance_overview()

@app.get("/analytics/runs")
def analytics_runs(limit: int = 30):
    """Get run profiles with timing data."""
    if not _HAS_ANALYTICS:
        return []
    return _analytics.get_run_profiles(limit)

@app.get("/analytics/runs/{run_id}/operations")
def analytics_operations(run_id: int):
    """Get all operations for a specific run with timing."""
    if not _HAS_ANALYTICS:
        return []
    return _analytics.get_operations_for_run(run_id)

@app.get("/analytics/runs/{run_id}/layers")
def analytics_layers(run_id: int):
    """Get layer-by-layer timing breakdown for a run."""
    if not _HAS_ANALYTICS:
        return []
    return _analytics.get_layer_timings_for_run(run_id)

@app.get("/analytics/runs/{run_id}/api_calls")
def analytics_api_calls(run_id: int, limit: int = 200):
    """Get all external API calls for a run."""
    if not _HAS_ANALYTICS:
        return []
    return _analytics.get_api_calls_for_run(run_id, limit)

@app.get("/analytics/daily")
def analytics_daily(limit: int = 30):
    """Get daily performance summaries."""
    if not _HAS_ANALYTICS:
        return []
    _analytics.compute_daily_summary()
    return _analytics.get_daily_summaries(limit)

@app.get("/analytics/db_size")
def analytics_db_size():
    """Get analytics database file size."""
    if not _HAS_ANALYTICS:
        return {"size_mb": 0}
    return {"size_mb": _analytics.db_size_mb(),
            "path": get_analytics_db_path(PROJECT_ROOT)}


# =====================================================================
# ENTRY POINT — must be at the very end after all endpoints are registered
# =====================================================================
if __name__ == "__main__":
    import uvicorn
    # Diagnostic: list all registered routes
    _routes = [r.path for r in app.routes if hasattr(r, "path")]
    print(f"NyoSig API {APP_VERSION} starting...")
    print(f"Root: {PROJECT_ROOT}")
    print(f"DB: {paths.db_path}")
    print(f"Registered endpoints: {len(_routes)}")
    for _r in sorted(_routes):
        print(f"  {_r}")
    print(f"Docs: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)

