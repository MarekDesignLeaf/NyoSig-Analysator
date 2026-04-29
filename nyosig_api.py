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

APP_VERSION = "v7.5e-web"

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
        try:
            with get_db() as con:
                scopes = [lr["scope_key"] for lr in LAYER_REGISTRY]
                res = _core.prepare_and_store_composite_preview(
                    con, req.selection_id, scopes, run_id=req.run_id)
                # Generate predictions + trade plans
                if req.run_id:
                    _core.persist_feature_vectors(con, req.run_id, req.selection_id)
                    _core.persist_predictions(con, req.run_id, req.selection_id)
                    _core.persist_trade_plans(con, req.run_id, req.selection_id)
            _pipeline_state["status"] = "done"
            _pipeline_state["result"] = {
                "updated": res.get("updated_items", 0),
                "layers": len(res.get("scopes", [])),
            }
        except Exception as e:
            _pipeline_state["status"] = "failed"
            _pipeline_state["result"] = {"error": str(e)[:500]}
        finally:
            _pipeline_state["running"] = False

    background_tasks.add_task(_run)
    return {"status": "started"}

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
    """Generate AI market intelligence report from run data."""
    with get_db() as con:
        summary = _core.run_summary(con, run_id, selection_id)
        preds = _core.load_predictions(con, run_id, selection_id)
        plans = _core.load_trade_plans(con, run_id, selection_id)
        feats = _core.load_feature_vectors_for_view(con, run_id, selection_id)
        cors = _core.cross_scope_correlation(con, run_id)
        risk = _core.compute_portfolio_risk(con)

    if not _HAS_AI:
        from nyosig_ai_commentator import _generate_fallback_report
        return {"report": _generate_fallback_report(summary, preds, cors),
                "model": "fallback", "error": "AI commentator not loaded"}

    scope = summary.get("scope", "crypto_spot")
    if multi:
        return generate_multi_ai_commentary(
            summary, preds, plans, feats, cors, risk, scope)
    else:
        return generate_ai_commentary(
            summary, preds, plans, feats, cors, risk, scope)


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
    """Load saved keys and set as environment variables."""
    keys = _load_keys()
    for provider, info in _SUPPORTED_PROVIDERS.items():
        if provider in keys and keys[provider]:
            os.environ[info["env_var"]] = keys[provider]

# Apply saved keys on startup
try:
    _apply_keys_to_env()
except Exception as _ake:
    print(f"WARNING: Failed to load saved API keys: {_ake}")

def _mask_key(key: str) -> str:
    if not key or len(key) < 12:
        return "****"
    return key[:6] + "..." + key[-4:]

class KeyInput(BaseModel):
    key: str

@app.get("/keys")
def list_keys():
    """List all API key providers with status (set/not set, masked preview)."""
    saved = _load_keys()
    result = []
    for provider, info in _SUPPORTED_PROVIDERS.items():
        key = saved.get(provider, "") or os.environ.get(info["env_var"], "")
        result.append({
            "provider": provider,
            "label": info["label"],
            "is_set": bool(key),
            "preview": _mask_key(key) if key else "",
            "env_var": info["env_var"],
            "expected_prefix": info["prefix"],
        })
    return result

@app.post("/keys/{provider}")
def set_key(provider: str, body: KeyInput):
    """Save an API key for a provider."""
    if provider not in _SUPPORTED_PROVIDERS:
        raise HTTPException(400, f"Unknown provider: {provider}. "
                                  f"Supported: {list(_SUPPORTED_PROVIDERS.keys())}")
    key = body.key.strip()
    if not key:
        raise HTTPException(400, "Key cannot be empty")
    info = _SUPPORTED_PROVIDERS[provider]
    # Set in env immediately
    os.environ[info["env_var"]] = key
    # Persist to file
    keys = _load_keys()
    keys[provider] = key
    _save_keys(keys)
    return {"status": "saved", "provider": provider, "preview": _mask_key(key)}

@app.delete("/keys/{provider}")
def delete_key(provider: str):
    """Delete a saved API key."""
    if provider not in _SUPPORTED_PROVIDERS:
        raise HTTPException(400, f"Unknown provider: {provider}")
    info = _SUPPORTED_PROVIDERS[provider]
    # Remove from env
    os.environ.pop(info["env_var"], None)
    # Remove from file
    keys = _load_keys()
    keys.pop(provider, None)
    _save_keys(keys)
    return {"status": "deleted", "provider": provider}

@app.post("/keys/{provider}/test")
def test_key(provider: str):
    """Quick test if the API key works."""
    saved = _load_keys()
    key = saved.get(provider, "") or os.environ.get(
        _SUPPORTED_PROVIDERS.get(provider, {}).get("env_var", ""), "")
    if not key:
        return {"status": "no_key", "message": "No key set for " + provider}
    try:
        if provider == "anthropic":
            import anthropic
            c = anthropic.Anthropic(api_key=key)
            r = c.messages.create(model="claude-sonnet-4-20250514", max_tokens=10,
                                   messages=[{"role": "user", "content": "Say OK"}])
            return {"status": "ok", "message": "Claude API working", "response": r.content[0].text[:50]}
        elif provider == "openai":
            import openai
            c = openai.OpenAI(api_key=key)
            r = c.chat.completions.create(model="gpt-4o-mini", max_tokens=10,
                                           messages=[{"role": "user", "content": "Say OK"}])
            return {"status": "ok", "message": "OpenAI API working", "response": r.choices[0].message.content[:50]}
        elif provider == "gemini":
            import google.generativeai as genai
            genai.configure(api_key=key)
            m = genai.GenerativeModel("gemini-2.0-flash")
            r = m.generate_content("Say OK")
            return {"status": "ok", "message": "Gemini API working", "response": r.text[:50]}
        elif provider == "github":
            import urllib.request
            req = urllib.request.Request("https://api.github.com/rate_limit",
                headers={"Authorization": f"token {key}", "User-Agent": "NyoSig"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            limit = data.get("resources", {}).get("core", {}).get("limit", 0)
            return {"status": "ok", "message": f"GitHub token valid (limit: {limit}/hr)"}
        else:
            return {"status": "unknown", "message": f"No test available for {provider}"}
    except Exception as e:
        return {"status": "error", "message": str(e)[:200]}


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

