#!/usr/bin/env python3
# -*- coding: utf-8-sig -*-
# nyosig_analysator_core_v7.5c -- CORE
# NyoSig_Analysator -- Core engine
# Architecture ref: v4.0a  |  Tasklist: IMPLEMENTATION_SYNC_TASKLIST_v2.4a
# v7.6c hotfix: stocks_spot non-crypto normalisation + no crypto OHLCV cascade for stocks
# v7.5c changes vs v7.5a:
#   FIX-1: Integrates v7.5b live-run history/profile logging into the full v7.5a core
#   FIX-2: Adds run_pipeline() compatibility wrapper without replacing the full analytical core
#   FIX-3: Strengthens CoinGecko HTTP headers and uses the project root required by NyoSig
# v7.5a changes vs v7.4a:
#   PORT-1: Portfolio positions table + open/close/update CRUD
#   PORT-2: Risk metrics -- portfolio heat, max drawdown, concentration
#   PORT-3: Auto-retention in scheduler -- prune old runs automatically
#   PORT-4: Config persistence -- save/load analysis profiles to JSON
#   PORT-5: Portfolio dashboard query functions for GUI
# v7.4a: Scheduler, health check, E2E test, backtest v2
from __future__ import annotations
import sys as _sys, os as _os
# BOM guard v1.0e: remove U+FEFF if present (Pydroid3 / Android file transfer issue)
def _strip_bom_and_reexec():
    try:
        _path = _os.path.abspath(__file__)
        with open(_path, 'rb') as _f:
            _raw = _f.read()
        if _raw[:3] == b'\xef\xbb\xbf' or b'\xef\xbb\xbf' in _raw[3:]:
            _clean = _raw.replace(b'\xef\xbb\xbf', b'')
            import tempfile as _tmp, runpy as _rp
            _tf = _tmp.NamedTemporaryFile(suffix='.py', delete=False, mode='wb')
            _tf.write(_clean)
            _tf.close()
            _rp.run_path(_tf.name, run_name='__main__')
            _os.unlink(_tf.name)
            raise SystemExit(0)
    except SystemExit:
        raise
    except Exception:
        pass
_strip_bom_and_reexec()

# ---- secrets env loader (v6.0f) ----
import pathlib as _pl

def _load_secrets_env():
    """Load config/secrets.env into os.environ (minimal key=value, no overrides)."""
    try:
        # If token already provided via environment, do nothing
        if _os.environ.get('NYOSIG_GITHUB_TOKEN'):
            return False
        _root = _pl.Path(__file__).resolve().parents[2]
        _p = _root / 'config' / 'secrets.env'
        if not _p.is_file():
            return False
        for _line in _p.read_text(encoding='utf-8').splitlines():
            _s = _line.strip()
            if not _s or _s.startswith('#') or '=' not in _s:
                continue
            _k, _v = _s.split('=', 1)
            _k = _k.strip()
            _v = _v.strip()
            if not _k:
                continue
            if _k not in _os.environ:
                _os.environ[_k] = _v
        return True
    except Exception:
        return False

_SECRETS_LOADED = _load_secrets_env()


# ---- util_time ----
import time

def utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def utc_stamp_compact() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.gmtime())

# ---- cache ----
import hashlib, json, os, time
from typing import Any, Optional

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def cache_key(obj: Any) -> str:
    b = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(b).hexdigest()

def cache_path(cache_dir: str, key: str) -> str:
    return os.path.join(cache_dir, key + ".json")

def load_cache_if_fresh(path: str, ttl_s: int) -> Optional[Any]:
    if not os.path.isfile(path):
        return None
    age = time.time() - os.path.getmtime(path)
    if age > ttl_s:
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_cache(path: str, obj: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)

# ---- config ----
import os
from dataclasses import dataclass
from typing import Any, Dict


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def parse_secrets_env(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not os.path.isfile(path):
        return out
    for line in read_text(path).splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _parse_scalar(val: str):
    v = val.strip()
    if v.lower() in ("true","false"):
        return v.lower() == "true"
    try:
        if "." in v:
            return float(v)
        return int(v)
    except Exception:
        return v

def parse_simple_yaml(path: str) -> Dict[str, Any]:
    """Tiny YAML subset: nested dicts, lists in [a,b], bool, int/float, strings."""
    if not os.path.isfile(path):
        return {}
    data: Dict[str, Any] = {}
    stack = [(0, data)]
    for raw in read_text(path).splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        key = key.strip()
        val = val.strip()
        while stack and indent < stack[-1][0]:
            stack.pop()
        cur = stack[-1][1] if stack else data
        if val == "":
            cur[key] = {}
            stack.append((indent + 2, cur[key]))
            continue
        if val.startswith("[") and val.endswith("]"):
            inner = val[1:-1].strip()
            if not inner:
                cur[key] = []
            else:
                cur[key] = [_parse_scalar(x) for x in inner.split(",")]
            continue
        cur[key] = _parse_scalar(val)
    return data


# Task 19: Providers config -- centralised provider settings ----------------------
# Loaded from config/providers.yaml at startup.
# Falls back to built-in defaults if file missing.

_DEFAULT_PROVIDERS_YAML = """# NyoSig_Analysator -- providers.yaml
# Task 19: centralised provider config (plan, limits, ttl, cascade priority)
# Edit to enable/disable providers or switch free->paid plan.

retention:
  keep_runs: 5

providers:
  coingecko:
    enabled: true
    plan: free
    limit_per_min: 25
    ttl_spot_s: 300
    ttl_ohlcv_s: 3600
    priority: 1
  cryptocompare:
    enabled: true
    plan: free
    limit_per_min: 30
    ttl_ohlcv_s: 3600
    priority: 2
  binance:
    enabled: true
    plan: free
    limit_per_min: 60
    ttl_ohlcv_s: 1800
    priority: 3
  github:
    enabled: true
    plan: free
    limit_per_min: 1
    cache_h: 24
    token_env: NYOSIG_GITHUB_TOKEN
  yahoo:
    enabled: true
    plan: free
    limit_per_min: 10
  blockchain:
    enabled: true
    plan: free
    limit_per_min: 6
  fred:
    enabled: true
    plan: free
    limit_per_min: 10
  alternative:
    enabled: true
    plan: free
    limit_per_min: 10
"""


def write_default_providers_yaml(path: str) -> None:
    """Write providers.yaml with defaults if file does not exist. Task 19."""
    if os.path.isfile(path):
        return
    try:
        ensure_dir(os.path.dirname(path))
        with open(path, "w", encoding="utf-8") as _f:
            _f.write(_DEFAULT_PROVIDERS_YAML)
    except Exception:
        pass


def parse_providers_config(path: str) -> Dict[str, Any]:
    """
    Load providers.yaml. Task 19.
    Returns dict with keys: retention (dict), providers (dict of provider_id -> settings).
    Falls back to built-in defaults if file missing or unparseable.
    """
    try:
        if os.path.isfile(path):
            raw = parse_simple_yaml(path)
            if isinstance(raw, dict) and "providers" in raw:
                return raw
    except Exception:
        pass
    # Fallback: parse built-in defaults
    import io as _io
    _tmp_path = None
    try:
        import tempfile as _tmp
        fd, _tmp_path = _tmp.mkstemp(suffix=".yaml", text=True)
        import os as _os2
        with _os2.fdopen(fd, "w") as _f:
            _f.write(_DEFAULT_PROVIDERS_YAML)
        return parse_simple_yaml(_tmp_path)
    except Exception:
        return {"retention": {"keep_runs": 5}, "providers": {}}
    finally:
        if _tmp_path:
            try:
                import os as _os3
                _os3.unlink(_tmp_path)
            except Exception:
                pass


def apply_providers_to_rl_manager(providers_cfg: Dict[str, Any]) -> None:
    """
    Update RateLimitManager DEFAULT_LIMITS from providers config. Task 19.
    Called at startup after providers.yaml is loaded.
    """
    try:
        rl = get_rate_limit_manager()
        prov = providers_cfg.get("providers", {})
        for pid, settings in prov.items():
            if not isinstance(settings, dict):
                continue
            lpm = settings.get("limit_per_min")
            if isinstance(lpm, (int, float)) and lpm > 0:
                rl.DEFAULT_LIMITS[str(pid)] = int(lpm)
    except Exception:
        pass





RETENTION_KEEP_RUNS = 5  # default; overridden by config at startup (Task 17)

def apply_retention_policy(con: sqlite3.Connection,
                           keep_runs: int = RETENTION_KEEP_RUNS,
                           log_cb=None) -> dict:
    """Prune non-tracked data older than keep_runs runs. Tracked symbols kept forever.
    Spec G2.3: only tracked assets archived long-term.
    Task 5:  write state_log event after pruning.
    Task 17: also prune layer_results for non-tracked symbols in pruned runs.
    """
    def _log(m):
        if log_cb: log_cb("RETENTION: " + m)
    tracked = set(
        r[0] for r in con.execute(
            "SELECT unified_symbol FROM watchlist WHERE exit_timestamp_utc IS NULL;"
        ).fetchall()
    )
    all_runs = [r[0] for r in con.execute(
        "SELECT run_id FROM runs ORDER BY run_id DESC;"
    ).fetchall()]
    prune_ids = all_runs[keep_runs:]
    if not prune_ids:
        _log("nothing to prune")
        return {"pruned": False}
    ph = ",".join("?" * len(prune_ids))
    tr = list(tracked)
    tp = ",".join("?" * len(tr)) if tr else "''"
    removed = {}
    for table in ("market_snapshots", "ohlcv_snapshots"):
        if tr:
            cur = con.execute(
                "DELETE FROM " + table + " WHERE run_id IN (" + ph + ")"
                " AND unified_symbol NOT IN (" + tp + ");",
                prune_ids + tr)
        else:
            cur = con.execute(
                "DELETE FROM " + table + " WHERE run_id IN (" + ph + ");", prune_ids)
        removed[table] = cur.rowcount
    old_sels = [r[0] for r in con.execute(
        "SELECT selection_id FROM topnow_selection WHERE run_id IN (" + ph + ");",
        prune_ids).fetchall()]
    if old_sels:
        sp = ",".join("?" * len(old_sels))
        if tr:
            cur = con.execute(
                "DELETE FROM topnow_selection_items WHERE selection_id IN (" + sp + ")"
                " AND unified_symbol NOT IN (" + tp + ");", old_sels + tr)
        else:
            cur = con.execute(
                "DELETE FROM topnow_selection_items WHERE selection_id IN (" + sp + ");",
                old_sels)
        removed["topnow_selection_items"] = cur.rowcount
    for table in ("raw_snapshots", "cascade_log"):
        # Task 6: raw_snapshots has immutability triggers -- must drop temporarily for retention
        if table == "raw_snapshots":
            try:
                con.execute("DROP TRIGGER IF EXISTS trg_raw_snapshots_no_delete;")
            except Exception:
                pass
        cur = con.execute(
            "DELETE FROM " + table + " WHERE run_id IN (" + ph + ");", prune_ids)
        removed[table] = cur.rowcount
        # Restore trigger after deletion
        if table == "raw_snapshots":
            try:
                con.execute("""
                    CREATE TRIGGER IF NOT EXISTS trg_raw_snapshots_no_delete
                    BEFORE DELETE ON raw_snapshots
                    BEGIN
                        SELECT RAISE(ABORT,
                            'raw_snapshots is immutable: DELETE not allowed (use retention policy)');
                    END;
                """)
            except Exception:
                pass
    # Task 17: prune layer_results for non-tracked symbols in pruned runs
    if tr:
        cur = con.execute(
            "DELETE FROM layer_results WHERE run_id IN (" + ph + ")"
            " AND (unified_symbol IS NULL OR unified_symbol NOT IN (" + tp + "));",
            prune_ids + tr)
    else:
        cur = con.execute(
            "DELETE FROM layer_results WHERE run_id IN (" + ph + ");", prune_ids)
    removed["layer_results"] = cur.rowcount
    con.commit()
    total = sum(removed.values())
    _log("done removed=" + str(total) + " tracked_protected=" + str(len(tracked))
         + " pruned_runs=" + str(len(prune_ids)))
    # Task 5: state_log event for audit trail
    import json as _rj
    try:
        state_log(con, None, "retention_done",
                  from_status="retention_start",
                  message=_rj.dumps({"pruned_runs": len(prune_ids),
                                     "total_removed": total,
                                     "keep_runs": keep_runs,
                                     "tracked_protected": len(tracked),
                                     "by_table": removed}, ensure_ascii=True),
                  severity="info")
        con.commit()
    except Exception:
        pass
    return {"pruned": True, "keep_runs": keep_runs, "pruned_runs": len(prune_ids),
            "tracked_protected": len(tracked), "removed": removed, "total_removed": total}

# ===========================================================================
# SCOPE REGISTRY (v7.0a) -- Multi-asset scope definitions
# Each scope defines: universe source, scoring adapters, display config.
# Spec: Crypto (primary), Forex, Stocks, Macro (future expansion).
# ===========================================================================

@dataclass
class ScopeDefinition:
    """Definition of an analysis scope (asset class)."""
    scope_key: str
    display_name: str
    asset_class: str          # crypto, forex, stocks, macro
    universe_source: str      # primary data provider
    universe_endpoint: str
    default_vs_currency: str
    default_limit: int
    default_timeframes: list
    supported_layers: list    # which layer scope_keys apply
    symbol_examples: list


SCOPE_REGISTRY = {
    "crypto_spot": ScopeDefinition(
        scope_key="crypto_spot",
        display_name="Crypto Spot",
        asset_class="crypto",
        universe_source="coingecko",
        universe_endpoint="/coins/markets",
        default_vs_currency="usd",
        default_limit=250,
        default_timeframes=["1d", "1h", "15m"],
        supported_layers=[
            "crypto_spot", "crypto_derivatives", "onchain", "institutions",
            "macro", "sentiment", "technical", "community", "open_interest", "fundamental",
        ],
        symbol_examples=["BTC", "ETH", "SOL", "ADA", "XRP"],
    ),
    "forex_spot": ScopeDefinition(
        scope_key="forex_spot",
        display_name="Forex Spot",
        asset_class="forex",
        universe_source="yahoo_finance",
        universe_endpoint="fx_quotes",
        default_vs_currency="usd",
        default_limit=20,
        default_timeframes=["1d", "1h"],
        supported_layers=["forex_spot", "macro", "sentiment", "technical"],
        symbol_examples=["EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X"],
    ),
    "stocks_spot": ScopeDefinition(
        scope_key="stocks_spot",
        display_name="Stocks / ETFs",
        asset_class="stocks",
        universe_source="yahoo_finance",
        universe_endpoint="equity_quotes",
        default_vs_currency="usd",
        default_limit=30,
        default_timeframes=["1d"],
        supported_layers=["stocks_spot", "macro", "sentiment", "technical", "fundamental"],
        symbol_examples=["SPY", "QQQ", "AAPL", "MSFT", "NVDA"],
    ),
    "macro_dashboard": ScopeDefinition(
        scope_key="macro_dashboard",
        display_name="Macro Dashboard",
        asset_class="macro",
        universe_source="yahoo_finance",
        universe_endpoint="macro_quotes",
        default_vs_currency="usd",
        default_limit=10,
        default_timeframes=["1d"],
        supported_layers=["macro", "sentiment"],
        symbol_examples=["DX-Y.NYB", "^VIX", "^TNX", "GC=F", "CL=F"],
    ),
}


def get_scope(scope_key: str) -> Optional[ScopeDefinition]:
    """Return scope definition or None."""
    return SCOPE_REGISTRY.get(scope_key)


def list_scopes() -> list:
    """Return list of (scope_key, display_name) tuples."""
    return [(k, v.display_name) for k, v in SCOPE_REGISTRY.items()]


# ===========================================================================
# FOREX + STOCKS DATA FETCHERS (v7.0a)
# All use Yahoo Finance (free, public, no API key).
# ===========================================================================

# --- Forex universe ---
FOREX_PAIRS = [
    ("EURUSD=X", "EUR/USD"), ("GBPUSD=X", "GBP/USD"), ("USDJPY=X", "USD/JPY"),
    ("AUDUSD=X", "AUD/USD"), ("USDCAD=X", "USD/CAD"), ("USDCHF=X", "USD/CHF"),
    ("NZDUSD=X", "NZD/USD"), ("EURGBP=X", "EUR/GBP"), ("EURJPY=X", "EUR/JPY"),
    ("GBPJPY=X", "GBP/JPY"), ("EURCHF=X", "EUR/CHF"), ("AUDJPY=X", "AUD/JPY"),
    ("USDMXN=X", "USD/MXN"), ("USDZAR=X", "USD/ZAR"), ("USDTRY=X", "USD/TRY"),
    ("USDPLN=X", "USD/PLN"), ("USDCZK=X", "USD/CZK"), ("USDSEK=X", "USD/SEK"),
    ("USDNOK=X", "USD/NOK"), ("USDHKD=X", "USD/HKD"),
]

# --- Stocks universe (sector ETFs + blue chips) ---
STOCKS_UNIVERSE = [
    ("SPY", "S&P 500 ETF"), ("QQQ", "Nasdaq 100 ETF"), ("DIA", "Dow Jones ETF"),
    ("IWM", "Russell 2000 ETF"), ("XLF", "Financials ETF"), ("XLK", "Technology ETF"),
    ("XLE", "Energy ETF"), ("XLV", "Healthcare ETF"), ("XLI", "Industrials ETF"),
    ("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "NVIDIA"),
    ("GOOGL", "Alphabet"), ("AMZN", "Amazon"), ("META", "Meta"),
    ("TSLA", "Tesla"), ("JPM", "JPMorgan"), ("V", "Visa"),
    ("JNJ", "Johnson & Johnson"), ("WMT", "Walmart"),
    ("BRK-B", "Berkshire Hathaway"), ("MA", "Mastercard"),
    ("PG", "Procter & Gamble"), ("HD", "Home Depot"),
    ("BAC", "Bank of America"), ("XOM", "ExxonMobil"),
    ("PFE", "Pfizer"), ("KO", "Coca-Cola"), ("PEP", "PepsiCo"),
    ("COST", "Costco"),
]

# --- Macro dashboard instruments ---
MACRO_INSTRUMENTS = [
    ("DX-Y.NYB", "US Dollar Index (DXY)"),
    ("^VIX", "CBOE Volatility Index"),
    ("^TNX", "US 10Y Treasury Yield"),
    ("^GSPC", "S&P 500 Index"),
    ("^DJI", "Dow Jones Industrial"),
    ("^IXIC", "Nasdaq Composite"),
    ("GC=F", "Gold Futures"),
    ("CL=F", "Crude Oil WTI Futures"),
    ("SI=F", "Silver Futures"),
    ("BTC-USD", "Bitcoin (cross-ref)"),
]


def fetch_yahoo_universe(tickers_with_names: list, log_cb=None) -> list:
    """
    Fetch current quotes for a list of (ticker, name) pairs from Yahoo Finance.
    Returns list of dicts compatible with market_snapshots normalisation.
    Free, public, no API key.
    """
    import time as _t
    if log_cb is None:
        log_cb = lambda m: None
    results = []
    for i, (ticker, name) in enumerate(tickers_with_names):
        try:
            price, date_str = _fetch_yahoo_quote(ticker, log_cb=log_cb)
            if price is not None:
                # Normalise to market_snapshots format
                symbol = ticker.replace("=X", "").replace("=F", "").replace("^", "")
                results.append({
                    "id": ticker,
                    "symbol": symbol,
                    "name": name,
                    "current_price": float(price),
                    "market_cap": 0,        # N/A for forex/macro
                    "total_volume": 0,       # Would need separate API
                    "market_cap_rank": i + 1,
                    "price_change_percentage_24h": 0,  # Yahoo doesn't return this directly
                    "_source": "yahoo_finance",
                    "_ticker": ticker,
                    "_date": date_str,
                })
            _t.sleep(0.8)  # Yahoo rate limiting
        except Exception as exc:
            log_cb(f"Yahoo SKIP {ticker}: {str(exc)[:80]}")
    log_cb(f"Yahoo universe: fetched {len(results)}/{len(tickers_with_names)}")
    return results


def normalise_non_crypto_rows(
    items: list, run_id: int, snapshot_key: str,
    scope: str, timeframe: str, fetched_utc: str, source: str,
    snapshot_ref=None,
) -> list:
    """
    Normalise Yahoo Finance data into market_snapshots row tuples.
    Same format as normalise_rows() but without crypto-specific fields.
    """
    rows = []
    for item in items:
        sym = (item.get("symbol") or "").upper()
        if not sym:
            continue
        rows.append((
            snapshot_key,
            snapshot_ref,
            run_id,
            fetched_utc,
            scope,
            timeframe,
            sym,
            f"{sym}/USD",
            float(item.get("current_price") or 0),
            float(item.get("price_change_percentage_24h") or 0),
            float(item.get("total_volume") or 0),
            float(item.get("market_cap") or 0),
            int(item.get("market_cap_rank") or 0),
            None,   # base_score -- computed later
            source,
        ))
    return rows


# --- Scoring for non-crypto assets ---

def score_forex_spot(rank, price, change_24h_pct=0):
    """
    Score forex pair. No market cap concept -- use rank proximity + momentum.
    Returns 0-100.
    """
    # Rank score: major pairs (top 8) score higher
    rank_score = max(0, 100 - rank * 5)
    # Momentum: small daily changes are normal in FX (0.5% = significant)
    momentum = min(20, abs(change_24h_pct or 0) * 20)
    return round(min(100, rank_score * 0.7 + momentum * 0.3 + 30), 2)


def score_stocks_spot(rank, price, market_cap=0, change_24h_pct=0):
    """
    Score equity/ETF. Uses rank + market cap tier + momentum.
    Returns 0-100.
    """
    rank_score = max(0, 100 - rank * 3)
    # Market cap tier: >1T = mega, >100B = large, >10B = mid
    if market_cap > 1e12:
        cap_score = 90
    elif market_cap > 1e11:
        cap_score = 70
    elif market_cap > 1e10:
        cap_score = 50
    else:
        cap_score = 30  # ETFs and smaller
    momentum = min(15, abs(change_24h_pct or 0) * 5)
    return round(min(100, rank_score * 0.4 + cap_score * 0.4 + momentum * 0.2), 2)


DEFAULT_PROJECT_ROOT = "/storage/emulated/0/Programy/analyza_trhu"


def get_project_root(default: str = DEFAULT_PROJECT_ROOT) -> str:
    return os.environ.get("NYOSIG_PROJECT_ROOT", default).strip() or default


@dataclass(frozen=True)
class Paths:
    project_root: str
    db_path: str
    db_dir: str
    defaults_yaml: str
    providers_yaml: str   # Task 19: provider config file
    secrets_env: str
    cache_dir: str
    log_dir: str
    logs_dir: str
    samples_dir: str
    data_dir: str       # spec 2.8: vystupy mimo DB
    exports_dir: str    # backward-compat alias -> data_dir


def make_paths(project_root: str) -> Paths:
    data = os.path.join(project_root, "data")
    cfg  = os.path.join(project_root, "config")
    return Paths(
        project_root=project_root,
        db_path=os.path.join(project_root, "db", "nyosig_analysator.db"),
        db_dir=os.path.join(project_root, "db"),
        defaults_yaml=os.path.join(cfg, "defaults.yaml"),
        providers_yaml=os.path.join(cfg, "providers.yaml"),
        secrets_env=os.path.join(cfg, "secrets.env"),
        cache_dir=os.path.join(project_root, "cache"),
        log_dir=os.path.join(project_root, "logs", "runs"),
        logs_dir=os.path.join(project_root, "logs"),
        samples_dir=os.path.join(project_root, "samples", "raw_snapshots"),
        data_dir=data,
        exports_dir=data,   # alias kept for existing callers
    )

# ---- scoring ----
from typing import Any, Dict, List, Tuple, Optional
import math


def normalise_rows(
    markets: List[Dict[str, Any]],
    run_id: int,
    snapshot_key: str,
    scope_text: str,
    timeframe: str,
    timestamp_utc: str,
    source: str,
    snapshot_ref: Optional[int] = None,
) -> List[Tuple]:
    """
    Map raw CoinGecko market items into market_snapshots row tuples.
    Column order: snapshot_key, snapshot_ref, run_id, timestamp_utc, scope, timeframe,
                  unified_symbol, pair, price, change_24h_pct, vol24, mcap, rank, base_score, source
    """
    rows = []
    for it in markets:
        sym = (it.get("symbol") or "").strip()
        if not sym:
            continue
        rows.append((
            snapshot_key,
            snapshot_ref,
            run_id,
            timestamp_utc,
            scope_text,
            timeframe,
            sym.upper(),
            None,                                    # pair
            it.get("current_price"),
            it.get("price_change_percentage_24h"),
            it.get("total_volume"),
            it.get("market_cap"),
            it.get("market_cap_rank"),
            None,                                    # base_score (filled by pipeline)
            source,
        ))
    return rows


def spot_basic_score(
    rank: Optional[float],
    mcap: Optional[float],
    vol24: Optional[float],
    chg24: Optional[float],
) -> float:
    """
    Deterministic spot_basic score in [0, 100].
    Weights: rank 40, mcap 30, vol24 20, chg24 10 (directional bonus/penalty).
    """
    score = 0.0
    if rank is not None and rank > 0:
        score += max(0.0, 40.0 - min(40.0, math.log(rank + 1, 1.6) * 8.0))
    if mcap is not None and mcap > 0:
        score += min(30.0, math.log(mcap, 10) * 3.0)
    if vol24 is not None and vol24 > 0:
        score += min(20.0, math.log(vol24, 10) * 2.0)
    if chg24 is not None:
        score += max(-10.0, min(10.0, chg24 / 3.0))
    return max(0.0, min(100.0, score))


# ---- rate_limit_manager ----
import threading
import time as _time_rl
from collections import defaultdict, deque


class RateLimitManager:
    """
    Thread-safe rolling-window rate limiter. Spec 8.1.
    Tracks requests per provider in last 60 s.
    Blocks caller if limit exceeded, logs all throttle events.
    """
    DEFAULT_LIMITS = {
        "coingecko":     25,
        "binance":       60,
        "alternative":   10,
        "cryptocompare": 30,
        "yahoo":         10,   # Task 14: unofficial, conservative estimate
        "yahoo_cme":      5,   # v6.1a: CME futures quotes, conservative
        "yahoo_etf":      5,   # v6.1a: ETF price quotes, conservative
        "github":         1,   # Task 14: 60/hr unauth = 1/min; 83/min with token
        "blockchain":     6,   # Task 14: blockchain.com public, conservative
        "blockchair":     6,   # v6.1a: Blockchair free tier, ~30/min but conservative
        "fred":          10,   # Task 14: FRED CSV, very generous
        "default":       20,
    }

    def __init__(self, log_cb=None):
        self._lock = threading.Lock()
        self._windows = defaultdict(deque)
        self._counters = defaultdict(int)
        self._log_cb = log_cb or (lambda m: None)

    def _limit(self, provider):
        return self.DEFAULT_LIMITS.get(provider, self.DEFAULT_LIMITS["default"])

    def acquire(self, provider, block=True):
        limit = self._limit(provider)
        while True:
            with self._lock:
                now = _time_rl.time()
                w = self._windows[provider]
                while w and now - w[0] > 60.0:
                    w.popleft()
                if len(w) < limit:
                    w.append(now)
                    self._counters[provider] += 1
                    return True
                wait_s = 60.0 - (now - w[0]) + 0.1
                if not block:
                    self._log_cb(f"RATE_LIMIT {provider} BLOCKED (non-blocking)")
                    return False
                self._log_cb(
                    f"RATE_LIMIT {provider} limit={limit}/min "
                    f"active={len(w)} waiting={wait_s:.1f}s"
                )
            _time_rl.sleep(min(wait_s, 5.0))

    def stats(self, provider):
        with self._lock:
            now = _time_rl.time()
            w = self._windows[provider]
            while w and now - w[0] > 60.0:
                w.popleft()
            return {
                "provider": provider,
                "requests_last_60s": len(w),
                "limit_per_min": self._limit(provider),
                "total": self._counters[provider],
            }


_GLOBAL_RL_MANAGER = None


def get_rate_limit_manager(log_cb=None):
    global _GLOBAL_RL_MANAGER
    if _GLOBAL_RL_MANAGER is None:
        _GLOBAL_RL_MANAGER = RateLimitManager(log_cb=log_cb)
    if log_cb is not None:
        _GLOBAL_RL_MANAGER._log_cb = log_cb
    return _GLOBAL_RL_MANAGER


# ---- coingecko ----
import json, time, urllib.parse, urllib.request
from typing import Any, Callable, Dict, List, Tuple

COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"

# CoinGecko rate-limit/backoff defaults (standalone safe)
CG_RATE_LIMIT = {
    "max_retries_per_symbol": 4,
    "backoff_sequence_s": [1, 2, 4, 8],
    "first_429_cooldown_s": 20,
    "repeat_429_cooldown_s": 40,
    "safe_interval_s": 1.5,
}

def http_get_json(url: str, headers: Dict[str,str], timeout_s: int) -> Any:
    # v7.5c: always send a clear User-Agent. Some public endpoints reject
    # anonymous default urllib requests even when the endpoint itself is public.
    safe_headers = {"User-Agent": "NyoSig-Analysator/7.5c"}
    if headers:
        safe_headers.update(headers)
    req = urllib.request.Request(url, headers=safe_headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))

def build_markets_url(vs_currency: str, order: str, per_page: int, page: int, price_change_percentage: str) -> Tuple[str, Dict[str,str]]:
    q = {
        "vs_currency": vs_currency,
        "order": order,
        "per_page": str(per_page),
        "page": str(page),
        "sparkline": "false",
        "price_change_percentage": price_change_percentage,
    }
    return COINGECKO_BASE_URL + "/coins/markets?" + urllib.parse.urlencode(q), q

def retry_fetch(url: str, headers: Dict[str,str], connect_timeout_s: int, read_timeout_s: int,
                max_attempts: int, backoff_s: List[int], on_429_sleep_s: int,
                log_cb: Callable[[str], None]) -> Any:
    rl = get_rate_limit_manager(log_cb)
    rl.acquire("coingecko")
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            st = rl.stats("coingecko")
            log_cb(f"FETCH attempt={attempt} rl={st['requests_last_60s']}/{st['limit_per_min']} url={url}")
            return http_get_json(url, headers=headers, timeout_s=connect_timeout_s + read_timeout_s)
        except Exception as e:
            last_err = e
            msg = str(e)
            if "HTTP Error 429" in msg:
                log_cb(f"RATE_LIMIT 429 sleep_s={on_429_sleep_s}")
                time.sleep(float(on_429_sleep_s))
            else:
                sleep_s = backoff_s[min(attempt - 1, len(backoff_s) - 1)]
                log_cb(f"RETRY sleep_s={sleep_s} err={msg}")
                time.sleep(float(sleep_s))
    if last_err:
        raise last_err
    raise RuntimeError("retry_fetch failed without exception")

def fetch_markets(vs_currency: str, order: str, coins_limit: int, headers: Dict[str,str],
                  cache_dir: str, ttl_s: int,
                  connect_timeout_s: int, read_timeout_s: int, max_attempts: int,
                  backoff_s: List[int], on_429_sleep_s: int,
                  log_cb: Callable[[str], None]) -> List[Dict[str, Any]]:
    ensure_dir(cache_dir)
    per_page = 250
    pages = (coins_limit + per_page - 1) // per_page
    all_markets: List[Dict[str, Any]] = []
    for page in range(1, pages + 1):
        url, q = build_markets_url(vs_currency, order, per_page, page, "24h")
        ck = cache_key({"source": "coingecko", "endpoint": "/coins/markets", **q})
        cp = cache_path(cache_dir, ck)
        cached = load_cache_if_fresh(cp, ttl_s)
        if cached is not None:
            log_cb(f"cache_hit page={page}")
            markets_page = cached
        else:
            markets_page = retry_fetch(url, headers, connect_timeout_s, read_timeout_s, max_attempts, backoff_s, on_429_sleep_s, log_cb)
            save_cache(cp, markets_page)
            log_cb(f"cache_save page={page}")

        if isinstance(markets_page, list):
            all_markets.extend(markets_page)
        else:
            raise RuntimeError("Unexpected response type from CoinGecko")

        if len(all_markets) >= coins_limit:
            break
    return all_markets[:coins_limit]


def fetch_coingecko_ohlcv(
    coin_id: str,
    vs_currency: str,
    days: int,
    headers: dict,
    cache_dir: str,
    ttl_s: int,
    log_cb,
) -> list:
    """Fetch OHLCV candles from CoinGecko /coins/{id}/ohlc (free, public)."""
    import urllib.parse, urllib.request, json, time as _time
    q = {"vs_currency": vs_currency, "days": str(days)}
    url = COINGECKO_BASE_URL + "/coins/" + coin_id + "/ohlc?" + urllib.parse.urlencode(q)
    ck = cache_key({"source": "coingecko", "endpoint": "/coins/ohlc", "coin_id": coin_id, **q})
    cp = cache_path(cache_dir, ck)
    cached = load_cache_if_fresh(cp, ttl_s)
    if cached is not None:
        log_cb(f"ohlcv cache_hit coin_id={coin_id}")
        return cached
    req = urllib.request.Request(url, headers=headers, method="GET")
    max_retries = CG_RATE_LIMIT["max_retries_per_symbol"]
    backoff_seq = CG_RATE_LIMIT["backoff_sequence_s"]
    _429_hits = 0
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            save_cache(cp, data)
            log_cb(f"ohlcv fetched coin_id={coin_id} candles={len(data)}")
            return data
        except Exception as exc:
            msg = str(exc)
            if "429" in msg or "Too Many" in msg:
                _429_hits += 1
                cooldown = (
                    CG_RATE_LIMIT["first_429_cooldown_s"] if _429_hits == 1
                    else CG_RATE_LIMIT["repeat_429_cooldown_s"]
                )
                log_cb(f"ohlcv 429 coin_id={coin_id} attempt={attempt} sleep={cooldown}s")
                _time.sleep(cooldown)
                if attempt >= max_retries:
                    raise RateLimitError(f"CoinGecko OHLCV 429 exhausted: {coin_id}") from exc
            else:
                sleep_s = backoff_seq[min(attempt - 1, len(backoff_seq) - 1)]
                log_cb(f"ohlcv error coin_id={coin_id} attempt={attempt} sleep={sleep_s}s err={msg[:60]}")
                _time.sleep(float(sleep_s))
                if attempt >= max_retries:
                    raise
    return []


def normalise_ohlcv_rows(
    raw_candles: list,
    run_id: int,
    snapshot_key: str,
    unified_symbol: str,
    timeframe: str,
    exchange: str = "coingecko",
) -> list:
    """DEPRECATED -- use normalise_ohlcv_cascade() instead.
    Retained as thin wrapper for backward compat. Task 15.
    Maps raw CoinGecko OHLC candles [[ts_ms,o,h,l,c], ...] to canonical cascade dicts
    then delegates to normalise_ohlcv_cascade().
    """
    import datetime as _dt
    canonical = []
    for c in raw_candles:
        if len(c) < 5:
            continue
        ts_ms = int(c[0])
        canonical.append({
            "ts":       ts_ms,
            "open":     float(c[1]),
            "high":     float(c[2]),
            "low":      float(c[3]),
            "close":    float(c[4]),
            "volume":   None,
            "_source":  exchange,
        })
    return normalise_ohlcv_cascade(canonical, run_id, snapshot_key, unified_symbol, timeframe)


def insert_ohlcv_snapshots(con, rows: list) -> None:
    """Insert normalised OHLCV rows into ohlcv_snapshots. Skips duplicates."""
    con.executemany(
        """INSERT OR IGNORE INTO ohlcv_snapshots
           (run_id, snapshot_key, exchange, unified_symbol, timeframe,
            open_time, open, high, low, close, volume)
           VALUES (?,?,?,?,?,?,?,?,?,?,?);""",
        rows,
    )


def insert_raw_ohlcv(con, run_id: int, coin_id: str, raw_data: list,
                     fetched_utc: str, snapshot_key: str,
                     source: str = "coingecko",
                     endpoint: str = None) -> None:
    """
    Store raw OHLCV response in raw_snapshots table.
    source: actual data source (coingecko / cryptocompare / binance).
            Used for provenance audit per ohlcv_data_rules_cz.md sect 4.1.
    endpoint: API endpoint used. Defaults to source-specific standard.
    """
    import json
    _ENDPOINTS = {
        "coingecko":     "/coins/{id}/ohlc",
        "cryptocompare": "/data/v2/histoday",
        "binance":       "/api/v3/klines",
    }
    ep = endpoint or _ENDPOINTS.get(source, "/unknown")
    insert_raw_snapshot(
        con, run_id, source, ep,
        json.dumps({"coin_id": coin_id, "source": source}),
        json.dumps(raw_data),
        fetched_utc,
        snapshot_key=snapshot_key,
    )

# ---- db ----
"""DB layer -- canonical schema, append-only, run_id keyed."""

import json
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple



# Connection

def db_connect(db_path: str) -> sqlite3.Connection:
    import os
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA foreign_keys=ON;")
    return con


# Schema helpers

def table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in con.execute(f"PRAGMA table_info({table});").fetchall()]


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,)
    ).fetchone() is not None


def ensure_column(con: sqlite3.Connection, table: str, col_def: str) -> None:
    """ADD COLUMN if not present. col_def e.g. 'finished_utc TEXT'."""
    col = col_def.split()[0]
    if col not in table_columns(con, table):
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col_def};")


# ensure_schema  (single entry point -- idempotent)

def ensure_schema(con: sqlite3.Connection) -> None:
    """
    Create all tables and run all migrations.
    Safe to call on every startup -- fully idempotent.
    """

    # ---- runs ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            created_utc  TEXT    NOT NULL,
            app_version  TEXT    NOT NULL DEFAULT '',
            scope        TEXT    NOT NULL DEFAULT '',
            vs_currency  TEXT,
            coins_limit  INTEGER,
            sort_mode    TEXT,
            filtry_json  TEXT,
            timeframes_json   TEXT,
            sources_used_json TEXT,
            status       TEXT    NOT NULL DEFAULT 'created',
            finished_utc TEXT,
            error_message TEXT
        );
    """)
    for col in ["app_version TEXT", "scope TEXT", "vs_currency TEXT",
                "coins_limit INTEGER", "sort_mode TEXT", "filtry_json TEXT",
                "timeframes_json TEXT", "sources_used_json TEXT",
                "finished_utc TEXT", "error_message TEXT", "params_json TEXT"]:
        ensure_column(con, "runs", col)

    # NOTE: watchlist exit column migrations moved to after watchlist CREATE below

    # ---- run_scopes ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS run_scopes (
            run_id  INTEGER NOT NULL,
            scope   TEXT    NOT NULL,
            PRIMARY KEY (run_id, scope),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- snapshots ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_ref        INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_key        TEXT    UNIQUE NOT NULL,
            run_id              INTEGER NOT NULL,
            created_utc         TEXT    NOT NULL,
            source              TEXT    NOT NULL,
            scope               TEXT,
            timeframe           TEXT,
            vs_currency         TEXT,
            coins_limit         INTEGER,
            sort_mode           TEXT,
            meta_json           TEXT,
            parent_snapshot_key TEXT,
            refresh_reason      TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)
    for col in ["vs_currency TEXT", "coins_limit INTEGER", "sort_mode TEXT",
                "parent_snapshot_key TEXT", "refresh_reason TEXT", "meta_json TEXT"]:
        ensure_column(con, "snapshots", col)

    # ---- market_snapshots ----
    # snapshot_id = snapshot_key (TEXT).  UNIQUE prevents duplicates on re-run.
    con.execute("""
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id     TEXT    NOT NULL,
            snapshot_ref    INTEGER,
            run_id          INTEGER NOT NULL,
            timestamp_utc   TEXT,
            scope           TEXT,
            timeframe       TEXT    NOT NULL DEFAULT 'spot',
            unified_symbol  TEXT    NOT NULL,
            pair            TEXT,
            price           REAL,
            change_24h_pct  REAL,
            vol24           REAL,
            mcap            REAL,
            rank            INTEGER,
            base_score      REAL,
            source          TEXT,
            UNIQUE (snapshot_id, unified_symbol, timeframe)
        );
    """)
    for col in ["snapshot_ref INTEGER", "scope TEXT", "pair TEXT", "base_score REAL"]:
        ensure_column(con, "market_snapshots", col)

    # ---- raw_snapshots ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_snapshots (
            raw_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id        INTEGER NOT NULL,
            source        TEXT    NOT NULL,
            endpoint      TEXT,
            query_json    TEXT,
            response_json TEXT,
            fetched_utc   TEXT,
            snapshot_key  TEXT,
            snapshot_ref  INTEGER,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)
    for col in ["snapshot_key TEXT", "snapshot_ref INTEGER"]:
        ensure_column(con, "raw_snapshots", col)
    # Task 6: immutability triggers -- raw data is the source of truth (spec 1.4)
    # BEFORE UPDATE/DELETE on raw_snapshots raises an error, enforcing append-only
    con.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_raw_snapshots_no_update
        BEFORE UPDATE ON raw_snapshots
        BEGIN
            SELECT RAISE(ABORT,
                'raw_snapshots is immutable: UPDATE not allowed (spec 1.4 append-only)');
        END;
    """)
    con.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_raw_snapshots_no_delete
        BEFORE DELETE ON raw_snapshots
        BEGIN
            SELECT RAISE(ABORT,
                'raw_snapshots is immutable: DELETE not allowed (use retention policy)');
        END;
    """)

    # ---- ohlcv_snapshots ----
    # Task 16: data_source = API provider (coingecko/cryptocompare/binance)
    #          exchange = actual trading venue (Binance spot, Coinbase, etc.) -- future use
    #          'exchange' column retained for backward compat, populated with data_source value
    con.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv_snapshots (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id         INTEGER NOT NULL,
            snapshot_key   TEXT    NOT NULL,
            exchange       TEXT    NOT NULL DEFAULT 'coingecko',
            unified_symbol TEXT    NOT NULL,
            timeframe      TEXT    NOT NULL,
            open_time      TEXT    NOT NULL,
            open           REAL,
            high           REAL,
            low            REAL,
            close          REAL,
            volume         REAL,
            data_source    TEXT,
            UNIQUE (run_id, unified_symbol, timeframe, open_time),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)
    # Migration: backfill data_source from exchange for rows that lack it
    ensure_column(con, "ohlcv_snapshots", "data_source TEXT")
    con.execute(
        "UPDATE ohlcv_snapshots SET data_source=exchange WHERE data_source IS NULL;"
    )
    # Task 16: market_snapshots provenance
    ensure_column(con, "market_snapshots", "data_source TEXT DEFAULT 'coingecko'")

    # ---- topnow_selection ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS topnow_selection (
            selection_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id       INTEGER,
            snapshot_id  TEXT,
            snapshot_ref INTEGER,
            params_json  TEXT,
            created_utc  TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)
    for col in ["snapshot_ref INTEGER", "params_json TEXT", "created_utc TEXT"]:
        ensure_column(con, "topnow_selection", col)

    # ---- topnow_selection_items ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS topnow_selection_items (
            selection_id      INTEGER NOT NULL,
            unified_symbol    TEXT    NOT NULL,
            rank_in_selection INTEGER,
            composite_preview REAL,
            reason_json       TEXT,
            PRIMARY KEY (selection_id, unified_symbol),
            FOREIGN KEY (selection_id) REFERENCES topnow_selection(selection_id)
        );
    """)
    for col in ["composite_preview REAL", "reason_json TEXT"]:
        ensure_column(con, "topnow_selection_items", col)

    # ---- watchlist ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            watch_id             INTEGER PRIMARY KEY AUTOINCREMENT,
            unified_symbol       TEXT    NOT NULL UNIQUE,
            tag                  TEXT    DEFAULT '',
            stage                TEXT    DEFAULT 'new',
            tracking_since_utc   TEXT    NOT NULL,
            entry_snapshot_id    TEXT    DEFAULT '',
            entry_score          REAL    DEFAULT 0.0,
            exit_timestamp_utc   TEXT,
            exit_reason          TEXT
        );
    """)
    # Migrations for watchlist exit columns (were pre-CREATE above -- moved here)
    ensure_column(con, "watchlist", "exit_timestamp_utc TEXT")
    ensure_column(con, "watchlist", "exit_reason TEXT")

    # ---- watchlist_metrics ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_metrics (
            watch_id          INTEGER PRIMARY KEY,
            last_refreshed_utc TEXT,
            last_price        REAL,
            last_chg24_pct    REAL,
            last_score        REAL,
            FOREIGN KEY (watch_id) REFERENCES watchlist(watch_id) ON DELETE CASCADE
        );
    """)

    # ---- watchlist_alerts ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_alerts (
            alert_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            watch_id   INTEGER NOT NULL,
            created_utc TEXT   NOT NULL,
            severity   TEXT    NOT NULL,
            message    TEXT    NOT NULL,
            FOREIGN KEY (watch_id) REFERENCES watchlist(watch_id) ON DELETE CASCADE
        );
    """)

    # ---- layer_results ----
    # Task 13: extended -- symbol_status per-symbol, provenance_json for audit
    con.execute("""
        CREATE TABLE IF NOT EXISTS layer_results (
            result_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER,
            snapshot_id     TEXT,
            snapshot_ref    INTEGER,
            unified_symbol  TEXT,
            layer_name      TEXT    NOT NULL DEFAULT 'unknown',
            timeframe       TEXT,
            layer_score     REAL,
            layer_status    TEXT    NOT NULL DEFAULT 'unknown',
            symbol_status   TEXT,
            confidence      REAL,
            raw_data_json   TEXT,
            provenance_json TEXT,
            created_utc     TEXT
        );
    """)
    try:
        con.execute("UPDATE layer_results SET layer_status='unknown' WHERE layer_status IS NULL OR layer_status='';")
        con.execute("UPDATE layer_results SET layer_name='unknown'   WHERE layer_name   IS NULL OR layer_name='';")
    except Exception:
        pass
    for col in ["run_id INTEGER", "snapshot_id TEXT", "snapshot_ref INTEGER",
                "unified_symbol TEXT", "layer_name TEXT", "timeframe TEXT",
                "layer_score REAL", "layer_status TEXT", "symbol_status TEXT",
                "confidence REAL", "raw_data_json TEXT", "provenance_json TEXT",
                "created_utc TEXT"]:
        ensure_column(con, "layer_results", col)

    # ---- state_log ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS state_log (
            event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id       INTEGER,
            from_status  TEXT,
            to_status    TEXT,
            timestamp_utc TEXT   NOT NULL,
            message      TEXT,
            severity     TEXT    DEFAULT 'info'
        );
    """)

    # ---- version_registry ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS version_registry (
            version     TEXT PRIMARY KEY,
            sha256      TEXT NOT NULL DEFAULT '',
            created_utc TEXT NOT NULL,
            notes       TEXT DEFAULT ''
        );
    """)

    # ---- macro_snapshots (v5.0a) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS macro_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id      INTEGER NOT NULL,
            source      TEXT    NOT NULL,
            series_id   TEXT    NOT NULL,
            series_name TEXT    NOT NULL DEFAULT '',
            value       REAL,
            value_date  TEXT,
            fetched_utc TEXT    NOT NULL,
            raw_json    TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- sentiment_ingestion (v5.0a) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS sentiment_ingestion (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            source          TEXT    NOT NULL,
            metric_key      TEXT    NOT NULL,
            value           REAL,
            classification  TEXT    DEFAULT '',
            fetched_utc     TEXT    NOT NULL,
            raw_json        TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- fundamental_snapshots (v5.0a) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS fundamental_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            unified_symbol  TEXT    NOT NULL,
            coin_id         TEXT    NOT NULL DEFAULT '',
            source          TEXT    NOT NULL,
            metric_key      TEXT    NOT NULL,
            num_value       REAL,
            text_value      TEXT    DEFAULT '',
            fetched_utc     TEXT    NOT NULL,
            raw_json        TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- onchain_snapshots (v5.0a) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS onchain_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            unified_symbol  TEXT    NOT NULL DEFAULT 'BTC',
            source          TEXT    NOT NULL,
            metric_key      TEXT    NOT NULL,
            value           REAL,
            fetched_utc     TEXT    NOT NULL,
            raw_json        TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- migrations for new tables NULL safety ----
    try:
        con.execute("UPDATE macro_snapshots SET source='unknown' WHERE source IS NULL OR source='';")
        con.execute("UPDATE sentiment_ingestion SET source='unknown' WHERE source IS NULL OR source='';")
    except Exception:
        pass

    # ---- cascade provenance migrations (v5.1a) ----
    # Add data_source column to ohlcv_snapshots for cascade provenance
    try:
        con.execute(
            "ALTER TABLE ohlcv_snapshots ADD COLUMN data_source TEXT DEFAULT 'coingecko';"
        )
    except Exception:
        pass  # column already exists

    # cascade_log: records which sources were tried/exhausted per run per layer
    con.execute("""
        CREATE TABLE IF NOT EXISTS cascade_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id      INTEGER NOT NULL,
            layer       TEXT    NOT NULL,
            source      TEXT    NOT NULL,
            symbol      TEXT    DEFAULT '',
            status      TEXT    NOT NULL DEFAULT 'ok',
            detail      TEXT    DEFAULT '',
            logged_utc  TEXT    NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- feature_vectors (v6.3a, spec 6.5) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS feature_vectors (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            selection_id    INTEGER NOT NULL,
            unified_symbol  TEXT    NOT NULL,
            feature_json    TEXT    NOT NULL,
            norm_score      REAL,
            created_utc     TEXT    NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- predictions (v6.3a, spec 6.5) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            selection_id    INTEGER NOT NULL,
            unified_symbol  TEXT    NOT NULL,
            signal          TEXT    NOT NULL,
            confidence      REAL,
            reasoning_json  TEXT,
            created_utc     TEXT    NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- trade_plans (v6.3a, spec 6.5) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS trade_plans (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            selection_id    INTEGER NOT NULL,
            unified_symbol  TEXT    NOT NULL,
            direction       TEXT    NOT NULL,
            entry_zone_low  REAL,
            entry_zone_high REAL,
            stop_loss       REAL,
            target_1        REAL,
            target_2        REAL,
            position_pct    REAL,
            risk_score      REAL,
            plan_json       TEXT,
            created_utc     TEXT    NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- trade_plan_alerts (v7.2a) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS trade_plan_alerts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            plan_id         INTEGER,
            unified_symbol  TEXT    NOT NULL,
            alert_type      TEXT    NOT NULL,
            message         TEXT,
            current_price   REAL,
            trigger_price   REAL,
            severity        TEXT    NOT NULL DEFAULT 'info',
            acknowledged    INTEGER NOT NULL DEFAULT 0,
            created_utc     TEXT    NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- prediction_performance (v7.2a) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS prediction_performance (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            unified_symbol  TEXT    NOT NULL,
            signal          TEXT    NOT NULL,
            confidence      REAL,
            price_at_prediction REAL,
            price_at_eval   REAL,
            pnl_pct         REAL,
            outcome         TEXT,
            eval_utc        TEXT    NOT NULL,
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)

    # ---- portfolio_positions (v7.5a) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_positions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            unified_symbol  TEXT    NOT NULL,
            direction       TEXT    NOT NULL DEFAULT 'long',
            entry_price     REAL    NOT NULL,
            entry_size      REAL    NOT NULL DEFAULT 1.0,
            stop_loss       REAL,
            target_price    REAL,
            status          TEXT    NOT NULL DEFAULT 'open',
            exit_price      REAL,
            pnl_pct         REAL,
            pnl_abs         REAL,
            source_run_id   INTEGER,
            source_plan_id  INTEGER,
            notes           TEXT,
            opened_utc      TEXT    NOT NULL,
            closed_utc      TEXT,
            scope           TEXT    DEFAULT 'crypto_spot'
        );
    """)

    # ---- config_profiles (v7.5a) ----
    con.execute("""
        CREATE TABLE IF NOT EXISTS config_profiles (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_name    TEXT    NOT NULL UNIQUE,
            scope           TEXT    NOT NULL,
            config_json     TEXT    NOT NULL,
            created_utc     TEXT    NOT NULL,
            updated_utc     TEXT
        );
    """)

    _ensure_indexes(con)
    con.commit()


def _ensure_indexes(con: sqlite3.Connection) -> None:
    con.execute("CREATE INDEX IF NOT EXISTS idx_ms_snapshot ON market_snapshots(snapshot_id, timeframe);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_ms_symbol   ON market_snapshots(unified_symbol);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_ms_run      ON market_snapshots(run_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_snap_key    ON snapshots(snapshot_key);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_snap_run    ON snapshots(run_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tns_run     ON topnow_selection(run_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_tnsi_sel    ON topnow_selection_items(selection_id);")
    con.execute("CREATE INDEX IF NOT EXISTS idx_statelog    ON state_log(run_id);")


# Keep alias for backward compat with v3.9u GUI imports
def require_schema(con: sqlite3.Connection, *args, **kwargs) -> None:
    ensure_schema(con)


def ensure_indexes(con: sqlite3.Connection) -> None:
    _ensure_indexes(con)


# Runs

def create_run(
    con: sqlite3.Connection,
    created_utc: str,
    app_version: str,
    status: str,
    scope_text: str,
    vs_currency: str,
    coins_limit: int,
    sort_mode: str,
    timeframes: List[str],
    source: str,
    filtry: Optional[dict] = None,
) -> int:
    con.execute(
        """INSERT INTO runs
           (created_utc, app_version, scope, vs_currency, coins_limit, sort_mode,
            filtry_json, timeframes_json, sources_used_json, status)
           VALUES (?,?,?,?,?,?,?,?,?,?);""",
        (
            created_utc, app_version, scope_text, vs_currency, int(coins_limit), sort_mode,
            json.dumps(filtry or {}),
            json.dumps(timeframes), json.dumps([source]), status,
        ),
    )
    return con.execute("SELECT last_insert_rowid();").fetchone()[0]


def update_run_status(
    con: sqlite3.Connection,
    run_id: int,
    status: str,
    error_message: Optional[str] = None,
    timestamp_utc: Optional[str] = None,
) -> None:
    ts = timestamp_utc or utc_now_iso()
    if error_message:
        con.execute(
            "UPDATE runs SET status=?, finished_utc=?, error_message=? WHERE run_id=?;",
            (status, ts, error_message, run_id),
        )
    else:
        con.execute(
            "UPDATE runs SET status=?, finished_utc=? WHERE run_id=?;",
            (status, ts, run_id),
        )


def upsert_run_scope(con: sqlite3.Connection, run_id: int, scope_text: str) -> None:
    con.execute(
        "INSERT OR IGNORE INTO run_scopes(run_id, scope) VALUES (?,?);",
        (run_id, scope_text),
    )


def set_run_status(con: sqlite3.Connection, run_id: int, status: str) -> None:
    con.execute("UPDATE runs SET status=? WHERE run_id=?;", (status, run_id))


# Snapshots

def create_snapshot(
    con: sqlite3.Connection,
    snapshot_key: str,
    run_id: int,
    created_utc: str,
    source: str,
    scope: str,
    timeframe: str,
    refresh_reason: str,
    parent_snapshot_key: Optional[str],
    status: str,
    vs_currency: str,
    coins_limit: int,
    sort_mode: str,
) -> int:
    meta = {"status": status}
    con.execute(
        """INSERT INTO snapshots
           (snapshot_key, run_id, created_utc, source, scope, timeframe,
            vs_currency, coins_limit, sort_mode, meta_json, parent_snapshot_key, refresh_reason)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?);""",
        (
            snapshot_key, run_id, created_utc, source, scope, timeframe,
            vs_currency, int(coins_limit), sort_mode,
            json.dumps(meta), parent_snapshot_key, refresh_reason,
        ),
    )
    return con.execute("SELECT last_insert_rowid();").fetchone()[0]


def update_snapshot_status(
    con: sqlite3.Connection,
    snapshot_key: str,
    status: str,
    error_message: Optional[str] = None,
    timestamp_utc: Optional[str] = None,
) -> None:
    """Update snapshot meta_json status. snapshot_key is TEXT (e.g. snap_20260219T...)."""
    ts = timestamp_utc or utc_now_iso()
    row = con.execute(
        "SELECT meta_json FROM snapshots WHERE snapshot_key=?;", (snapshot_key,)
    ).fetchone()
    if row is None:
        return
    try:
        meta = json.loads(row[0]) if row[0] else {}
    except Exception:
        meta = {}
    meta["status"] = status
    meta["status_ts_utc"] = ts
    if error_message:
        meta["error_message"] = error_message
    con.execute(
        "UPDATE snapshots SET meta_json=? WHERE snapshot_key=?;",
        (json.dumps(meta, ensure_ascii=True), snapshot_key),
    )


def get_snapshot_ref(con: sqlite3.Connection, snapshot_key: str) -> Optional[int]:
    row = con.execute(
        "SELECT snapshot_ref FROM snapshots WHERE snapshot_key=?;", (snapshot_key,)
    ).fetchone()
    return int(row[0]) if row else None


def resolve_snapshot(
    con: sqlite3.Connection, identifier: str | int
) -> Tuple[Optional[int], Optional[str]]:
    """Return (snapshot_ref, snapshot_key) for any identifier (key string or ref int)."""
    if isinstance(identifier, int):
        row = con.execute(
            "SELECT snapshot_ref, snapshot_key FROM snapshots WHERE snapshot_ref=?;",
            (identifier,),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT snapshot_ref, snapshot_key FROM snapshots WHERE snapshot_key=?;",
            (str(identifier),),
        ).fetchone()
    if not row:
        return None, str(identifier) if isinstance(identifier, str) else None
    return int(row[0]), str(row[1])


# Raw snapshots

def insert_raw_snapshot(
    con: sqlite3.Connection,
    run_id: int,
    source: str,
    endpoint: str,
    query_json: str,
    response_json: str,
    fetched_utc: str,
    snapshot_key: Optional[str] = None,
    snapshot_ref: Optional[int] = None,
) -> None:
    con.execute(
        """INSERT INTO raw_snapshots
           (run_id, source, endpoint, query_json, response_json, fetched_utc,
            snapshot_key, snapshot_ref)
           VALUES (?,?,?,?,?,?,?,?);""",
        (run_id, source, endpoint, query_json, response_json, fetched_utc,
         snapshot_key, snapshot_ref),
    )


# Market snapshots

def insert_market_snapshots(con: sqlite3.Connection, rows: Iterable[Tuple]) -> None:
    """Insert normalised market rows."""
    con.executemany(
        """INSERT OR IGNORE INTO market_snapshots
           (snapshot_id, snapshot_ref, run_id, timestamp_utc, scope, timeframe,
            unified_symbol, pair, price, change_24h_pct, vol24, mcap, rank, base_score, source)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",
        rows,
    )


# TopNow selection

def select_candidates(
    con: sqlite3.Connection,
    snapshot_key: str,
    timeframe: str,
    limit_n: int,
) -> List[Tuple]:
    """Return top N candidates from market_snapshots ordered by rank ASC."""
    return con.execute(
        """SELECT unified_symbol, rank, mcap, vol24, change_24h_pct, base_score
           FROM market_snapshots
           WHERE snapshot_id=? AND timeframe=?
           ORDER BY rank ASC NULLS LAST
           LIMIT ?;""",
        (snapshot_key, timeframe, int(limit_n)),
    ).fetchall()


def create_topnow_selection(
    con: sqlite3.Connection,
    run_id: int,
    snapshot_key: str,
    params: Dict[str, Any],
) -> int:
    snap_ref = get_snapshot_ref(con, snapshot_key)
    con.execute(
        """INSERT INTO topnow_selection
           (run_id, snapshot_id, snapshot_ref, params_json, created_utc)
           VALUES (?,?,?,?,?);""",
        (run_id, snapshot_key, snap_ref,
         json.dumps(params, sort_keys=True), utc_now_iso()),
    )
    return con.execute("SELECT last_insert_rowid();").fetchone()[0]


def insert_topnow_items(
    con: sqlite3.Connection, selection_id: int, candidates: List[Tuple]
) -> int:
    items = [
        (selection_id, row[0], i + 1, None)
        for i, row in enumerate(candidates)
    ]
    con.executemany(
        """INSERT OR REPLACE INTO topnow_selection_items
           (selection_id, unified_symbol, rank_in_selection, composite_preview)
           VALUES (?,?,?,?);""",
        items,
    )
    return len(items)


# State log

def state_log(
    con: sqlite3.Connection,
    run_id: Optional[int],
    to_status: str,
    from_status: Optional[str] = None,
    message: Optional[str] = None,
    severity: str = "info",
) -> None:
    con.execute(
        """INSERT INTO state_log
           (run_id, from_status, to_status, timestamp_utc, message, severity)
           VALUES (?,?,?,?,?,?);""",
        (run_id, from_status, to_status, utc_now_iso(), message or "", severity),
    )


# Version registry

def upsert_version_registry(
    con: sqlite3.Connection,
    version: str,
    sha256: str,
    created_utc: str,
    notes: str = "",
) -> None:
    con.execute(
        """INSERT OR IGNORE INTO version_registry(version, sha256, created_utc, notes)
           VALUES (?,?,?,?);""",
        (version, sha256, created_utc, notes),
    )


def compute_self_sha256() -> str:
    """Compute SHA256 of the running script file. Task 2 -- version_registry audit."""
    import hashlib as _hl
    try:
        with open(__file__, "rb") as _f:
            return _hl.sha256(_f.read()).hexdigest()
    except Exception:
        return "unknown"


# Layer results

def upsert_layer_result(
    con: sqlite3.Connection,
    run_id: int,
    snapshot_key: str,
    snapshot_ref: Optional[int],
    unified_symbol: Optional[str],
    layer_name: str,
    timeframe: str,
    layer_score: Optional[float],
    layer_status: str,
    raw_data_json: Optional[str] = None,
    confidence: Optional[float] = None,
) -> None:
    # Guard: layer_status must never be None (NOT NULL constraint)
    safe_status = str(layer_status).strip() if layer_status else "unknown"
    safe_name   = str(layer_name).strip()   if layer_name   else "unknown"
    if not safe_status:
        safe_status = "unknown"
    if not safe_name:
        safe_name = "unknown"
    # Use INSERT OR IGNORE to avoid duplicate key errors on re-run
    con.execute(
        """INSERT OR IGNORE INTO layer_results
           (run_id, snapshot_id, snapshot_ref, unified_symbol, layer_name, timeframe,
            layer_score, layer_status, confidence, raw_data_json, created_utc)
           VALUES (?,?,?,?,?,?,?,?,?,?,?);""",
        (run_id, snapshot_key, snapshot_ref, unified_symbol, safe_name, timeframe,
         layer_score, safe_status, confidence, raw_data_json, utc_now_iso()),
    )


# ===========================================================================
# LIVE RUN HISTORY / PROFILE LOGGING (v7.5c)
# Integrated from the v7.5b fix without replacing the full v7.5a core.
# Writes simple JSON mirrors for dashboards or external monitors that do not
# read the main SQLite database directly.
# ===========================================================================

@dataclass
class RunSummary:
    """Compact run summary compatible with the v7.5b fix interface."""
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


def _nyosig_load_json_list(path: str) -> list:
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _nyosig_save_json_list(path: str, data: list) -> None:
    ensure_dir(os.path.dirname(path))
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=True)
    os.replace(tmp_path, path)


def _nyosig_count_api_records(con, run_id: int) -> int:
    try:
        row = con.execute(
            "SELECT COUNT(*) FROM raw_snapshots WHERE run_id=?;",
            (run_id,)
        ).fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _nyosig_record_run_profile(
    paths: Paths,
    run_id: Any,
    app_version: str,
    scope: str,
    status: str,
    started_utc: str,
    duration_s: float,
    candidates_n: int,
    api_calls: int,
    errors: int,
    message: str = "",
    snapshot_id: str = "",
    selection_id: Any = None,
) -> RunSummary:
    """Append run metadata to run_history.json and run_profiles.json."""
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

    history = _nyosig_load_json_list(history_path)
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
    _nyosig_save_json_list(history_path, history[-500:])

    profiles = _nyosig_load_json_list(profiles_path)
    profile = summary.to_dict()
    profile.update({
        "version": app_version,
        "scope": scope,
        "snapshot_id": snapshot_id,
        "selection_id": selection_id,
    })
    profiles.append(profile)
    _nyosig_save_json_list(profiles_path, profiles[-500:])

    return summary


def run_pipeline(
    scope: str = "crypto_spot",
    vs_currency: str = "usd",
    limit: int = 250,
    top_n: int = 15,
    history_dir: str | None = None,
) -> Tuple[Any, RunSummary]:
    """
    v7.5b-compatible entry point backed by the full v7.5c engine.
    This does not use the simplified v7.5b-only pipeline. It runs the
    complete snapshot + TopNow workflow and returns a compact summary.
    """
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
            api_calls = _nyosig_count_api_records(con, res.run_id)
        finally:
            con.close()
        summary = _nyosig_record_run_profile(
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
        summary = _nyosig_record_run_profile(
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


# ---- pipeline ----
"""
core_v4_0a/pipeline.py
NyoSig_Analysator v4.0a  --  Pipeline: Snapshot + SpotBasic + TopNow.
"""

import json
import os
from dataclasses import dataclass
from typing import Callable, List, Optional


SOURCE = "coingecko"
ENDPOINT = "/coins/markets"


@dataclass
class PipelineResult:
    run_id: int
    snapshot_id: str      # snapshot_key TEXT
    selection_id: int
    candidates_n: int


def _new_snapshot_key() -> str:
    import uuid
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"snap_{ts}_{uuid.uuid4().hex[:8]}"


def _load_offline_sample(samples_dir: str) -> list:
    p = os.path.join(samples_dir, "coingecko_markets_sample_v1.0a.json")
    if not os.path.isfile(p):
        # also try parent/samples
        p2 = os.path.join(os.path.dirname(samples_dir), "samples",
                          "coingecko_markets_sample_v1.0a.json")
        if os.path.isfile(p2):
            p = p2
        else:
            raise FileNotFoundError(f"Offline sample not found: {p}")
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def _spot_basic_run(
    con, snapshot_key: str, timeframe: str, log_cb: Callable[[str], None]
) -> int:
    """Compute SpotBasic score for all rows in snapshot. Returns rows scored."""
    rows = con.execute(
        "SELECT unified_symbol, rank, mcap, vol24, change_24h_pct "
        "FROM market_snapshots WHERE snapshot_id=? AND timeframe=?;",
        (snapshot_key, timeframe),
    ).fetchall()
    scored = [
        (float(spot_basic_score(r[1], r[2], r[3], r[4])), snapshot_key, timeframe, r[0])
        for r in rows
    ]
    con.executemany(
        "UPDATE market_snapshots SET base_score=? "
        "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
        scored,
    )
    log_cb(f"SpotBasic scored {len(scored)} rows")
    return len(scored)


def _ohlcv_ingest(con, run_id, snap_key, markets, vs_currency, paths, log_cb,
                  timeframes=None):
    """
    Fetch and store OHLCV for top 15 candidates using SourceCascade.
    Spec 7.2: CoinGecko (primary) -> CryptoCompare (fallback) -> Binance klines.
    v6.2a: Multi-timeframe support. timeframes list defaults to ["1d"].
    Each symbol records which source delivered its data (_source provenance).
    """
    import time as _time, json as _json
    top15 = sorted(
        [m for m in markets if m.get("id") and m.get("market_cap_rank")],
        key=lambda x: int(x.get("market_cap_rank") or 9999)
    )[:15]

    if timeframes is None:
        timeframes = ["1d"]

    # Timeframe -> days mapping for CoinGecko/cascade
    TF_DAYS_MAP = {"15m": 1, "1h": 7, "4h": 14, "1d": 30}

    total_ok = 0
    total_fail = 0

    for ohlcv_tf in timeframes:
        days = TF_DAYS_MAP.get(ohlcv_tf, 30)
        ohlcv_casc = make_ohlcv_cascade(log_cb=log_cb)
        ok_count = 0
        fail_count = 0

        log_cb(f"OHLCV ingestion: tf={ohlcv_tf} days={days} symbols={len(top15)}")

        for item in top15:
            coin_id = item.get("id", "")
            sym     = (item.get("symbol") or "").strip().upper()
            if not sym:
                continue
            try:
                raw_rows, src = ohlcv_casc.fetch_one(
                    sym, vs_currency=vs_currency, days=days, coin_id=coin_id)
                fetched_utc = utc_now_iso()
                if raw_rows:
                    insert_raw_ohlcv(
                        con, run_id, coin_id, raw_rows, fetched_utc, snap_key, source=src)
                    rows = normalise_ohlcv_cascade(raw_rows, run_id, snap_key, sym, ohlcv_tf)
                    insert_ohlcv_snapshots(con, rows)
                    ok_count += 1
                    log_cb(f"OHLCV OK [{src}]: {sym} tf={ohlcv_tf} candles={len(rows)}")
                _time.sleep(CG_RATE_LIMIT["safe_interval_s"] / 4.0)
            except Exception as exc:
                fail_count += 1
                log_cb(f"OHLCV FAILED {sym} tf={ohlcv_tf}: {str(exc)[:80]}")
                _time.sleep(1.5)

        total_ok += ok_count
        total_fail += fail_count
        log_cb(f"OHLCV tf={ohlcv_tf} done: ok={ok_count} failed={fail_count}")

    con.commit()
    log_cb(f"OHLCV multi-tf done: total_ok={total_ok} total_fail={total_fail}")


def _topnow_build(
    con,
    run_id: int,
    snapshot_key: str,
    timeframe: str,
    top_n: int,
    log_cb: Callable[[str], None],
) -> tuple[int, int]:
    """Build TopNow selection. Returns (selection_id, n_items)."""
    log_cb("TOP selection: building...")
    candidates = select_candidates(con, snapshot_key, timeframe, top_n)
    params = {"limit": top_n, "timeframe": timeframe, "order": "rank_asc"}
    selection_id = create_topnow_selection(con, run_id, snapshot_key, params)
    n = insert_topnow_items(con, selection_id, candidates)
    log_cb(f"TOP selection built: selection_id={selection_id} items={n}")
    return selection_id, n


def run_snapshot_and_topnow(
    project_root: str,
    app_version: str,
    scope_text: str,
    vs_currency: str,
    coins_limit: int,
    order: str,
    offline_mode: bool,
    log_cb: Callable[[str], None],
    topnow_limit: int = 100,
    timeframe: str = "spot",
    parent_snapshot_key: str = "",
) -> PipelineResult:
    """
    Full pipeline: create run -> fetch markets -> normalise -> SpotBasic -> TopNow.
    Returns PipelineResult.
    """
    _profile_started_utc = utc_now_iso()
    _profile_t0 = time.time()

    paths = make_paths(project_root)
    ensure_dir(paths.cache_dir)
    ensure_dir(paths.db_dir)
    ensure_dir(paths.log_dir)

    cfg = parse_simple_yaml(paths.defaults_yaml)
    secrets = parse_secrets_env(paths.secrets_env)

    # Config values
    connect_timeout_s = int(cfg.get("network", {}).get("connect_timeout_s", 10))
    read_timeout_s    = int(cfg.get("network", {}).get("read_timeout_s", 30))
    max_attempts      = int(cfg.get("retry",   {}).get("max_attempts", 4))
    backoff_s         = cfg.get("retry",       {}).get("backoff_s", [1, 2, 4, 8])
    on_429_sleep_s    = int(cfg.get("rate_limit", {}).get("on_429_sleep_s", 20))
    ttl_s             = int(cfg.get("cache",   {}).get("ttl_s", 120))

    con = db_connect(paths.db_path)
    ensure_schema(con)

    created_utc = utc_now_iso()

    # Register this app version (idempotent) -- Task 2: real SHA256 of running file
    try:
        _sha = compute_self_sha256()
        upsert_version_registry(con, app_version, _sha, created_utc,
                                notes="auto-registered at run start")
    except Exception:
        pass

    run_id = create_run(
        con=con,
        created_utc=created_utc,
        app_version=app_version,
        status="created",
        scope_text=scope_text,
        vs_currency=vs_currency,
        coins_limit=coins_limit,
        sort_mode=order,
        timeframes=[timeframe],
        source=SOURCE,
    )
    upsert_run_scope(con, run_id, scope_text)
    state_log(con, run_id, "data_collecting", "created")
    con.commit()

    log_cb(f"scope={scope_text} vs_currency={vs_currency} coins_limit={coins_limit} "
           f"order={order} topnow={topnow_limit} offline={int(offline_mode)}")

    snap_key = _new_snapshot_key()
    create_snapshot(
        con=con,
        snapshot_key=snap_key,
        run_id=run_id,
        created_utc=created_utc,
        source=SOURCE,
        scope=scope_text,
        timeframe=timeframe,
        refresh_reason="auto_pipeline",
        parent_snapshot_key=parent_snapshot_key or None,
        status="running",
        vs_currency=vs_currency,
        coins_limit=coins_limit,
        sort_mode=order,
    )
    con.commit()

    try:
        # --- Data collection (v7.0a: scope-aware) ---
        scope_def = get_scope(scope_text)
        asset_class = scope_def.asset_class if scope_def else "crypto"

        if offline_mode:
            markets = _load_offline_sample(paths.samples_dir)
            log_cb(f"Offline mode: loaded {len(markets)} records from sample.")

        elif asset_class == "forex":
            # v7.0a: Forex -- Yahoo Finance FX pairs
            limit = min(coins_limit, len(FOREX_PAIRS))
            log_cb(f"Fetching Yahoo FX: pairs={limit}")
            markets = fetch_yahoo_universe(FOREX_PAIRS[:limit], log_cb=log_cb)

        elif asset_class == "stocks":
            # v7.0a: Stocks -- Yahoo Finance equities + ETFs
            limit = min(coins_limit, len(STOCKS_UNIVERSE))
            log_cb(f"Fetching Yahoo stocks: symbols={limit}")
            markets = fetch_yahoo_universe(STOCKS_UNIVERSE[:limit], log_cb=log_cb)

        elif asset_class == "macro":
            # v7.0a: Macro dashboard -- Yahoo Finance indices + commodities
            log_cb(f"Fetching Yahoo macro instruments: {len(MACRO_INSTRUMENTS)}")
            markets = fetch_yahoo_universe(MACRO_INSTRUMENTS, log_cb=log_cb)

        else:
            # Default: Crypto -- CoinGecko
            headers: dict = {}
            api_key = secrets.get("COINGECKO_API_KEY", "").strip()
            if api_key:
                headers["x-cg-pro-api-key"] = api_key
            log_cb(f"Fetching CoinGecko markets: vs={vs_currency} limit={coins_limit} "
                   f"order={order} tf={timeframe}")
            markets = fetch_markets(
                vs_currency, order, coins_limit, headers,
                paths.cache_dir, ttl_s,
                connect_timeout_s, read_timeout_s,
                max_attempts, backoff_s, on_429_sleep_s,
                log_cb,
            )

        fetched_utc = utc_now_iso()
        snap_ref = get_snapshot_ref(con, snap_key)

        # v7.0a: Scope-aware normalisation
        if asset_class in ("forex", "stocks", "macro"):
            source_name = "yahoo_finance"
            rows = normalise_non_crypto_rows(
                markets, run_id, snap_key, scope_text, timeframe, fetched_utc, source_name,
                snapshot_ref=snap_ref,
            )
        else:
            source_name = SOURCE
            rows = normalise_rows(
                markets, run_id, snap_key, scope_text, timeframe, fetched_utc, SOURCE,
                snapshot_ref=snap_ref,
            )

        insert_market_snapshots(con, rows)
        insert_raw_snapshot(
            con, run_id, source_name, scope_def.universe_endpoint if scope_def else ENDPOINT,
            json.dumps({"vs_currency": vs_currency, "order": order, "limit": coins_limit,
                        "scope": scope_text, "asset_class": asset_class}),
            json.dumps(markets),
            fetched_utc,
            snapshot_key=snap_key,
            snapshot_ref=snap_ref,
        )
        log_cb(f"Snapshot OK: {snap_key} rows={len(rows)}")
        state_log(con, run_id, "normalized", "data_collecting")
        con.commit()

        # --- SpotBasic scoring (v7.0a: scope-aware) ---
        if asset_class == "crypto":
            log_cb("Layer SpotBasic: scoring (crypto)...")
            _spot_basic_run(con, snap_key, timeframe, log_cb)
        elif asset_class == "forex":
            log_cb("Layer ForexSpot: scoring (forex)...")
            _fx_rows = con.execute(
                "SELECT unified_symbol, rank, price, change_24h_pct "
                "FROM market_snapshots WHERE snapshot_id=? AND timeframe=?;",
                (snap_key, timeframe)).fetchall()
            for sym, rnk, price, chg in _fx_rows:
                bs = score_forex_spot(rnk or 1, price or 0, chg or 0)
                con.execute("UPDATE market_snapshots SET base_score=? "
                    "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
                    (bs, snap_key, timeframe, sym))
            log_cb(f"ForexSpot scored {len(_fx_rows)} pairs")
        elif asset_class == "stocks":
            log_cb("Layer StocksSpot: scoring (equities)...")
            _eq_rows = con.execute(
                "SELECT unified_symbol, rank, price, mcap, change_24h_pct "
                "FROM market_snapshots WHERE snapshot_id=? AND timeframe=?;",
                (snap_key, timeframe)).fetchall()
            for sym, rnk, price, mcap, chg in _eq_rows:
                bs = score_stocks_spot(rnk or 1, price or 0, mcap or 0, chg or 0)
                con.execute("UPDATE market_snapshots SET base_score=? "
                    "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
                    (bs, snap_key, timeframe, sym))
            log_cb(f"StocksSpot scored {len(_eq_rows)} equities")
        else:
            log_cb("Layer Macro: scoring (informational)...")
            _macro_rows = con.execute(
                "SELECT unified_symbol FROM market_snapshots "
                "WHERE snapshot_id=? AND timeframe=?;", (snap_key, timeframe)).fetchall()
            for (sym,) in _macro_rows:
                con.execute("UPDATE market_snapshots SET base_score=50.0 "
                    "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
                    (snap_key, timeframe, sym))
            log_cb(f"MacroDashboard: {len(_macro_rows)} instruments set to neutral")
        con.commit()

        # --- OHLCV ingestion ---
        # v7.6c: Crypto OHLCV cascade is crypto-only.
        # Stocks/ETFs must not be sent to CoinGecko, CryptoCompare, or Binance.
        if asset_class == "crypto":
            ohlcv_timeframes = cfg.get("mvp", {}).get("timeframes", ["1d"])
            if isinstance(ohlcv_timeframes, str):
                ohlcv_timeframes = [ohlcv_timeframes]
            if "1d" not in ohlcv_timeframes:
                ohlcv_timeframes = ["1d"] + list(ohlcv_timeframes)
            _ohlcv_ingest(
                con=con, run_id=run_id, snap_key=snap_key,
                markets=markets, vs_currency=vs_currency,
                paths=paths, log_cb=log_cb,
                timeframes=ohlcv_timeframes,
            )
        elif asset_class == "stocks":
            log_cb("OHLCV ingestion skipped for stocks: Yahoo OHLCV provider is not implemented in this build")
        elif asset_class == "forex":
            log_cb("OHLCV ingestion skipped for forex: Yahoo OHLCV provider is not implemented in this build")

        # --- TopNow ---
        selection_id, n_items = _topnow_build(
            con, run_id, snap_key, timeframe, int(topnow_limit), log_cb
        )

        # --- Macro ingestion (all scopes benefit) ---
        log_cb("Layer Macro: ingesting SP500, DXY, VIX, US10Y, FEDFUNDS...")
        try:
            ingest_macro_layer(con, run_id, log_cb)
        except Exception as _exc:
            log_cb(f"macro ingestion ERROR: {str(_exc)[:120]}")

        # --- Sentiment ingestion (all scopes benefit) ---
        log_cb("Layer Sentiment: ingesting Fear & Greed...")
        try:
            ingest_sentiment_layer(con, run_id, log_cb)
        except Exception as _exc:
            log_cb(f"sentiment ingestion ERROR: {str(_exc)[:120]}")

        # --- Crypto-only layers (v7.0a: scope-guarded) ---
        if asset_class == "crypto":
            # --- OnChain ingestion (BTC blockchain.com) ---
            log_cb("Layer OnChain: ingesting BTC on-chain (blockchain.com)...")
            try:
                ingest_onchain_btc(con, run_id, log_cb)
            except Exception as _exc:
                log_cb(f"onchain BTC ingestion ERROR: {str(_exc)[:120]}")

            # --- OnChain ETH (Blockchair) ---
            log_cb("Layer OnChain: ingesting ETH on-chain (blockchair)...")
            try:
                ingest_onchain_eth(con, run_id, log_cb)
            except Exception as _exc:
                log_cb(f"onchain ETH ingestion ERROR: {str(_exc)[:120]}")

            # --- OnChain multi-asset (Blockchair) ---
            log_cb("Layer OnChain: ingesting multi-asset on-chain (blockchair)...")
            try:
                top_syms_oc = [r[0] for r in con.execute(
                    "SELECT unified_symbol FROM topnow_selection_items "
                    "WHERE selection_id=? ORDER BY rank_in_selection ASC LIMIT 15;",
                    (selection_id,)
                ).fetchall()]
                ingest_onchain_multi(con, run_id, top_syms_oc, log_cb)
            except Exception as _exc:
                log_cb(f"onchain multi ingestion ERROR: {str(_exc)[:120]}")

            # --- Institutional ingestion (CME + ETF) ---
            log_cb("Layer Institutional: ingesting CME futures + ETF data...")
            try:
                top_syms_inst = [r[0] for r in con.execute(
                    "SELECT unified_symbol FROM topnow_selection_items "
                    "WHERE selection_id=? ORDER BY rank_in_selection ASC LIMIT 15;",
                    (selection_id,)
                ).fetchall()]
                ingest_institutional_layer(con, run_id, top_syms_inst, log_cb)
            except Exception as _exc:
                log_cb(f"institutional ingestion ERROR: {str(_exc)[:120]}")

            # --- Fundamental ingestion (GitHub) ---
            log_cb("Layer Fundamental: ingesting GitHub dev metrics...")
            try:
                top_syms = [r[0] for r in con.execute(
                    "SELECT unified_symbol FROM topnow_selection_items "
                    "WHERE selection_id=? ORDER BY rank_in_selection ASC LIMIT 20;",
                    (selection_id,)
                ).fetchall()]
                ingest_fundamental_layer(con, run_id, top_syms, log_cb)
            except Exception as _exc:
                log_cb(f"fundamental ingestion ERROR: {str(_exc)[:120]}")
        else:
            log_cb(f"Scope {scope_text} ({asset_class}): crypto-specific layers skipped")

        update_snapshot_status(con, snap_key, "ok")
        update_run_status(con, run_id, "completed")
        state_log(con, run_id, "completed", "normalized")
        con.commit()
        try:
            _nyosig_record_run_profile(
                paths=paths,
                run_id=run_id,
                app_version=app_version,
                scope=scope_text,
                status="completed",
                started_utc=_profile_started_utc,
                duration_s=time.time() - _profile_t0,
                candidates_n=n_items,
                api_calls=_nyosig_count_api_records(con, run_id),
                errors=0,
                snapshot_id=snap_key,
                selection_id=selection_id,
            )
        except Exception as _profile_exc:
            log_cb("run profile write WARN: " + str(_profile_exc)[:120])

    except Exception as exc:
        try:
            update_snapshot_status(con, snap_key, "error", str(exc)[:500])
            update_run_status(con, run_id, "failed", str(exc)[:500])
            state_log(con, run_id, "failed", message=str(exc)[:500], severity="error")
            con.commit()
            try:
                _nyosig_record_run_profile(
                    paths=paths,
                    run_id=run_id,
                    app_version=app_version,
                    scope=scope_text,
                    status="failed",
                    started_utc=_profile_started_utc,
                    duration_s=time.time() - _profile_t0,
                    candidates_n=0,
                    api_calls=_nyosig_count_api_records(con, run_id),
                    errors=1,
                    message=str(exc)[:1000],
                    snapshot_id=snap_key,
                    selection_id=None,
                )
            except Exception:
                pass
        except Exception:
            pass
        log_cb("FAILED " + str(exc))
        con.close()
        raise

    con.close()

    return PipelineResult(
        run_id=run_id,
        snapshot_id=snap_key,
        selection_id=selection_id,
        candidates_n=n_items,
    )

# ---- analysis ----
"""
core_v4_0a/analysis.py
NyoSig_Analysator v4.0a  --  Analysis layer: SpotBasic + Composite preview.

Rules:
- snapshot_key (TEXT) is always the primary lookup key.
- timeframe default is 'spot' (matches pipeline default).
- composite_preview = weighted average of available layer scores.
- reason_json stores per-symbol breakdown for audit.
"""

import json
import sqlite3
from typing import Dict, List, Optional, Tuple



# SpotBasic

def compute_and_store_spot_basic(
    con: sqlite3.Connection,
    snapshot_key: str,
    timeframe: str = "spot",
) -> int:
    """
    Compute spot_basic_score for every row in market_snapshots matching
    (snapshot_id=snapshot_key, timeframe=timeframe).
    Writes result to base_score column.
    Returns number of rows updated.
    Task 4: state_log event wraps the UPDATE.
    """
    rows = con.execute(
        "SELECT unified_symbol, rank, mcap, vol24, change_24h_pct "
        "FROM market_snapshots WHERE snapshot_id=? AND timeframe=?;",
        (snapshot_key, timeframe),
    ).fetchall()
    if not rows:
        return 0
    scores = [(float(spot_basic_score(r[1], r[2], r[3], r[4])), snapshot_key, timeframe, r[0])
              for r in rows]
    con.executemany(
        "UPDATE market_snapshots SET base_score=? "
        "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
        scores,
    )
    # Task 4: audit event for enrichment UPDATE
    try:
        state_log(con, None, "spot_basic_scored",
                  from_status="raw",
                  message="snapshot=" + str(snapshot_key) + " rows=" + str(len(scores)),
                  severity="info")
    except Exception:
        pass
    return len(scores)


def apply_scores_to_selection(
    con: sqlite3.Connection,
    selection_id: int,
    snapshot_key: str,
    timeframe: str = "spot",
) -> int:
    """
    Copy base_score from market_snapshots into topnow_selection_items.composite_preview.
    Returns number of items updated.
    """
    rows = con.execute(
        """SELECT i.unified_symbol, m.base_score
           FROM topnow_selection_items i
           LEFT JOIN market_snapshots m
                ON m.snapshot_id=? AND m.timeframe=? AND m.unified_symbol=i.unified_symbol
           WHERE i.selection_id=?;""",
        (snapshot_key, timeframe, selection_id),
    ).fetchall()
    payload = [(float(sc) if sc is not None else 0.0, selection_id, sym) for sym, sc in rows]
    con.executemany(
        "UPDATE topnow_selection_items SET composite_preview=? "
        "WHERE selection_id=? AND unified_symbol=?;",
        payload,
    )
    # Task 4: audit event for composite_preview enrichment UPDATE
    try:
        state_log(con, None, "composite_preview_updated",
                  from_status="selection_built",
                  message="selection_id=" + str(selection_id) + " rows=" + str(len(payload)),
                  severity="info")
    except Exception:
        pass
    return len(payload)



# Ingestion connectors -- RAW data fetch + DB persist per run_id (v5.0a)
# Spec: each layer has primary free source, limits, fallback, paid upgrade path

# ---------- MACRO LAYER ----------
# Sources: Yahoo Finance unofficial (SP500, DXY, VIX, BTC -- no key, free)
#          FRED CSV endpoint (fed funds rate -- no key required)
# Fallback: cached last-known value
# Paid upgrade: Bloomberg API, Refinitiv

_YAHOO_SERIES = {
    "SP500":  "^GSPC",
    "DXY":    "DX-Y.NYB",
    "VIX":    "^VIX",
    "US10Y":  "^TNX",
    "NASDAQ": "^IXIC",
}

def _fetch_yahoo_quote(ticker_symbol, log_cb=None):
    """
    Fetch latest price for a Yahoo Finance ticker using unofficial chart API.
    Returns (price_float, date_str) or raises on error.
    Free, no key. Task 14: routed through RateLimitManager("yahoo").
    """
    rl = get_rate_limit_manager(log_cb)
    rl.acquire("yahoo")
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        + str(ticker_symbol)
        + "?interval=1d&range=5d"
    )
    data = _http_get_json(url, timeout=10)
    result = data["chart"]["result"][0]
    meta   = result["meta"]
    price  = float(meta.get("regularMarketPrice") or meta.get("previousClose") or 0.0)
    ts     = meta.get("regularMarketTime") or 0
    import datetime as _dt
    date_str = _dt.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") if ts else ""
    return price, date_str


def ingest_macro_layer(con, run_id, log_cb=None):
    """
    Ingest macro market data and persist to macro_snapshots with run_id.
    Sources: Yahoo Finance (SP500, DXY, VIX, US10Y, NASDAQ).
    Spec A3: RAW stored unchanged, normalised layer separate.
    Returns list of series ingested.
    """
    import json as _j, time as _t
    if log_cb is None:
        log_cb = lambda m: None
    fetched_utc = utc_now_iso()
    ingested = []

    for series_id, ticker in _YAHOO_SERIES.items():
        try:
            price, date_str = _fetch_yahoo_quote(ticker)
            raw = {"ticker": ticker, "price": price, "date": date_str}
            con.execute(
                "INSERT INTO macro_snapshots "
                "(run_id, source, series_id, series_name, value, value_date, fetched_utc, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
                (run_id, "yahoo_finance", series_id, ticker,
                 price, date_str, fetched_utc, _j.dumps(raw)),
            )
            ingested.append(series_id)
            log_cb(f"macro ingested: {series_id}={price:.2f} ({date_str})")
            _t.sleep(0.3)
        except Exception as exc:
            log_cb(f"macro SKIP {series_id}: {str(exc)[:80]}")

    # Fed Funds Rate from FRED CSV (no API key required)
    try:
        import urllib.request as _ur
        req = _ur.Request(
            "https://fred.stlouisfed.org/graph/fredgraph.csv?id=FEDFUNDS",
            headers={"User-Agent": "NyoSig-Analysator/7.5a"}
        )
        with _ur.urlopen(req, timeout=10) as r:
            lines = r.read().decode("utf-8").strip().splitlines()
        # Last non-empty data line
        last = [l for l in lines[1:] if l.strip() and "." in l][-1]
        date_str, val_str = last.split(",")
        fed_rate = float(val_str.strip())
        raw = {"series": "FEDFUNDS", "value": fed_rate, "date": date_str.strip()}
        con.execute(
            "INSERT INTO macro_snapshots "
            "(run_id, source, series_id, series_name, value, value_date, fetched_utc, raw_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
            (run_id, "fred", "FEDFUNDS", "Fed Funds Rate",
             fed_rate, date_str.strip(), fetched_utc, _j.dumps(raw)),
        )
        ingested.append("FEDFUNDS")
        log_cb(f"macro ingested: FEDFUNDS={fed_rate:.2f}%")
    except Exception as exc:
        log_cb(f"macro SKIP FEDFUNDS: {str(exc)[:80]}")

    con.commit()
    log_cb(f"macro ingestion done: {len(ingested)} series")
    return ingested


def load_macro_from_db(con, run_id):
    """
    Load latest macro snapshot for a run_id from DB.
    Returns dict {series_id: value}.
    Falls back to most recent run if current run has no macro data.
    """
    rows = con.execute(
        "SELECT series_id, value FROM macro_snapshots WHERE run_id=? ORDER BY id ASC;",
        (run_id,)
    ).fetchall()
    if not rows:
        # Fallback: latest available
        rows = con.execute(
            "SELECT series_id, value FROM macro_snapshots "
            "ORDER BY id DESC LIMIT 20;"
        ).fetchall()
    return {r[0]: r[1] for r in rows if r[1] is not None}


def score_macro_from_db(macro_data):
    """
    Score macro environment from DB-loaded series.
    Inputs: SP500 trend (positive = risk-on), DXY level (high = USD strong = crypto weak),
            VIX (high = fear = risk-off), Fed Funds Rate (high = tighter = risk-off).
    Returns score 0-100.
    """
    score = 50.0
    sp500  = macro_data.get("SP500")
    dxy    = macro_data.get("DXY")
    vix    = macro_data.get("VIX")
    fedfunds = macro_data.get("FEDFUNDS")

    # VIX: low (<15) = calm = bullish crypto; high (>30) = panic = bearish
    if vix is not None:
        if vix < 15:
            score += 10.0
        elif vix < 20:
            score += 5.0
        elif vix > 30:
            score -= 15.0
        elif vix > 25:
            score -= 8.0

    # DXY: high (>105) = strong dollar = risk-off for crypto
    if dxy is not None:
        if dxy > 108:
            score -= 12.0
        elif dxy > 104:
            score -= 6.0
        elif dxy < 100:
            score += 6.0
        elif dxy < 97:
            score += 10.0

    # Fed Funds: very high (>5%) = tight money = headwind
    if fedfunds is not None:
        if fedfunds > 5.0:
            score -= 8.0
        elif fedfunds > 4.0:
            score -= 4.0
        elif fedfunds < 2.0:
            score += 8.0

    return round(max(0.0, min(100.0, score)), 2)


# ---------- SENTIMENT LAYER ----------
# Sources: alternative.me Fear & Greed (free), stored in DB per run_id
# Planned fallback: Santiment (paid), LunarCrush (paid), Google Trends

def ingest_sentiment_layer(con, run_id, log_cb=None):
    """
    Ingest sentiment data and persist to sentiment_ingestion with run_id.
    Primary: alternative.me Fear & Greed Index.
    Planned: Santiment, LunarCrush, Google Trends.
    Spec A3.2: RAW stored, then normalised layer separate.
    """
    import json as _j
    if log_cb is None:
        log_cb = lambda m: None
    fetched_utc = utc_now_iso()
    ingested = []

    # Fear & Greed
    try:
        fng = fetch_fng_index()
        con.execute(
            "INSERT INTO sentiment_ingestion "
            "(run_id, source, metric_key, value, classification, fetched_utc, raw_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?);",
            (run_id, "alternative.me", "fear_greed_index",
             float(fng["value"]), fng.get("classification", ""),
             fetched_utc, _j.dumps(fng)),
        )
        ingested.append("fear_greed_index")
        log_cb(f"sentiment ingested: FNG={fng['value']} ({fng.get('classification','')})")
    except Exception as exc:
        log_cb(f"sentiment SKIP FNG: {str(exc)[:80]}")

    # Santiment -- stub (paid, planned)
    con.execute(
        "INSERT INTO sentiment_ingestion "
        "(run_id, source, metric_key, value, classification, fetched_utc, raw_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?);",
        (run_id, "santiment", "social_volume",
         None, "not_implemented", fetched_utc,
         '{"status":"planned","note":"Santiment paid API"}'),
    )

    # LunarCrush -- stub (paid, planned)
    con.execute(
        "INSERT INTO sentiment_ingestion "
        "(run_id, source, metric_key, value, classification, fetched_utc, raw_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?);",
        (run_id, "lunarcrush", "galaxy_score",
         None, "not_implemented", fetched_utc,
         '{"status":"planned","note":"LunarCrush paid API"}'),
    )

    # Google Trends -- stub (no stdlib solution, planned via pytrends)
    con.execute(
        "INSERT INTO sentiment_ingestion "
        "(run_id, source, metric_key, value, classification, fetched_utc, raw_json) "
        "VALUES (?, ?, ?, ?, ?, ?, ?);",
        (run_id, "google_trends", "crypto_search_trend",
         None, "not_implemented", fetched_utc,
         '{"status":"planned","note":"requires pytrends library"}'),
    )

    con.commit()
    log_cb(f"sentiment ingestion done: {len(ingested)} live sources")
    return ingested


def load_sentiment_from_db(con, run_id):
    """
    Load sentiment snapshot for a run_id.
    Returns dict of {metric_key: {value, classification, source}}.
    Falls back to most recent run.
    """
    rows = con.execute(
        "SELECT source, metric_key, value, classification "
        "FROM sentiment_ingestion WHERE run_id=? ORDER BY id ASC;",
        (run_id,)
    ).fetchall()
    if not rows:
        rows = con.execute(
            "SELECT source, metric_key, value, classification "
            "FROM sentiment_ingestion ORDER BY id DESC LIMIT 10;"
        ).fetchall()
    result = {}
    for source, key, val, cls in rows:
        result[key] = {"value": val, "classification": cls, "source": source}
    return result


# ---------- FUNDAMENTAL LAYER ----------
# Sources: GitHub API (free, 60 req/hour unauthenticated, 5000 with token)
#          CoinGecko /coins/{id} community_data (already in Community layer)
#          Planned: whitepapers, roadmap parsers

_COIN_GITHUB_MAP = {
    "BTC":    ("bitcoin",            "bitcoin"),
    "ETH":    ("ethereum",           "go-ethereum"),
    "SOL":    ("solana-labs",        "solana"),
    "ADA":    ("input-output-hk",    "cardano-node"),
    "DOT":    ("paritytech",         "polkadot"),
    "AVAX":   ("ava-labs",           "avalanchego"),
    "LINK":   ("smartcontractkit",   "chainlink"),
    "ATOM":   ("cosmos",             "cosmos-sdk"),
    "NEAR":   ("near",               "nearcore"),
    "APT":    ("aptos-labs",         "aptos-core"),
    "SUI":    ("MystenLabs",         "sui"),
    "OP":     ("ethereum-optimism",  "optimism"),
    "ARB":    ("OffchainLabs",       "nitro"),
    "INJ":    ("InjectiveLabs",      "injective-core"),
    "ICP":    ("dfinity",            "ic"),
    "FIL":    ("filecoin-project",   "lotus"),
    "RENDER": ("rendernetwork",      "rndr-token"),
    "GRT":    ("graphprotocol",      "graph-node"),
    "AAVE":   ("aave",               "aave-v3-core"),
    "UNI":    ("Uniswap",            "v3-core"),
    "LDO":    ("lidofinance",        "lido-dao"),
    "MKR":    ("makerdao",           "dss"),
    "CRV":    ("curvefi",            "curve-contract"),
    "SNX":    ("Synthetixio",        "synthetix"),
    "COMP":   ("compound-finance",   "compound-protocol"),
}


def ingest_fundamental_layer(con, run_id, symbols, log_cb=None):
    """
    Fetch GitHub dev activity for known coins and persist to fundamental_snapshots.
    Task 14: routed through RateLimitManager("github").
    Task 18: 24h DB-cache (skip API if fresh data exists), NYOSIG_GITHUB_TOKEN support.
    Free tier: 60 req/hour unauthenticated; 5000/hr with token.
    """
    import json as _j, os as _os, time as _t
    if log_cb is None:
        log_cb = lambda m: None
    fetched_utc = utc_now_iso()
    ingested = []

    # Task 18: GitHub token from env
    gh_token = _os.environ.get("NYOSIG_GITHUB_TOKEN", "").strip()
    rl = get_rate_limit_manager(log_cb)
    if gh_token:
        rl.DEFAULT_LIMITS["github"] = 83
        log_cb("GitHub: token present, limit=83/min")
    else:
        rl.DEFAULT_LIMITS["github"] = 1
        log_cb("GitHub: no token, limit=1/min (set NYOSIG_GITHUB_TOKEN to increase)")

    # Task 18: 24h cache threshold
    _CACHE_SECONDS = 86400
    now_ts = _t.time()

    for sym in (symbols or []):
        if sym not in _COIN_GITHUB_MAP:
            continue
        owner, repo = _COIN_GITHUB_MAP[sym]

        # Task 18: check DB cache
        try:
            cached = con.execute(
                "SELECT num_value, fetched_utc FROM fundamental_snapshots "
                "WHERE unified_symbol=? AND source='github' AND metric_key='github_stars' "
                "ORDER BY fetched_utc DESC LIMIT 1;",
                (sym,),
            ).fetchone()
            if cached:
                import datetime as _dt
                try:
                    cached_ts = _dt.datetime.strptime(
                        cached[1], "%Y-%m-%dT%H:%M:%SZ"
                    ).replace(tzinfo=_dt.timezone.utc).timestamp()
                    if (now_ts - cached_ts) < _CACHE_SECONDS:
                        log_cb(f"GitHub cache hit {sym}: stars={cached[0]:.0f} (age<24h)")
                        ingested.append(sym)
                        continue
                except Exception:
                    pass
        except Exception:
            pass

        try:
            rl.acquire("github")
            url = "https://api.github.com/repos/" + owner + "/" + repo
            headers = {"User-Agent": "NyoSig-Analysator/7.5a"}
            if gh_token:
                headers["Authorization"] = "Bearer " + gh_token
            data = _http_get_json(url, timeout=12, headers=headers)
            stars  = int(data.get("stargazers_count") or 0)
            forks  = int(data.get("forks_count")       or 0)
            issues = int(data.get("open_issues_count")  or 0)
            pushed = str(data.get("pushed_at")          or "")
            raw    = {"stars": stars, "forks": forks,
                      "open_issues": issues, "pushed_at": pushed,
                      "full_name": data.get("full_name")}
            for metric_key, num_val in [
                ("github_stars",  stars),
                ("github_forks",  forks),
                ("github_issues", issues),
            ]:
                con.execute(
                    "INSERT OR REPLACE INTO fundamental_snapshots "
                    "(run_id, unified_symbol, coin_id, source, metric_key, "
                    " num_value, text_value, fetched_utc, raw_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (run_id, sym, owner + "/" + repo,
                     "github", metric_key, float(num_val),
                     "", fetched_utc, _j.dumps(raw)),
                )
            con.execute(
                "INSERT OR REPLACE INTO fundamental_snapshots "
                "(run_id, unified_symbol, coin_id, source, metric_key, "
                " num_value, text_value, fetched_utc, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (run_id, sym, owner + "/" + repo,
                 "github", "last_pushed_utc", None,
                 pushed, fetched_utc, _j.dumps(raw)),
            )
            ingested.append(sym)
            log_cb(f"GitHub {sym}: stars={stars} forks={forks} source=api")
        except Exception as exc:
            log_cb(f"GitHub SKIP {sym}: {str(exc)[:80]}")

    con.commit()
    log_cb(f"fundamental ingestion done: {len(ingested)} symbols")
    return ingested

def score_from_fundamental_db(con, run_id, unified_symbol):
    """
    Score fundamental health 0-100 from DB-stored GitHub metrics.
    High stars + recent activity = strong developer ecosystem.
    Returns score float or None if no data.
    """
    import math as _m
    rows = con.execute(
        "SELECT metric_key, num_value, text_value "
        "FROM fundamental_snapshots "
        "WHERE run_id=? AND unified_symbol=? AND source='github';",
        (run_id, unified_symbol),
    ).fetchall()
    if not rows:
        # fallback to any run
        rows = con.execute(
            "SELECT metric_key, num_value, text_value "
            "FROM fundamental_snapshots "
            "WHERE unified_symbol=? AND source='github' "
            "ORDER BY id DESC LIMIT 10;",
            (unified_symbol,),
        ).fetchall()
    if not rows:
        return None

    metrics = {r[0]: (r[1], r[2]) for r in rows}
    stars  = (metrics.get("github_stars",  (0, ""))[0] or 0)
    forks  = (metrics.get("github_forks",  (0, ""))[0] or 0)

    def _log_s(v, cap):
        if v <= 0: return 0.0
        return min(1.0, _m.log(v + 1) / _m.log(cap + 1))

    score = _log_s(stars, 100_000) * 60.0 + _log_s(forks, 50_000) * 40.0
    return round(max(0.0, min(100.0, score)), 2)


# ---------- ONCHAIN LAYER ----------
# Sources: Blockchain.com stats API (BTC only, free, no key)
#          CryptoCompare blockchain data (limited free tier)
# Planned paid: Glassnode, CryptoQuant, IntoTheBlock, Nansen

def ingest_onchain_btc(con, run_id, log_cb=None):
    """Ingest BTC on-chain metrics from Blockchain.com stats API."""
    import json as _j
    if log_cb is None:
        log_cb = lambda m: None
    fetched_utc = utc_now_iso()

    try:
        data = _http_get_json("https://api.blockchain.info/stats", timeout=10)
        metrics = {
            "hash_rate":          float(data.get("hash_rate")         or 0),
            "n_tx_per_day":       float(data.get("n_tx")              or 0),
            "mempool_size":       float(data.get("mempool_size")      or 0),
            "difficulty":         float(data.get("difficulty")        or 0),
            "miners_revenue_usd": float(data.get("miners_revenue_usd") or 0),
            "total_fees_btc":     float(data.get("total_fees_btc")    or 0),
        }
        for key, val in metrics.items():
            con.execute(
                "INSERT INTO onchain_snapshots "
                "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?);",
                (run_id, "BTC", "blockchain.com", key, val, fetched_utc, _j.dumps(data)),
            )
        log_cb(f"onchain BTC: hash_rate={metrics['hash_rate']:.0f} n_tx={metrics['n_tx_per_day']:.0f}")
    except Exception as exc:
        log_cb(f"onchain SKIP BTC blockchain.com: {str(exc)[:80]}")
        con.execute(
            "INSERT INTO onchain_snapshots "
            "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?);",
            (run_id, "BTC", "blockchain.com", "ingestion_status",
             None, fetched_utc, '{"status":"failed"}'),
        )

    # Stubs for paid on-chain providers (recorded as planned, not failed)
    for sym, provider, note in [
        ("ETH",  "etherscan",   "tx_count -- free Etherscan API (planned)"),
        ("SOL",  "solscan",     "tps, validators -- free Solscan API (planned)"),
        ("BTC",  "glassnode",   "SOPR, NUPL, exchange_flow -- paid (planned)"),
        ("ETH",  "nansen",      "smart_money_flow -- paid (planned)"),
        ("BTC",  "cryptoquant", "exchange_reserves -- paid (planned)"),
        ("ETH",  "intotheblock","in_out_money -- paid (planned)"),
    ]:
        con.execute(
            "INSERT INTO onchain_snapshots "
            "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?);",
            (run_id, sym, provider, "status",
             None, fetched_utc, '{"status":"planned","note":"' + note + '"}'),
        )

    con.commit()
    log_cb("onchain ingestion done")


def load_onchain_btc_score(con, run_id):
    """
    Score BTC on-chain health from DB.
    Hash rate as network security proxy: higher = healthier.
    Returns score 0-100 or None.
    """
    import math as _m
    rows = con.execute(
        "SELECT metric_key, value FROM onchain_snapshots "
        "WHERE run_id=? AND unified_symbol='BTC' AND source='blockchain.com';",
        (run_id,)
    ).fetchall()
    if not rows:
        rows = con.execute(
            "SELECT metric_key, value FROM onchain_snapshots "
            "WHERE unified_symbol='BTC' AND source='blockchain.com' "
            "ORDER BY id DESC LIMIT 10;"
        ).fetchall()
    metrics = {r[0]: r[1] for r in rows if r[1] is not None}
    hr = metrics.get("hash_rate")
    if hr is None:
        return None
    # Hash rate in EH/s (exahashes). Normalise: 500 EH/s = ~80 score
    score = min(90.0, max(20.0, _m.log(hr + 1, 10) * 15.0))
    return round(score, 2)


# ---------- DERIVATIVES EXTENDED (Coinglass stub) ----------
# Coinglass free: open interest history, long/short ratio
# Planned: Coinglass API, Laevitas, CryptoQuant Derivatives

def ingest_derivatives_extended(con, run_id, symbols, log_cb=None):
    """Stub ingestion for extended derivatives data."""
    import json as _j
    if log_cb is None:
        log_cb = lambda m: None
    fetched_utc = utc_now_iso()

    planned_sources = [
        ("coinglass",             "long_short_ratio",   "Coinglass public API -- rate limited"),
        ("laevitas",              "options_oi",         "Laevitas -- paid API planned"),
        ("cryptoquant_deriv",     "futures_flow",       "CryptoQuant Derivatives -- paid"),
    ]
    for source, metric, note in planned_sources:
        for sym in (symbols or ["BTC", "ETH"])[:3]:
            con.execute(
                "INSERT INTO onchain_snapshots "
                "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?);",
                (run_id, sym, source, metric,
                 None, fetched_utc, '{"status":"planned","note":"' + note + '"}'),
            )
    con.commit()
    log_cb("derivatives_extended: planned sources recorded")


# SOURCE CASCADE MANAGER  (v5.1a)
# Spec 7.2: each layer has primary free source + fallback chain (max 2 extra).
# When source N raises RateLimitError or HTTPError 429, cascade to source N+1.
# RAW provenance records which source delivered each symbol.

class RateLimitError(Exception):
    """Raised by fetch functions when a provider returns 429 or signals exhaustion."""
    pass


class SourceCascade:
    """
    Ordered list of fetch functions for one data type.
    Tries each source in priority order; on RateLimitError or HTTPError 429
    logs the failure, marks source as exhausted for this session,
    and immediately moves to the next source.

    Usage:
        cascade = SourceCascade("spot", [fetch_cg_markets, fetch_cp_markets, fetch_cc_markets])
        data = cascade.fetch(symbols, ...)

    Each fetch_fn must:
        - Accept (symbols, **kwargs)
        - Return list of normalised dicts
        - Raise RateLimitError on 429 / exhaustion
        - Raise any other Exception on real error (cascade will NOT skip)
    """

    def __init__(self, layer_name, sources, log_cb=None):
        """
        sources: list of (source_id: str, fetch_fn: callable)
        """
        self.layer_name  = layer_name
        self.sources     = list(sources)   # [(source_id, fn), ...]
        self.log_cb      = log_cb or (lambda m: None)
        self._exhausted  = set()           # source_ids exhausted this session

    def fetch_one(self, symbol, **kwargs):
        """
        Fetch data for a single symbol, cascading through sources.
        Returns (result, source_id_used) or raises if all sources fail.
        """
        last_err = None
        for source_id, fn in self.sources:
            if source_id in self._exhausted:
                continue
            try:
                result = fn(symbol, **kwargs)
                return result, source_id
            except RateLimitError as e:
                self._exhausted.add(source_id)
                self.log_cb(
                    f"CASCADE [{self.layer_name}] {source_id} RATE_LIMITED "
                    f"-> trying next source. ({str(e)[:80]})"
                )
                last_err = e
            except Exception as e:
                # Non-rate-limit errors: log but also try next source
                self.log_cb(
                    f"CASCADE [{self.layer_name}] {source_id} ERROR "
                    f"-> {str(e)[:80]}"
                )
                last_err = e
        raise last_err or RuntimeError(
            f"All sources exhausted for layer={self.layer_name} sym={symbol}"
        )

    def fetch_batch(self, symbols, **kwargs):
        """Fetch data for a list of symbols."""
        results = {}
        failed  = {}
        for sym in symbols:
            try:
                data, src = self.fetch_one(sym, **kwargs)
                results[sym] = {"data": data, "source": src}
            except Exception as e:
                failed[sym]  = {"error": str(e)[:200]}
                self.log_cb(f"CASCADE [{self.layer_name}] FAILED sym={sym}: {str(e)[:80]}")
        return results, failed

    def reset_exhausted(self):
        """Clear exhausted set (e.g. after a sleep/retry cycle)."""
        self._exhausted.clear()

    def status(self):
        return {
            "layer":     self.layer_name,
            "sources":   [s for s, _ in self.sources],
            "exhausted": list(self._exhausted),
        }

    def log_to_db(self, con, run_id):
        """
        Persist cascade status snapshot to cascade_log table.
        Call after fetch_batch() to record which sources were tried.
        """
        if con is None or run_id is None:
            return
        now = utc_now_iso()
        available = [s for s, _ in self.sources]
        for src in available:
            status = "exhausted" if src in self._exhausted else "available"
            try:
                con.execute(
                    "INSERT INTO cascade_log "
                    "(run_id, layer, source, symbol, status, detail, logged_utc) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?);",
                    (run_id, self.layer_name, src, "",
                     status, "", now),
                )
            except Exception:
                pass


def _is_rate_limit_error(exc):
    """Detect 429 / rate limit from urllib or requests exceptions."""
    msg = str(exc)
    return ("429" in msg or
            "Too Many Requests" in msg or
            "rate limit" in msg.lower() or
            "rate_limit" in msg.lower())


# SPOT cascade  (CoinGecko -> CoinPaprika -> CoinCap)

def _fetch_spot_coingecko(symbol, vs_currency="usd", **_kw):
    """
    Fetch single-symbol SPOT from CoinGecko /coins/markets.
    Raises RateLimitError on 429.
    """
    url = (
        "https://api.coingecko.com/api/v3/coins/markets"
        "?vs_currency=" + vs_currency +
        "&ids=" + symbol.lower() +
        "&order=market_cap_desc&per_page=1&page=1"
        "&sparkline=false&price_change_percentage=24h"
    )
    try:
        data = _http_get_json(url, timeout=8)
        if not data:
            raise ValueError("empty response")
        return data[0]
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def _fetch_spot_coinpaprika(symbol, vs_currency="usd", **_kw):
    """
    Fetch SPOT from CoinPaprika /tickers (free, no key, no rate limit).
    Normalises to CoinGecko-compatible field names.
    Note: symbol -> paprika id mapping is approximate (btc-bitcoin, eth-ethereum).
    """
    sym_lower = symbol.lower()
    # Common paprika IDs
    _PAPRIKA_IDS = {
        "btc":"btc-bitcoin","eth":"eth-ethereum","bnb":"bnb-binance-coin",
        "sol":"sol-solana","xrp":"xrp-xrp","ada":"ada-cardano",
        "doge":"doge-dogecoin","trx":"trx-tron","dot":"dot-polkadot",
        "avax":"avax-avalanche","link":"link-chainlink","ltc":"ltc-litecoin",
        "atom":"atom-cosmos","near":"near-near-protocol","apt":"apt-aptos",
        "sui":"sui-sui","op":"op-optimism","arb":"arb-arbitrum",
        "inj":"inj-injective","fil":"fil-filecoin","icp":"icp-internet-computer",
        "uni":"uni-uniswap","aave":"aave-aave","mkr":"mkr-maker",
        "ldo":"ldo-lido-dao","shib":"shib-shiba-inu","grt":"grt-the-graph",
        "snx":"snx-synthetix","crv":"crv-curve-dao-token",
    }
    pid = _PAPRIKA_IDS.get(sym_lower, sym_lower + "-" + sym_lower)
    url = "https://api.coinpaprika.com/v1/tickers/" + pid
    try:
        d = _http_get_json(url, timeout=8)
        q = d.get("quotes", {}).get("USD", {})
        return {
            "symbol":                    d.get("symbol", symbol).upper(),
            "current_price":             q.get("price"),
            "market_cap":                q.get("market_cap"),
            "total_volume":              q.get("volume_24h"),
            "market_cap_rank":           d.get("rank"),
            "price_change_percentage_24h": q.get("percent_change_24h"),
            "_source":                   "coinpaprika",
        }
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def _fetch_spot_coincap(symbol, vs_currency="usd", **_kw):
    """
    Fetch SPOT from CoinCap /assets (free, no key, 200 req/min).
    """
    url = "https://api.coincap.io/v2/assets?search=" + symbol.lower() + "&limit=1"
    try:
        data = _http_get_json(url, timeout=8)
        items = data.get("data", [])
        if not items:
            raise ValueError("no data for " + symbol)
        d = items[0]
        return {
            "symbol":                    d.get("symbol", symbol).upper(),
            "current_price":             float(d["priceUsd"]) if d.get("priceUsd") else None,
            "market_cap":                float(d["marketCapUsd"]) if d.get("marketCapUsd") else None,
            "total_volume":              float(d["volumeUsd24Hr"]) if d.get("volumeUsd24Hr") else None,
            "market_cap_rank":           int(d["rank"]) if d.get("rank") else None,
            "price_change_percentage_24h": float(d["changePercent24Hr"]) if d.get("changePercent24Hr") else None,
            "_source":                   "coincap",
        }
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def make_spot_cascade(log_cb=None):
    """Build SPOT data SourceCascade: CoinGecko -> CoinPaprika -> CoinCap."""
    return SourceCascade(
        "spot",
        [
            ("coingecko",   _fetch_spot_coingecko),
            ("coinpaprika", _fetch_spot_coinpaprika),
            ("coincap",     _fetch_spot_coincap),
        ],
        log_cb=log_cb,
    )


# FUNDING RATE cascade  (Binance -> Bybit -> OKX)

def _fetch_funding_binance(symbol, **_kw):
    """Binance USD-M perpetual funding rate. Public, no key."""
    bsym = to_binance_symbol(symbol) if not symbol.endswith("USDT") else symbol
    if not bsym:
        raise ValueError("no perp market for " + symbol)
    url = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=" + bsym
    try:
        data = _http_get_json(url, timeout=8)
        return {
            "funding_rate":   float(data.get("lastFundingRate", 0.0)),
            "binance_symbol": bsym,
            "_source":        "binance",
        }
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def _fetch_funding_bybit(symbol, **_kw):
    """Bybit linear perpetual funding rate. Public, no key."""
    bsym = symbol.upper() + "USDT"
    url = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=" + bsym
    try:
        data  = _http_get_json(url, timeout=8)
        items = data.get("result", {}).get("list", [])
        if not items:
            raise ValueError("no data for " + bsym)
        fr = float(items[0].get("fundingRate", 0.0))
        return {
            "funding_rate":   fr,
            "bybit_symbol":   bsym,
            "_source":        "bybit",
        }
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def _fetch_funding_okx(symbol, **_kw):
    """OKX swap funding rate. Public, no key."""
    okx_sym = symbol.upper() + "-USDT-SWAP"
    url = "https://www.okx.com/api/v5/public/funding-rate?instId=" + okx_sym
    try:
        data  = _http_get_json(url, timeout=8)
        items = data.get("data", [])
        if not items:
            raise ValueError("no data for " + okx_sym)
        fr = float(items[0].get("fundingRate", 0.0))
        return {
            "funding_rate": fr,
            "okx_symbol":   okx_sym,
            "_source":      "okx",
        }
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def make_funding_cascade(log_cb=None):
    """Build funding rate SourceCascade: Binance -> Bybit -> OKX."""
    return SourceCascade(
        "derivatives_funding",
        [
            ("binance", _fetch_funding_binance),
            ("bybit",   _fetch_funding_bybit),
            ("okx",     _fetch_funding_okx),
        ],
        log_cb=log_cb,
    )


# OPEN INTEREST cascade  (Binance -> Bybit -> OKX)

def _fetch_oi_binance(symbol, **_kw):
    """Binance USD-M futures OI. Public, no key."""
    bsym = to_binance_symbol(symbol) if not symbol.endswith("USDT") else symbol
    if not bsym:
        raise ValueError("no perp market for " + symbol)
    try:
        url  = "https://fapi.binance.com/fapi/v1/openInterest?symbol=" + bsym
        data = _http_get_json(url, timeout=8)
        oi   = float(data.get("openInterest", 0.0))
        hist_url = (
            "https://fapi.binance.com/futures/data/openInterestHist"
            "?symbol=" + bsym + "&period=1d&limit=5"
        )
        try:
            hist = _http_get_json(hist_url, timeout=8)
            oi_vals = [float(h.get("sumOpenInterestValue", 0)) for h in hist]
        except Exception:
            oi_vals = []
        return {
            "open_interest_contracts": oi,
            "oi_history_usd":          oi_vals,
            "binance_symbol":          bsym,
            "_source":                 "binance",
        }
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def _fetch_oi_bybit(symbol, **_kw):
    """Bybit linear perpetual open interest. Public, no key."""
    bsym = symbol.upper() + "USDT"
    url  = "https://api.bybit.com/v5/market/open-interest?category=linear&symbol=" + bsym + "&intervalTime=1d&limit=5"
    try:
        data  = _http_get_json(url, timeout=8)
        items = data.get("result", {}).get("list", [])
        if not items:
            raise ValueError("no OI data for " + bsym)
        oi_vals = [float(it.get("openInterestValue", 0)) for it in items]
        return {
            "open_interest_contracts": float(items[0].get("openInterest", 0)),
            "oi_history_usd":          oi_vals,
            "bybit_symbol":            bsym,
            "_source":                 "bybit",
        }
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def make_oi_cascade(log_cb=None):
    """Build OI SourceCascade: Binance -> Bybit."""
    return SourceCascade(
        "open_interest",
        [
            ("binance", _fetch_oi_binance),
            ("bybit",   _fetch_oi_bybit),
        ],
        log_cb=log_cb,
    )


# COMMUNITY cascade  (CoinGecko -> CoinCap basic)

def _fetch_community_coingecko(symbol, coin_id=None, **_kw):
    """CoinGecko /coins/{id} community data. Free, ~10-15/min."""
    cid = coin_id or symbol.lower()
    return fetch_community_data(cid)  # uses existing function with cache


def _fetch_community_coincap(symbol, **_kw):
    """CoinCap /assets/{id} -- basic community proxy (no social data, returns low score)."""
    url = "https://api.coincap.io/v2/assets/" + symbol.lower()
    try:
        data = _http_get_json(url, timeout=8)
        d    = data.get("data", {})
        return {
            "reddit_subscribers": 0,
            "twitter_followers":  0,
            "github_commits_4w":  0,
            "github_stars":       0,
            "coingecko_watchers": 0,
            "score":              20.0,  # minimal score -- limited data
            "source":             "coincap_basic",
            "_source":            "coincap",
            "_note":              "limited community data from CoinCap fallback",
        }
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def make_community_cascade(log_cb=None):
    """Build community SourceCascade: CoinGecko -> CoinCap (basic fallback)."""
    return SourceCascade(
        "community",
        [
            ("coingecko", _fetch_community_coingecko),
            ("coincap",   _fetch_community_coincap),
        ],
        log_cb=log_cb,
    )


# OHLCV cascade  (CoinGecko -> CryptoCompare -> Binance klines)

def _fetch_ohlcv_coingecko(symbol, vs_currency="usd", days=30, coin_id=None, **_kw):
    """
    CoinGecko /coins/{id}/ohlc  -- free, ~10-15 req/min.
    Returns list of [timestamp_ms, open, high, low, close].
    Raises RateLimitError on 429.
    """
    cid = coin_id or symbol.lower()
    url = (
        "https://api.coingecko.com/api/v3/coins/" + cid +
        "/ohlc?vs_currency=" + vs_currency +
        "&days=" + str(days)
    )
    try:
        data = _http_get_json(url, timeout=12)
        if not isinstance(data, list) or not data:
            raise ValueError("empty OHLCV from CoinGecko for " + cid)
        return [{"ts": r[0], "open": r[1], "high": r[2],
                 "low": r[3], "close": r[4], "_source": "coingecko"}
                for r in data]
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def _fetch_ohlcv_cryptocompare(symbol, vs_currency="usd", days=30, **_kw):
    """
    CryptoCompare /data/v2/histoday -- free, 100k calls/month, no key.
    Returns list of OHLCV dicts compatible with normalise_ohlcv_rows.
    """
    limit = min(days, 2000)
    url = (
        "https://min-api.cryptocompare.com/data/v2/histoday"
        "?fsym=" + symbol.upper() +
        "&tsym=" + vs_currency.upper() +
        "&limit=" + str(limit)
    )
    try:
        data  = _http_get_json(url, timeout=12)
        items = data.get("Data", {}).get("Data", [])
        if not items:
            raise ValueError("no histoday data for " + symbol)
        result = []
        for it in items:
            if it.get("open", 0) == 0:
                continue
            result.append({
                "ts":     int(it["time"]) * 1000,  # to ms
                "open":   float(it["open"]),
                "high":   float(it["high"]),
                "low":    float(it["low"]),
                "close":  float(it["close"]),
                "volume": float(it.get("volumefrom", 0.0)),
                "_source": "cryptocompare",
            })
        return result
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def _fetch_ohlcv_binance_klines(symbol, vs_currency="usd", days=30, **_kw):
    """
    Binance spot klines /api/v3/klines -- public, no key, 1200 req/min.
    Only USD pairs (USDT) -- vs_currency override ignored.
    Returns list of OHLCV dicts.
    """
    import time as _t
    bsym = symbol.upper() + "USDT"
    limit = min(days, 1000)
    url = (
        "https://api.binance.com/api/v3/klines"
        "?symbol=" + bsym +
        "&interval=1d&limit=" + str(limit)
    )
    try:
        data = _http_get_json(url, timeout=12)
        if not isinstance(data, list) or not data:
            raise ValueError("no klines from Binance for " + bsym)
        result = []
        for k in data:
            result.append({
                "ts":     int(k[0]),
                "open":   float(k[1]),
                "high":   float(k[2]),
                "low":    float(k[3]),
                "close":  float(k[4]),
                "volume": float(k[5]),
                "_source": "binance",
            })
        return result
    except Exception as e:
        if _is_rate_limit_error(e):
            raise RateLimitError(str(e)) from e
        raise


def make_ohlcv_cascade(log_cb=None):
    """
    Build OHLCV SourceCascade: CoinGecko -> CryptoCompare -> Binance klines.
    Spec 7.2: primary free source + up to 2 fallbacks.
    """
    return SourceCascade(
        "ohlcv",
        [
            ("coingecko",      _fetch_ohlcv_coingecko),
            ("cryptocompare",  _fetch_ohlcv_cryptocompare),
            ("binance",        _fetch_ohlcv_binance_klines),
        ],
        log_cb=log_cb,
    )


def validate_candle(o, h, l, c):
    """
    Validate OHLCV candle per ohlcv_data_rules_cz.md sect 1.1.
    Returns (is_valid: bool, reason: str).
    """
    if o is None or c is None or h is None or l is None:
        return False, "null_value"
    o, h, l, c = float(o), float(h), float(l), float(c)
    if o <= 0 or c <= 0:
        return False, "zero_price"
    if h < l:
        return False, "high_lt_low"
    if h < o or h < c:
        return False, "high_inconsistent"
    if l > o or l > c:
        return False, "low_inconsistent"
    return True, "ok"


def normalise_ohlcv_cascade(raw_rows, run_id, snap_key, unified_symbol, timeframe):
    """
    Canonical OHLCV normaliser (Task 15). Single function that all fetchers use.
    Input: List of canonical dicts with keys:
        ts (int ms), open, high, low, close, volume (opt),
        _source (provider: coingecko/cryptocompare/binance),
        _exchange (optional: actual exchange, e.g. Binance spot)
    Output: List of tuples for insert_ohlcv_snapshots() --
        (run_id, snap_key, data_source, unified_symbol, timeframe,
         open_time, open, high, low, close, volume)
    Task 16: data_source = provider (coingecko/cryptocompare), exchange = burza (Binance/Coinbase).
    Column 'exchange' in ohlcv_snapshots is now populated with data_source for
    backward compat; data_source column added separately via ensure_column().
    """
    import datetime as _dt
    normalised = []
    for r in raw_rows:
        ts_ms  = int(r.get("ts", 0))
        ts_dt  = _dt.datetime.utcfromtimestamp(ts_ms / 1000.0)
        ot_str = ts_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        data_src = r.get("_source", "unknown")   # e.g. "coingecko", "cryptocompare"
        normalised.append((
            run_id,
            snap_key,
            data_src,          # exchange column (backward compat) = data_source
            unified_symbol,
            timeframe,
            ot_str,
            r.get("open"),
            r.get("high"),
            r.get("low"),
            r.get("close"),
            r.get("volume"),
        ))
    return normalised



# Layer data-fetch and scoring functions  (spec 7.1 -- free public endpoints)

import urllib.request as _urllib_req

def _http_get_json(url, timeout=10, headers=None):
    """Minimal HTTP GET -> parsed JSON. No dependencies beyond stdlib."""
    _h = {"User-Agent": "NyoSig-Analysator/7.5c"}
    if headers:
        _h.update(headers)
    req = _urllib_req.Request(url, headers=_h)
    with _urllib_req.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    import json as _json
    return _json.loads(raw)


# Task 22: Unified error payload -----------------------------------------------
# All layer except-blocks return _make_layer_error() instead of bare str(exc)[:N]
# Audit chain: stored in reason_json["_error"] or cascade_log.error_payload

_HTTP_STATUS_HINTS = {
    "429": "RATE_LIMIT_429",
    "403": "AUTH_403",
    "404": "NOT_FOUND_404",
    "500": "SERVER_ERROR_5xx",
    "503": "SERVER_ERROR_5xx",
    "timed out": "TIMEOUT",
    "timeout": "TIMEOUT",
    "connection refused": "CONNECTION_REFUSED",
    "no data": "NO_DATA",
    "json": "PARSE_ERROR",
    "decode": "PARSE_ERROR",
}


def _make_layer_error(exc, layer="unknown", source="unknown",
                      symbol=None, retryable=None):
    """
    Task 22: build canonical error payload dict.
    All layer exception handlers should use this instead of bare str(exc)[:N].
    Returns dict suitable for json.dumps() and storage in reason_json["_error"].
    """
    msg = str(exc)
    # Infer error_code from message
    code = "UNKNOWN_ERROR"
    for hint, c in _HTTP_STATUS_HINTS.items():
        if hint.lower() in msg.lower():
            code = c
            break
    # Infer http_status
    http_status = None
    import re as _re
    m = _re.search(r"HTTP Error (\d{3})", msg)
    if m:
        http_status = int(m.group(1))
        code = "HTTP_" + str(http_status)
    # Infer retryable
    if retryable is None:
        retryable = code in ("RATE_LIMIT_429", "TIMEOUT", "SERVER_ERROR_5xx",
                             "CONNECTION_REFUSED")
    return {
        "error_code":    code,
        "layer":         layer,
        "source":        source,
        "symbol":        symbol,
        "retryable":     retryable,
        "http_status":   http_status,
        "message_short": msg[:120],
        "message_full":  msg[:600],
    }


# ---- Sentiment: Fear & Greed Index (alternative.me, free, no key) ----

def fetch_fng_index():
    """
    Fetch latest Fear & Greed index from alternative.me.
    Returns dict: value(int), classification(str), source(str).
    Spec 7.2: primary free source, limit=10/min.
    """
    data = _http_get_json("https://api.alternative.me/fng/?limit=1&format=json")
    item = data["data"][0]
    return {
        "value": int(item["value"]),
        "classification": item.get("value_classification", ""),
        "source": "alternative.me/fng",
    }


def score_from_fng_value(fng_value):
    """
    Map FNG 0-100 -> score 0-100.
    Extreme Fear (<20) = bullish contrarian signal -> high score.
    Extreme Greed (>80) = risk-off signal -> low score.
    Neutral zone (45-55) -> ~50.
    """
    v = max(0, min(100, int(fng_value)))
    # Invert: low FNG (fear) is good entry, high FNG (greed) is risk
    # Score = 100 - v  gives extreme fear=100, extreme greed=0
    # Moderate it: blend with 50 (neutral)
    raw = 100.0 - v
    score = raw * 0.6 + 50.0 * 0.4
    return round(max(0.0, min(100.0, score)), 2)


# ---- Macro: CoinGecko /global (free, no key) ----

def fetch_coingecko_global():
    """
    Fetch global crypto market data from CoinGecko /global.
    Returns relevant fields for macro scoring.
    Spec 7.2: primary free source.
    """
    data = _http_get_json("https://api.coingecko.com/api/v3/global")
    gd = data.get("data", data)
    return {
        "market_cap_change_percentage_24h_usd": float(gd.get("market_cap_change_percentage_24h_usd", 0.0)),
        "btc_dominance": float(gd.get("market_cap_percentage", {}).get("btc", 0.0)),
        "eth_dominance": float(gd.get("market_cap_percentage", {}).get("eth", 0.0)),
        "active_cryptocurrencies": int(gd.get("active_cryptocurrencies", 0)),
        "markets": int(gd.get("markets", 0)),
        "source": "coingecko/global",
    }


def score_from_global(market_cap_change_pct_24h, btc_dominance):
    """
    Score macro environment 0-100.
    Positive market_cap_change -> bullish.
    BTC dominance: 40-55% = neutral, >60% = risk-off (flight to BTC), <35% = alt season.
    """
    # Market cap change component (0-60 pts)
    mc_score = 50.0 + max(-25.0, min(25.0, market_cap_change_pct_24h * 5.0))
    # BTC dominance component (0-40 pts)
    btc = float(btc_dominance)
    if btc < 35.0:
        # Alt season - good for alts, neutral for BTC
        dom_score = 60.0
    elif btc < 55.0:
        # Normal range
        dom_score = 50.0
    else:
        # BTC flight - risk-off, bad for alts
        dom_score = 30.0
    score = mc_score * 0.6 + dom_score * 0.4
    return round(max(0.0, min(100.0, score)), 2)


WRAPPED_TO_ORIGINAL = {
    "WBTC": "BTC",
    "WETH": "ETH",
}

STABLECOIN_SYMBOLS = {
    "USDT","USDC","DAI","TUSD","USDP","BUSD","FDUSD","USDD","FRAX","PYUSD","EURC","USTC",
}

BINANCE_PERP_MAP = {
    "BTC": "BTCUSDT",
    "ETH": "ETHUSDT",
    "BNB": "BNBUSDT",
    "SOL": "SOLUSDT",
    "XRP": "XRPUSDT",
    "ADA": "ADAUSDT",
    "DOGE": "DOGEUSDT",
}

# ---- Derivatives: Binance funding rate (public, no key) ----

def to_binance_symbol(unified_symbol, log_cb=None):
    """Convert unified symbol to Binance perp pair using BINANCE_PERP_MAP constant."""
    if not unified_symbol:
        return None
    sym = str(unified_symbol).upper().strip()

    # Resolve wrapped tokens to original
    sym = (globals().get('WRAPPED_TO_ORIGINAL', {}) or {}).get(sym, sym)

    # Skip stablecoins
    if sym in STABLECOIN_SYMBOLS:
        return None

    # Use binding map constant
    if sym in BINANCE_PERP_MAP:
        return BINANCE_PERP_MAP[sym]

    # Unknown: generic fallback, log for audit
    candidate = sym + "USDT"
    if log_cb:
        log_cb(f"to_binance_symbol: unmapped_symbol={sym} using generic={candidate}")
    return candidate


def fetch_binance_funding_rate(binance_symbol):
    """
    Fetch current funding rate from Binance USD-M futures.
    Uses /fapi/v1/premiumIndex (public, no key, limit ~1200/min).
    Returns funding_rate as float (e.g. 0.0001 = 0.01%).
    Raises on HTTP error.
    """
    url = "https://fapi.binance.com/fapi/v1/premiumIndex?symbol=" + str(binance_symbol)
    data = _http_get_json(url, timeout=8)
    return float(data.get("lastFundingRate", data.get("fundingRate", 0.0)))


def score_from_funding_rate(funding_rate):
    """
    Map Binance perpetual funding rate -> score 0-100.
    Positive funding = longs pay shorts = crowded long = overbought -> lower score.
    Negative funding = shorts pay longs = crowded short = oversold -> higher score.
    Neutral (~0) -> 50.
    Range: typical -0.03% to +0.03% (-0.0003 to +0.0003).
    """
    fr = float(funding_rate)
    # Scale: +0.001 (very high positive) -> score 5
    #          0.0  (neutral)             -> score 50
    #        -0.001 (very negative)       -> score 95
    score = 50.0 - (fr * 45000.0)
    return round(max(0.0, min(100.0, score)), 2)


# ---- OnChain: placeholder (spec 7.4 note: no free realtime source) ----

def fetch_onchain_placeholder(sym):
    """
    OnChain layer fallback for assets without dedicated on-chain source.
    Returns DEGRADED status with partial coverage note.
    Spec: graceful degradation -> DEGRADED, pipeline continues (8.4).
    """
    return {"status": "degraded", "score": None,
            "note": f"OnChain: no free on-chain source for {sym}. DEGRADED."}


# ---- OnChain: Etherscan ETH + Blockchair multi-asset (v6.1a) ----

def ingest_onchain_eth(con, run_id, log_cb=None):
    """Ingest ETH on-chain metrics from Blockchair (free, no key)."""
    import json as _j
    if log_cb is None:
        log_cb = lambda m: None
    fetched_utc = utc_now_iso()
    try:
        data = _http_get_json("https://api.blockchair.com/ethereum/stats", timeout=15)
        stats = data.get("data", {}) if isinstance(data, dict) else {}
        metrics = {
            "transactions_24h":    float(stats.get("transactions_24h") or 0),
            "blocks_24h":          float(stats.get("blocks_24h") or 0),
            "difficulty":          float(stats.get("difficulty") or 0),
            "mempool_transactions": float(stats.get("mempool_transactions") or 0),
            "average_transaction_fee_usd_24h": float(stats.get("average_transaction_fee_usd_24h") or 0),
            "market_dominance_percentage": float(stats.get("market_dominance_percentage") or 0),
        }
        for key, val in metrics.items():
            con.execute(
                "INSERT INTO onchain_snapshots "
                "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?);",
                (run_id, "ETH", "blockchair", key, val, fetched_utc, _j.dumps(stats)),
            )
        log_cb(f"onchain ETH: tx_24h={metrics['transactions_24h']:.0f} "
               f"fee_avg=${metrics['average_transaction_fee_usd_24h']:.2f}")
    except Exception as exc:
        log_cb(f"onchain SKIP ETH blockchair: {str(exc)[:80]}")
        con.execute(
            "INSERT INTO onchain_snapshots "
            "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
            "VALUES (?, ?, ?, ?, ?, ?, ?);",
            (run_id, "ETH", "blockchair", "ingestion_status",
             None, fetched_utc, '{"status":"failed"}'),
        )
    con.commit()


def ingest_onchain_multi(con, run_id, symbols, log_cb=None):
    """
    Ingest on-chain stats for multiple assets via Blockchair free API.
    Covers: BTC, ETH, LTC, DOGE, BCH, XRP, ADA, SOL.
    Blockchair free tier: ~30 req/min, no key needed.
    Spec 7.3: graceful degradation for unsupported symbols.
    """
    import json as _j
    import time as _t
    if log_cb is None:
        log_cb = lambda m: None

    CHAIN_MAP = {
        "BTC": "bitcoin", "ETH": "ethereum", "LTC": "litecoin",
        "DOGE": "dogecoin", "BCH": "bitcoin-cash", "XRP": "ripple",
        "ADA": "cardano", "SOL": "solana",
    }
    fetched_utc = utc_now_iso()
    ok_count = 0
    skip_count = 0

    for sym in symbols:
        chain = CHAIN_MAP.get(sym)
        if not chain:
            skip_count += 1
            continue
        try:
            url = f"https://api.blockchair.com/{chain}/stats"
            data = _http_get_json(url, timeout=15)
            stats = data.get("data", {}) if isinstance(data, dict) else {}
            universal = {}
            for key in ("transactions_24h", "blocks_24h", "difficulty",
                        "mempool_transactions", "average_transaction_fee_usd_24h",
                        "market_dominance_percentage", "hashrate_24h",
                        "suggested_transaction_fee_per_byte_sat"):
                val = stats.get(key)
                if val is not None:
                    universal[key] = float(val)
            first_key = True
            for key, val in universal.items():
                con.execute(
                    "INSERT OR IGNORE INTO onchain_snapshots "
                    "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?);",
                    (run_id, sym, "blockchair", key, val, fetched_utc,
                     _j.dumps(stats) if first_key else "{}"),
                )
                first_key = False
            ok_count += 1
            log_cb(f"onchain {sym}: {len(universal)} metrics from blockchair")
            _t.sleep(1.0)
        except Exception as exc:
            skip_count += 1
            log_cb(f"onchain SKIP {sym} blockchair: {str(exc)[:80]}")
            con.execute(
                "INSERT OR IGNORE INTO onchain_snapshots "
                "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?);",
                (run_id, sym, "blockchair", "ingestion_status",
                 None, fetched_utc, '{"status":"failed"}'),
            )
    con.commit()
    log_cb(f"onchain multi done: ok={ok_count} skip={skip_count}")


def load_onchain_score(con, run_id, unified_symbol):
    """
    Compute on-chain health score for any symbol.
    Uses blockchain.com for BTC, Blockchair for others.
    Returns score 0-100 or None.
    Spec 7.4: OnChain is key decision layer (weight 0.15).
    """
    import math as _m
    rows = con.execute(
        "SELECT metric_key, value FROM onchain_snapshots "
        "WHERE run_id=? AND unified_symbol=? AND value IS NOT NULL;",
        (run_id, unified_symbol)
    ).fetchall()
    if not rows:
        rows = con.execute(
            "SELECT metric_key, value FROM onchain_snapshots "
            "WHERE unified_symbol=? AND value IS NOT NULL "
            "ORDER BY id DESC LIMIT 20;",
            (unified_symbol,)
        ).fetchall()
    if not rows:
        return None
    metrics = {r[0]: r[1] for r in rows}

    if unified_symbol == "BTC":
        hr = metrics.get("hash_rate") or metrics.get("hashrate_24h")
        if hr and hr > 0:
            score = min(90.0, max(20.0, _m.log(hr + 1, 10) * 15.0))
        else:
            score = 50.0
        tx = metrics.get("n_tx_per_day") or metrics.get("transactions_24h")
        if tx and tx > 0:
            tx_factor = min(1.1, max(0.9, tx / 350000.0))
            score *= tx_factor
        return round(min(100.0, max(0.0, score)), 2)

    # ETH + others: tx volume + fee environment
    tx_24h = metrics.get("transactions_24h")
    fee_avg = metrics.get("average_transaction_fee_usd_24h")
    dominance = metrics.get("market_dominance_percentage")
    if tx_24h is not None and tx_24h > 0:
        EXPECTED_TX = {
            "ETH": 1200000, "LTC": 100000, "DOGE": 50000,
            "BCH": 30000, "XRP": 1500000, "ADA": 50000, "SOL": 30000000,
        }
        expected = EXPECTED_TX.get(unified_symbol, 100000)
        activity_ratio = tx_24h / expected
        score = 40.0 + min(40.0, activity_ratio * 25.0)
    else:
        score = 50.0
    if fee_avg is not None and fee_avg > 0:
        if fee_avg < 0.5:
            score += 5.0
        elif fee_avg > 10.0:
            score -= 5.0
    if dominance is not None and dominance > 0:
        if dominance > 5.0:
            score += 3.0
        elif dominance < 0.5:
            score -= 2.0
    return round(min(100.0, max(0.0, score)), 2)


# ---- Institutional: CME futures + ETF flows (v6.1a) ----

def fetch_institutional_placeholder(sym):
    """
    Institutional layer fallback for assets without institutional tracking.
    Returns DEGRADED status. Spec 8.4: graceful degradation.
    """
    return {"status": "degraded", "score": None,
            "note": f"Institutional: no CME/ETF data for {sym}. DEGRADED."}


def ingest_institutional_layer(con, run_id, symbols, log_cb=None):
    """
    Ingest institutional data: CME futures OI + ETF AUM proxies.
    Sources: Yahoo Finance BTC=F, ETH=F, IBIT, FBTC, GBTC, ETHA, FETH.
    All free, public, no API key required.
    Spec 7.2: primary free source with graceful degradation.
    """
    import json as _j
    import time as _t
    if log_cb is None:
        log_cb = lambda m: None
    fetched_utc = utc_now_iso()

    CME_TICKERS = {"BTC": "BTC=F", "ETH": "ETH=F"}
    for sym in symbols:
        ticker = CME_TICKERS.get(sym)
        if not ticker:
            continue
        try:
            price, date_str = _fetch_yahoo_quote(ticker, log_cb=log_cb)
            if price is not None:
                con.execute(
                    "INSERT OR IGNORE INTO onchain_snapshots "
                    "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?);",
                    (run_id, sym, "yahoo_cme", "cme_futures_price", price, fetched_utc,
                     _j.dumps({"ticker": ticker, "price": price, "date": date_str})),
                )
                log_cb(f"institutional CME {sym}: price=${price:.2f}")
            _t.sleep(1.0)
        except Exception as exc:
            log_cb(f"institutional SKIP CME {sym}: {str(exc)[:80]}")

    ETF_MAP = {
        "BTC": [("IBIT", "iShares BTC ETF"), ("FBTC", "Fidelity BTC ETF"),
                ("GBTC", "Grayscale BTC Trust")],
        "ETH": [("ETHA", "iShares ETH ETF"), ("FETH", "Fidelity ETH ETF")],
    }
    for sym in symbols:
        etfs = ETF_MAP.get(sym, [])
        for etf_ticker, etf_name in etfs:
            try:
                price, date_str = _fetch_yahoo_quote(etf_ticker, log_cb=log_cb)
                if price is not None:
                    con.execute(
                        "INSERT OR IGNORE INTO onchain_snapshots "
                        "(run_id, unified_symbol, source, metric_key, value, fetched_utc, raw_json) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?);",
                        (run_id, sym, "yahoo_etf", f"etf_{etf_ticker}_price",
                         price, fetched_utc,
                         _j.dumps({"ticker": etf_ticker, "name": etf_name,
                                   "price": price, "date": date_str})),
                    )
                    log_cb(f"institutional ETF {etf_ticker}: ${price:.2f}")
                _t.sleep(0.8)
            except Exception as exc:
                log_cb(f"institutional SKIP ETF {etf_ticker}: {str(exc)[:80]}")
    con.commit()
    log_cb("institutional ingestion done")


def load_institutional_score(con, run_id, unified_symbol):
    """
    Compute institutional activity score for a symbol.
    Signals: CME futures presence, ETF product count.
    Returns score 0-100 or None.
    """
    rows = con.execute(
        "SELECT metric_key, value, raw_json FROM onchain_snapshots "
        "WHERE run_id=? AND unified_symbol=? "
        "AND source IN ('yahoo_cme', 'yahoo_etf') AND value IS NOT NULL;",
        (run_id, unified_symbol)
    ).fetchall()
    if not rows:
        rows = con.execute(
            "SELECT metric_key, value, raw_json FROM onchain_snapshots "
            "WHERE unified_symbol=? "
            "AND source IN ('yahoo_cme', 'yahoo_etf') AND value IS NOT NULL "
            "ORDER BY id DESC LIMIT 10;",
            (unified_symbol,)
        ).fetchall()
    if not rows:
        return None
    metrics = {r[0]: r[1] for r in rows}
    score = 50.0
    cme_price = metrics.get("cme_futures_price")
    if cme_price and cme_price > 0:
        score += 10.0
    etf_count = sum(1 for k in metrics if k.startswith("etf_") and metrics[k] and metrics[k] > 0)
    if etf_count > 0:
        score += min(15.0, etf_count * 5.0)
    return round(min(100.0, max(0.0, score)), 2)


# Technical Analysis layer  (RSI-14 + MACD from local OHLCV -- no API call)

def _ema(values, period):
    """Exponential moving average."""
    if not values or period <= 0:
        return []
    k = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(v * k + result[-1] * (1.0 - k))
    return result


def compute_ema_slope(closes, period=20, slope_window=5):
    """EMA-20 slope: returns (ema_current, price_above_ema, slope_pct, is_confirmed_trend)."""
    if len(closes) < period + slope_window:
        return None, None, None, False
    ema_vals = _ema(closes, period)
    if len(ema_vals) < slope_window + 1:
        return None, None, None, False
    ema_current = ema_vals[-1]
    ema_prev    = ema_vals[-(slope_window + 1)]
    price_now   = closes[-1]
    # Slope as % change over slope_window candles
    slope_pct = ((ema_current - ema_prev) / ema_prev * 100.0) if ema_prev != 0 else 0.0
    price_above = price_now > ema_current
    is_confirmed = price_above and slope_pct > 0.0
    return round(ema_current, 6), price_above, round(slope_pct, 4), is_confirmed


def compute_mean_reversion(closes, window=20):
    """Mean reversion: returns (sma, deviation_pct, signal). >+15% penalty, <-5% opportunity."""
    if len(closes) < window:
        return None, None, "insufficient_data"
    sma = sum(closes[-window:]) / window
    price = closes[-1]
    deviation_pct = (price - sma) / sma * 100.0 if sma != 0 else 0.0
    if deviation_pct > 15.0:
        signal = "overextended_correction_risk"
    elif deviation_pct > 5.0:
        signal = "mildly_extended"
    elif deviation_pct < -15.0:
        signal = "deeply_oversold_opportunity"
    elif deviation_pct < -5.0:
        signal = "undervalued_opportunity"
    else:
        signal = "near_mean"
    return round(sma, 6), round(deviation_pct, 3), signal


def compute_relative_volume(volumes, recent_days=1, baseline_days=7):
    """RVOL: recent_vol/baseline_avg. >1.5=confirmed, 0.8-1.2=neutral, <0.8=weak."""
    clean = [v for v in volumes if v is not None and v > 0]
    if len(clean) < baseline_days + recent_days:
        return None, "insufficient_data"
    recent_vols   = clean[-recent_days:]
    baseline_vols = clean[-(baseline_days + recent_days):-recent_days]
    if not baseline_vols:
        return None, "insufficient_data"
    recent_avg   = sum(recent_vols) / len(recent_vols)
    baseline_avg = sum(baseline_vols) / len(baseline_vols)
    if baseline_avg == 0:
        return None, "zero_baseline"
    rvol = recent_avg / baseline_avg
    if rvol > 2.0:
        signal = "extreme_spike"
    elif rvol > 1.5:
        signal = "elevated_confirmed"
    elif rvol > 1.2:
        signal = "mildly_elevated"
    elif rvol >= 0.8:
        signal = "normal"
    else:
        signal = "below_average"
    return round(rvol, 3), signal


def compute_rsi(closes, period=14):
    """
    RSI-14 from list of closing prices (oldest first).
    Returns float 0-100 or None if insufficient data.
    """
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i-1]
        gains.append(max(0.0, delta))
        losses.append(max(0.0, -delta))
    # Wilder smoothing (initial SMA then EMA)
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


def compute_macd(closes, fast=12, slow=26, signal=9):
    """
    MACD histogram from closing prices.
    Returns (macd_line, signal_line, histogram) or (None, None, None).
    """
    if len(closes) < slow + signal:
        return None, None, None
    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    sig_line  = _ema(macd_line, signal)
    histogram = [m - s for m, s in zip(macd_line, sig_line)]
    return macd_line[-1], sig_line[-1], histogram[-1]


def score_from_technical(rsi, macd_hist,
                         ema_slope_pct=None, ema_confirmed=None,
                         mean_dev_pct=None,
                         rvol=None):
    """Score 0-100: RSI(35%) MACD(25%) EMA-slope(20%) MeanRev(12%) RVOL(8%). Missing=neutral."""
    # --- RSI component: 0-35 pts ---
    if rsi is None:
        rsi_comp = 17.5  # neutral midpoint
    else:
        # RSI=20->35, RSI=50->17.5, RSI=80->3.5  (linear mapping)
        rsi_comp = max(1.0, min(35.0, 35.0 - (rsi - 20.0) * (33.0 / 60.0)))

    # --- MACD component: 0-25 pts ---
    if macd_hist is None:
        macd_comp = 12.5  # neutral midpoint
    else:
        # Positive histogram = bullish momentum
        # Clamp contribution to +/-12.5 pts around midpoint
        raw = float(macd_hist) * 25.0
        macd_comp = max(0.0, min(25.0, 12.5 + raw))

    # --- EMA-20 slope component: 0-20 pts ---
    # Confirmed trend (price > EMA AND EMA rising) = high score
    # Price below EMA AND EMA falling = low score
    if ema_slope_pct is None or ema_confirmed is None:
        ema_comp = 10.0  # neutral
    else:
        # Base: confirmed trend = 18 pts, not confirmed = 8 pts
        base = 18.0 if ema_confirmed else 8.0
        # Bonus/penalty for slope magnitude (pct per 5 candles)
        # +1% slope = +1 pt bonus, -1% slope = -1 pt penalty, capped +/-5
        slope_bonus = max(-5.0, min(5.0, float(ema_slope_pct)))
        ema_comp = max(0.0, min(20.0, base + slope_bonus - (0.0 if ema_confirmed else slope_bonus)))

    # --- Mean reversion component: 0-12 pts ---
    # Near mean -> 6 pts (neutral). Overextended UP -> penalty. Oversold -> bonus.
    if mean_dev_pct is None:
        mean_comp = 6.0  # neutral
    else:
        d = float(mean_dev_pct)
        if d > 15.0:
            # Strongly overextended: correction risk -> low score
            mean_comp = max(0.0, 2.0 - (d - 15.0) * 0.1)
        elif d > 5.0:
            # Mildly extended: small penalty
            mean_comp = max(2.0, 6.0 - (d - 5.0) * 0.4)
        elif d < -15.0:
            # Deeply oversold: mean reversion opportunity -> bonus
            mean_comp = min(12.0, 10.0 + (abs(d) - 15.0) * 0.1)
        elif d < -5.0:
            # Undervalued: opportunity
            mean_comp = 8.0 + (abs(d) - 5.0) * 0.2
        else:
            # Near mean: neutral
            mean_comp = 6.0
        mean_comp = max(0.0, min(12.0, mean_comp))

    # --- Relative Volume component: 0-8 pts ---
    # RVOL > 1.5 confirms buyer interest -> full score
    # RVOL < 0.8 -> weak interest -> low score
    if rvol is None:
        rvol_comp = 4.0  # neutral
    else:
        r = float(rvol)
        if r >= 2.0:
            rvol_comp = 8.0
        elif r >= 1.5:
            rvol_comp = 6.0 + (r - 1.5) * 4.0
        elif r >= 1.2:
            rvol_comp = 5.0 + (r - 1.2) * (1.0 / 0.3)
        elif r >= 0.8:
            rvol_comp = 3.0 + (r - 0.8) * 5.0
        else:
            rvol_comp = max(0.0, 3.0 - (0.8 - r) * 5.0)
        rvol_comp = max(0.0, min(8.0, rvol_comp))

    total = rsi_comp + macd_comp + ema_comp + mean_comp + rvol_comp
    return round(max(0.0, min(100.0, total)), 2)


def fetch_technical_from_db(con, unified_symbol, timeframe="1d",
                            min_candles=None):
    # Use binding rule if not overridden
    if min_candles is None:
        min_candles = OHLCV_CANDLE_RULES["technical_layer"]  # 60
    """
    Load OHLCV closes from ohlcv_snapshots DB and compute RSI + MACD.
    Returns dict with rsi, macd_hist, score, candles_used.
    No API call - uses locally stored data.
    Spec 7.1: uses local persistence layer.
    """
    rows = con.execute(
        "SELECT open_time, close, volume FROM ohlcv_snapshots "
        "WHERE unified_symbol=? AND timeframe=? "
        "ORDER BY open_time ASC;",
        (unified_symbol, timeframe),
    ).fetchall()
    closes  = [float(r[1]) for r in rows if r[1] is not None]
    volumes = [r[2] for r in rows]  # May contain None (CoinGecko OHLC has no volume)

    if len(closes) < min_candles:
        return {
            "status":            "degraded",
            "reason":            "insufficient_candles",
            "candles_available": len(closes),
            "candles_required":  min_candles,
            "score":             None,
        }

    # --- Compute all 5 factors ---
    rsi = compute_rsi(closes)

    _, _, mh = compute_macd(closes)

    ema_current, price_above_ema, slope_pct, ema_confirmed = compute_ema_slope(closes)

    sma20, mean_dev_pct, mean_signal = compute_mean_reversion(closes, window=20)

    rvol, rvol_signal = compute_relative_volume(volumes, recent_days=1, baseline_days=7)

    score = score_from_technical(
        rsi=rsi,
        macd_hist=mh,
        ema_slope_pct=slope_pct,
        ema_confirmed=ema_confirmed,
        mean_dev_pct=mean_dev_pct,
        rvol=rvol,
    )

    return {
        # RSI
        "rsi":              rsi,
        # MACD
        "macd_hist":        round(mh, 6) if mh is not None else None,
        # EMA-20 slope
        "ema_20":           round(ema_current, 6) if ema_current is not None else None,
        "price_above_ema":  price_above_ema,
        "ema_slope_pct":    slope_pct,
        "ema_confirmed":    ema_confirmed,
        # Mean reversion
        "sma_20":           round(sma20, 6) if sma20 is not None else None,
        "mean_dev_pct":     mean_dev_pct,
        "mean_signal":      mean_signal,
        # Relative volume
        "rvol":             rvol,
        "rvol_signal":      rvol_signal,
        # Summary
        "score":            score,
        "candles_used":     len(closes),
        "source":           "local_ohlcv",
    }


# Community / Social layer  (CoinGecko /coins/{id} -- free, no key)

# Simple in-process cache: {coin_id: (ts, data)}
_COMMUNITY_CACHE = {}
_COMMUNITY_TTL   = 3600  # 1 hour


def fetch_community_data(coin_id):
    """Fetch community and developer metrics from CoinGecko /coins/{id}."""
    import time as _t
    now = _t.time()
    if coin_id in _COMMUNITY_CACHE:
        ts, cached = _COMMUNITY_CACHE[coin_id]
        if now - ts < _COMMUNITY_TTL:
            return cached

    url = "https://api.coingecko.com/api/v3/coins/" + str(coin_id) +           "?localization=false&tickers=false&market_data=false"           "&community_data=true&developer_data=true&sparkline=false"
    data = _http_get_json(url, timeout=12)

    cd  = data.get("community_data") or {}
    dd  = data.get("developer_data")  or {}
    wl  = (data.get("watchlist_portfolio_users") or 0)

    reddit_subs   = int(cd.get("reddit_subscribers")        or 0)
    twitter_fol   = int(cd.get("twitter_followers")         or 0)
    commits_4w    = int(dd.get("commit_count_4_weeks")      or 0)
    github_stars  = int(dd.get("stars")                     or 0)
    watchers      = int(wl)

    score = score_from_community(reddit_subs, twitter_fol, commits_4w, github_stars, watchers)
    result = {
        "reddit_subscribers":   reddit_subs,
        "twitter_followers":    twitter_fol,
        "github_commits_4w":    commits_4w,
        "github_stars":         github_stars,
        "coingecko_watchers":   watchers,
        "score":                score,
        "source":               "coingecko/coins/" + coin_id,
    }
    _COMMUNITY_CACHE[coin_id] = (now, result)
    return result


def score_from_community(reddit_subs, twitter_fol, commits_4w, github_stars, watchers):
    """
    Score 0-100 from community engagement signals.
    Uses log scaling to handle range from small coins to BTC.
    Weights: watchers 35, twitter 25, reddit 20, commits 12, stars 8.
    Spec 7.4: community is structural signal, not sentiment.
    """
    import math as _m
    def _log_score(val, cap):
        if val <= 0:
            return 0.0
        return min(1.0, _m.log(val + 1) / _m.log(cap + 1))

    score = (
        _log_score(watchers,   2_000_000) * 35.0 +
        _log_score(twitter_fol, 5_000_000) * 25.0 +
        _log_score(reddit_subs, 3_000_000) * 20.0 +
        _log_score(commits_4w,      1_000) * 12.0 +
        _log_score(github_stars,  100_000) *  8.0
    )
    return round(max(0.0, min(100.0, score)), 2)


# Open Interest layer  (Binance /fapi/v1/openInterest -- public, no key)

def fetch_open_interest(binance_symbol):
    """
    Fetch current open interest from Binance USD-M futures.
    Source: /fapi/v1/openInterest (public, no key, ~1200 req/min).
    Returns openInterest (contracts) and openInterestValue (USD).
    """
    url = "https://fapi.binance.com/fapi/v1/openInterest?symbol=" + str(binance_symbol)
    data = _http_get_json(url, timeout=8)
    oi_contracts = float(data.get("openInterest", 0.0))
    # Also get OI history for trend (last 5 periods)
    hist_url = (
        "https://fapi.binance.com/futures/data/openInterestHist"
        "?symbol=" + str(binance_symbol) +
        "&period=1d&limit=5"
    )
    try:
        hist = _http_get_json(hist_url, timeout=8)
        oi_values = [float(h.get("sumOpenInterestValue", 0)) for h in hist]
    except Exception:
        oi_values = []

    return {
        "open_interest_contracts": oi_contracts,
        "oi_history_usd":          oi_values,
        "binance_symbol":          binance_symbol,
    }


def score_from_open_interest(oi_data, funding_rate=None):
    """
    Score 0-100 from open interest trend + funding rate context.
    OI trend: rising OI = conviction (bull or bear)
               falling OI = unwinding positions
    Combined with funding:
      Rising OI + positive funding = crowded long = risky (low score)
      Rising OI + negative funding = short squeeze setup (high score)
      Falling OI = caution (neutral)
    Standalone (no history): neutral 50.
    """
    oi_vals = oi_data.get("oi_history_usd", [])
    if len(oi_vals) < 2:
        return 50.0

    # Trend: compare last value vs 5-period average
    oi_avg   = sum(oi_vals) / len(oi_vals)
    oi_last  = oi_vals[-1]
    if oi_avg <= 0:
        return 50.0

    oi_change_pct = (oi_last - oi_avg) / oi_avg * 100.0

    # Base score from OI change direction
    # Moderate rise = healthy trend confirmation
    # Very high rise = overheating
    if oi_change_pct > 20.0:
        base = 60.0  # high but caution
    elif oi_change_pct > 5.0:
        base = 65.0  # healthy rise
    elif oi_change_pct > -5.0:
        base = 50.0  # neutral
    elif oi_change_pct > -20.0:
        base = 40.0  # declining
    else:
        base = 30.0  # strong decline = capitulation

    # Funding rate modifier
    if funding_rate is not None:
        fr = float(funding_rate)
        if oi_change_pct > 5.0 and fr > 0.0005:
            # Rising OI + high positive funding = crowded long
            base -= 15.0
        elif oi_change_pct > 5.0 and fr < -0.0001:
            # Rising OI + negative funding = squeeze potential
            base += 10.0

    return round(max(0.0, min(100.0, base)), 2)


# ===========================================================================
# LAYER CONTRACT (Spec 7.1) -- v6.2a
# Each layer is a class with formal interface:
#   init(config), capabilities(), fetch(top_set, timeframe, mode),
#   score(enriched_top_set), persist(run_context), report()
# ===========================================================================

from dataclasses import dataclass, field as dc_field


@dataclass
class LayerCapabilities:
    """What this layer can do."""
    name: str
    scope_key: str
    version: str
    primary_source: str
    fallback_sources: list
    free_tier: bool = True
    requires_api_key: bool = False
    max_symbols_per_run: int = 15
    supported_timeframes: list = dc_field(default_factory=lambda: ["spot"])
    configurable_params: dict = dc_field(default_factory=dict)


@dataclass
class LayerRunContext:
    """Context passed to layer during execution."""
    con: object  # sqlite3.Connection
    run_id: int
    selection_id: int
    snapshot_key: str
    timeframe: str
    vs_currency: str
    symbols: list
    log_cb: object = None


@dataclass
class LayerRunResult:
    """Result returned by layer after execution."""
    layer_name: str
    status: str  # ok, degraded, skipped, error
    scores: dict  # {symbol: {score: float, ...details}}
    errors: dict  # {symbol: error_msg}
    metadata: dict = dc_field(default_factory=dict)


class LayerAdapter:
    """Base class for all analysis layers. Spec 7.1."""
    NAME = "base"
    SCOPE_KEY = "base"
    VERSION = "v1"

    def init(self, config=None):
        """Initialise with optional config dict."""
        self._config = config or {}

    def get_config(self) -> dict:
        """Return current runtime config (Setup params). Spec C2.3."""
        caps = self.capabilities()
        merged = dict(caps.configurable_params)
        merged.update(self._config)
        return merged

    def set_config(self, overrides: dict):
        """Set runtime config overrides. Stored in run_params_json for audit."""
        if not hasattr(self, "_config"):
            self._config = {}
        self._config.update(overrides)

    def capabilities(self) -> LayerCapabilities:
        """Return what this layer can do."""
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY,
            version=self.VERSION, primary_source="unknown", fallback_sources=[])

    def fetch(self, ctx: LayerRunContext) -> dict:
        """Fetch raw data for symbols. Returns {symbol: raw_data}."""
        raise NotImplementedError

    def score(self, ctx: LayerRunContext, raw_data: dict) -> dict:
        """Score symbols. Returns {symbol: {score: float, ...}}."""
        raise NotImplementedError

    def persist(self, ctx: LayerRunContext, scores: dict) -> int:
        """Persist results to DB. Returns count persisted."""
        return 0

    def report(self) -> dict:
        """Return layer run summary."""
        return {"name": self.NAME, "version": self.VERSION}

    def run(self, ctx: LayerRunContext) -> LayerRunResult:
        """Full lifecycle: fetch -> score -> persist. Returns LayerRunResult."""
        log = ctx.log_cb or (lambda m: None)
        scores = {}
        errors = {}
        try:
            raw = self.fetch(ctx)
            scores = self.score(ctx, raw)
            self.persist(ctx, scores)
            n_ok = sum(1 for v in scores.values() if isinstance(v, dict) and v.get("score") is not None)
            status = "ok" if n_ok > 0 else "degraded"
        except Exception as exc:
            status = "error"
            errors["_global"] = str(exc)[:300]
            log(f"[{self.NAME}] ERROR: {str(exc)[:120]}")
        return LayerRunResult(
            layer_name=self.NAME, status=status,
            scores=scores, errors=errors,
            metadata=self.report())


class SpotBasicAdapter(LayerAdapter):
    NAME = "SpotBasic"
    SCOPE_KEY = "crypto_spot"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="coingecko", fallback_sources=["coinpaprika", "coincap"],
            configurable_params={"vs_currency": "usd", "coins_limit": 250})

    def fetch(self, ctx):
        return {}  # Data already in DB from pipeline

    def score(self, ctx, raw_data):
        n = compute_and_store_spot_basic(ctx.con, ctx.snapshot_key, ctx.timeframe)
        scores = {}
        for sym in ctx.symbols:
            r = ctx.con.execute(
                "SELECT base_score FROM market_snapshots "
                "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
                (ctx.snapshot_key, ctx.timeframe, sym)).fetchone()
            scores[sym] = {"score": float(r[0]) if r and r[0] is not None else None}
        return scores

    def persist(self, ctx, scores):
        return len(scores)


class DerivativesAdapter(LayerAdapter):
    NAME = "Derivatives"
    SCOPE_KEY = "crypto_derivatives"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="binance", fallback_sources=["bybit", "okx"],
            configurable_params={"overbought_threshold": 0.01, "oversold_threshold": -0.01})

    def fetch(self, ctx):
        cascade = make_funding_cascade(log_cb=ctx.log_cb)
        raw = {}
        for sym in ctx.symbols:
            bsym = to_binance_symbol(sym)
            if not bsym:
                raw[sym] = {"error": "no_perp_market"}
                continue
            try:
                result, src = cascade.fetch_one(sym)
                result["_source"] = src
                raw[sym] = result
            except Exception as exc:
                raw[sym] = {"error": str(exc)[:200]}
        cascade.log_to_db(ctx.con, ctx.run_id)
        ctx.con.commit()
        return raw

    def score(self, ctx, raw_data):
        scores = {}
        for sym, data in raw_data.items():
            if "error" in data:
                scores[sym] = {"score": None, "error": data["error"]}
            else:
                fr = data.get("funding_rate", 0.0)
                scores[sym] = {"funding_rate": fr, "score": float(score_from_funding_rate(fr)),
                               "_source": data.get("_source")}
        return scores


class OnChainAdapter(LayerAdapter):
    NAME = "OnChain"
    SCOPE_KEY = "onchain"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="blockchain.com", fallback_sources=["blockchair"],
            max_symbols_per_run=15,
            configurable_params={"btc_expected_tx": 350000, "eth_expected_tx": 1200000})

    def fetch(self, ctx):
        return {}  # Data ingested during pipeline phase

    def score(self, ctx, raw_data):
        scores = {}
        for sym in ctx.symbols:
            try:
                s = load_onchain_score(ctx.con, ctx.run_id, sym)
                if s is not None:
                    scores[sym] = {"score": s, "source": "blockchain.com+blockchair"}
                else:
                    scores[sym] = fetch_onchain_placeholder(sym)
            except Exception as exc:
                scores[sym] = {"score": None, "error": str(exc)[:120]}
        return scores


class InstitutionalAdapter(LayerAdapter):
    NAME = "Institutional"
    SCOPE_KEY = "institutions"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="yahoo_cme", fallback_sources=["yahoo_etf"],
            configurable_params={"cme_boost": 10.0, "etf_boost_per_product": 5.0})

    def fetch(self, ctx):
        return {}  # Data ingested during pipeline phase

    def score(self, ctx, raw_data):
        scores = {}
        for sym in ctx.symbols:
            try:
                s = load_institutional_score(ctx.con, ctx.run_id, sym)
                if s is not None:
                    scores[sym] = {"score": s, "source": "yahoo_cme+etf"}
                else:
                    scores[sym] = fetch_institutional_placeholder(sym)
            except Exception as exc:
                scores[sym] = {"score": None, "error": str(exc)[:120]}
        return scores


class MacroAdapter(LayerAdapter):
    NAME = "Macro"
    SCOPE_KEY = "macro"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="yahoo_finance", fallback_sources=["fred", "coingecko_global"],
            configurable_params={})

    def fetch(self, ctx):
        try:
            db_data = load_macro_from_db(ctx.con, ctx.run_id if ctx.run_id else -1)
            if db_data:
                return {"_global": db_data}
            g = fetch_coingecko_global()
            return {"_global": g}
        except Exception as exc:
            return {"_global": {"error": str(exc)[:200]}}

    def score(self, ctx, raw_data):
        g = raw_data.get("_global", {})
        if "error" in g:
            return {"_global": {"score": None, "error": g["error"]}}
        if "SP500" in g:
            s = score_macro_from_db(g)
            return {"_global": {"score": s, "source": "yahoo_finance+fred", **g}}
        elif "market_cap_change_percentage_24h_usd" in g:
            s = score_from_global(float(g["market_cap_change_percentage_24h_usd"]),
                                  float(g.get("btc_dominance", 50)))
            return {"_global": {"score": s, "source": "coingecko_global",
                    "market_cap_change_pct_24h": g.get("market_cap_change_percentage_24h_usd"),
                    "btc_dominance": g.get("btc_dominance")}}
        return {"_global": {"score": None}}


class SentimentAdapter(LayerAdapter):
    NAME = "Sentiment"
    SCOPE_KEY = "sentiment"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="alternative.me", fallback_sources=[],
            configurable_params={"supplemental_only": True})

    def fetch(self, ctx):
        try:
            db = load_sentiment_from_db(ctx.con, ctx.run_id if ctx.run_id else -1)
            fng = db.get("fear_greed_index", {})
            if fng.get("value") is not None:
                return {"_global": fng}
            live = fetch_fng_index()
            return {"_global": live}
        except Exception as exc:
            return {"_global": {"error": str(exc)[:200]}}

    def score(self, ctx, raw_data):
        fng = raw_data.get("_global", {})
        if "error" in fng:
            return {"_global": {"score": None, "error": fng["error"]}}
        val = fng.get("value")
        if val is None:
            return {"_global": {"score": None}}
        return {"_global": {"fng_value": int(val),
                "fng_classification": fng.get("classification", ""),
                "score": float(score_from_fng_value(int(val))),
                "source": fng.get("source", "alternative.me")}}


class TechnicalAdapter(LayerAdapter):
    NAME = "Technical"
    SCOPE_KEY = "technical"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="local_ohlcv", fallback_sources=[],
            supported_timeframes=["1d", "1h", "15m"],
            configurable_params={"rsi_period": 14, "macd_fast": 12, "macd_slow": 26,
                                 "min_candles": 60, "weights": "RSI 35%, MACD 25%, EMA 20%, MR 12%, RVOL 8%"})

    def fetch(self, ctx):
        return {}  # Uses local OHLCV from DB

    def score(self, ctx, raw_data):
        scores = {}
        # Multi-timeframe: try configured TFs, fall back to 1d
        timeframes_to_try = ["1d", "1h", "15m"]
        for sym in ctx.symbols:
            best_result = None
            for tf in timeframes_to_try:
                try:
                    result = fetch_technical_from_db(ctx.con, sym, timeframe=tf)
                    if result.get("score") is not None:
                        result["_timeframe"] = tf
                        if best_result is None:
                            best_result = result
                        else:
                            # Multi-TF: weighted average (1d=50%, 1h=30%, 15m=20%)
                            _TF_W = {"1d": 0.50, "1h": 0.30, "15m": 0.20}
                            if "_multi_tf" not in best_result:
                                best_result["_multi_tf"] = {best_result["_timeframe"]: best_result["score"]}
                            best_result["_multi_tf"][tf] = result["score"]
                except Exception:
                    pass
            if best_result and "_multi_tf" in best_result:
                _TF_W = {"1d": 0.50, "1h": 0.30, "15m": 0.20}
                wsum = sum(best_result["_multi_tf"].get(t, 0) * _TF_W.get(t, 0.1) for t in best_result["_multi_tf"])
                wtot = sum(_TF_W.get(t, 0.1) for t in best_result["_multi_tf"])
                best_result["score"] = round(wsum / wtot, 2) if wtot > 0 else best_result["score"]
                best_result["_scoring_mode"] = "multi_timeframe"
            scores[sym] = best_result or {"score": None, "error": "no_ohlcv_data"}
        return scores


class CommunityAdapter(LayerAdapter):
    NAME = "Community"
    SCOPE_KEY = "community"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="coingecko", fallback_sources=["coincap"],
            configurable_params={"rate_limit_sleep": 1.5})

    def fetch(self, ctx):
        cascade = make_community_cascade(log_cb=ctx.log_cb)
        raw = {}
        import time as _tc
        for sym in ctx.symbols:
            coin_id = sym.lower()
            try:
                result, src = cascade.fetch_one(sym, coin_id=coin_id)
                result["_source"] = src
                raw[sym] = result
            except Exception as exc:
                raw[sym] = {"error": str(exc)[:200], "score": None}
            _tc.sleep(1.5)
        return raw

    def score(self, ctx, raw_data):
        return raw_data  # Community cascade already returns scored data


class OpenInterestAdapter(LayerAdapter):
    NAME = "OpenInterest"
    SCOPE_KEY = "open_interest"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="binance", fallback_sources=["bybit"],
            configurable_params={})

    def fetch(self, ctx):
        cascade = make_oi_cascade(log_cb=ctx.log_cb)
        raw = {}
        for sym in ctx.symbols:
            bsym = to_binance_symbol(sym)
            if not bsym:
                raw[sym] = {"error": "no_perp_market", "score": None}
                continue
            try:
                data, src = cascade.fetch_one(sym)
                data["_source"] = src
                raw[sym] = data
            except Exception as exc:
                raw[sym] = {"error": str(exc)[:200], "score": None}
        cascade.log_to_db(ctx.con, ctx.run_id)
        ctx.con.commit()
        return raw

    def score(self, ctx, raw_data):
        scores = {}
        for sym, data in raw_data.items():
            if "error" in data:
                scores[sym] = data
            else:
                fr = None  # Could look up funding rate for enhanced scoring
                data["score"] = score_from_open_interest(data, fr)
                scores[sym] = data
        return scores


class FundamentalAdapter(LayerAdapter):
    NAME = "Fundamental"
    SCOPE_KEY = "fundamental"
    VERSION = "v6.2a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="github", fallback_sources=[],
            requires_api_key=False,
            configurable_params={"token_env": "NYOSIG_GITHUB_TOKEN"})

    def fetch(self, ctx):
        return {}  # Data ingested during pipeline

    def score(self, ctx, raw_data):
        scores = {}
        for sym in ctx.symbols:
            try:
                s = score_from_fundamental_db(ctx.con, ctx.run_id, sym) if ctx.run_id else None
                if s is not None:
                    scores[sym] = {"score": s, "source": "github"}
                else:
                    scores[sym] = {"status": "no_data", "score": None}
            except Exception as exc:
                scores[sym] = {"error": str(exc)[:120], "score": None}
        return scores


# --- Layer Adapter Registry ---
# --- v7.0a: Non-crypto adapters ---

class ForexSpotAdapter(LayerAdapter):
    """Forex spot screening via Yahoo Finance. v7.0a."""
    NAME = "ForexSpot"
    SCOPE_KEY = "forex_spot"
    VERSION = "v7.0a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="yahoo_finance", fallback_sources=[],
            max_symbols_per_run=20,
            supported_timeframes=["1d", "1h"],
            configurable_params={"pairs_limit": 20, "major_only": False})

    def fetch(self, ctx):
        return {}  # Data already fetched during pipeline

    def score(self, ctx, raw_data):
        scores = {}
        for sym in ctx.symbols:
            r = ctx.con.execute(
                "SELECT rank, price, change_24h_pct FROM market_snapshots "
                "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
                (ctx.snapshot_key, ctx.timeframe, sym)).fetchone()
            if r:
                s = score_forex_spot(r[0] or 1, r[1] or 0, r[2] or 0)
                scores[sym] = {"score": s, "source": "yahoo_finance"}
            else:
                scores[sym] = {"score": None, "error": "no_data"}
        return scores


class StocksSpotAdapter(LayerAdapter):
    """Equities/ETF screening via Yahoo Finance. v7.0a."""
    NAME = "StocksSpot"
    SCOPE_KEY = "stocks_spot"
    VERSION = "v7.0a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="yahoo_finance", fallback_sources=[],
            max_symbols_per_run=30,
            supported_timeframes=["1d"],
            configurable_params={"include_etfs": True, "include_blue_chips": True})

    def fetch(self, ctx):
        return {}

    def score(self, ctx, raw_data):
        scores = {}
        for sym in ctx.symbols:
            r = ctx.con.execute(
                "SELECT rank, price, mcap, change_24h_pct FROM market_snapshots "
                "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
                (ctx.snapshot_key, ctx.timeframe, sym)).fetchone()
            if r:
                s = score_stocks_spot(r[0] or 1, r[1] or 0, r[2] or 0, r[3] or 0)
                scores[sym] = {"score": s, "source": "yahoo_finance"}
            else:
                scores[sym] = {"score": None, "error": "no_data"}
        return scores


class MacroDashboardAdapter(LayerAdapter):
    """Macro instrument overview via Yahoo Finance. v7.0a."""
    NAME = "MacroDashboard"
    SCOPE_KEY = "macro_dashboard"
    VERSION = "v7.0a"

    def capabilities(self):
        return LayerCapabilities(
            name=self.NAME, scope_key=self.SCOPE_KEY, version=self.VERSION,
            primary_source="yahoo_finance", fallback_sources=["fred"],
            max_symbols_per_run=10,
            supported_timeframes=["1d"],
            configurable_params={})

    def fetch(self, ctx):
        return {}

    def score(self, ctx, raw_data):
        # Macro dashboard doesn't score per-symbol in the traditional sense.
        # Each instrument gets a "health" score based on its current level.
        scores = {}
        for sym in ctx.symbols:
            r = ctx.con.execute(
                "SELECT price FROM market_snapshots "
                "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
                (ctx.snapshot_key, ctx.timeframe, sym)).fetchone()
            if r and r[0]:
                # Simple presence score -- macro dashboard is informational
                scores[sym] = {"score": 50.0, "price": r[0], "source": "yahoo_finance",
                               "note": "Macro instruments are informational, not scored competitively"}
            else:
                scores[sym] = {"score": None, "error": "no_data"}
        return scores


LAYER_ADAPTERS = {
    # Crypto layers (original)
    "crypto_spot":        SpotBasicAdapter(),
    "crypto_derivatives": DerivativesAdapter(),
    "onchain":            OnChainAdapter(),
    "institutions":       InstitutionalAdapter(),
    "macro":              MacroAdapter(),
    "sentiment":          SentimentAdapter(),
    "technical":          TechnicalAdapter(),
    "community":          CommunityAdapter(),
    "open_interest":      OpenInterestAdapter(),
    "fundamental":        FundamentalAdapter(),
    # v7.0a: Non-crypto layers
    "forex_spot":         ForexSpotAdapter(),
    "stocks_spot":        StocksSpotAdapter(),
    "macro_dashboard":    MacroDashboardAdapter(),
}

def get_layer_capabilities() -> dict:
    """Return capabilities for all registered layers."""
    return {k: v.capabilities() for k, v in LAYER_ADAPTERS.items()}

def get_layer_adapter(scope_key: str) -> Optional[LayerAdapter]:
    """Return adapter for scope_key or None."""
    return LAYER_ADAPTERS.get(scope_key)


# ===========================================================================
# FEATURE LAYER (Spec 6.5) -- v6.3a
# Aggregates all layer scores into a normalised feature vector per symbol.
# ===========================================================================

# Canonical layer keys and their normalisation ranges (0-100 scale)
_FEATURE_KEYS = [
    "spot_basic", "derivatives_funding", "onchain", "institutions",
    "technical", "community", "open_interest", "fundamental",
    "macro_global", "sentiment_fng",
]


def build_feature_vector(reason_json: dict) -> dict:
    """
    Extract normalised feature vector from a symbol's reason_json.
    Each feature is clamped to 0-100. Missing features are None.
    Returns dict of feature_name -> normalised_value.
    """
    features = {}
    for key in _FEATURE_KEYS:
        val = reason_json.get(key)
        score = None
        if isinstance(val, dict):
            score = val.get("score")
        elif isinstance(val, (int, float)):
            score = float(val)
        if score is not None:
            score = max(0.0, min(100.0, float(score)))
        features[key] = score
    return features


def compute_norm_score(features: dict) -> Optional[float]:
    """
    Compute a single normalised score from a feature vector.
    Uses available features only (graceful degradation for missing layers).
    Returns 0-100 or None if no features available.
    """
    valid = [(k, v) for k, v in features.items() if v is not None]
    if not valid:
        return None
    return round(sum(v for _, v in valid) / len(valid), 2)


def persist_feature_vectors(con, run_id: int, selection_id: int, log_cb=None):
    """
    Build and store feature vectors for all symbols in a selection.
    Reads reason_json from topnow_selection_items, builds feature vector,
    stores in feature_vectors table. Returns count of vectors stored.
    """
    import json as _j
    if log_cb is None:
        log_cb = lambda m: None
    items = con.execute(
        "SELECT unified_symbol, reason_json FROM topnow_selection_items "
        "WHERE selection_id=? ORDER BY rank_in_selection ASC;",
        (selection_id,)
    ).fetchall()
    count = 0
    created = utc_now_iso()
    for sym, rj_str in items:
        rj = {}
        if rj_str:
            try:
                rj = _j.loads(rj_str)
            except Exception:
                pass
        features = build_feature_vector(rj)
        norm = compute_norm_score(features)
        con.execute(
            "INSERT INTO feature_vectors "
            "(run_id, selection_id, unified_symbol, feature_json, norm_score, created_utc) "
            "VALUES (?, ?, ?, ?, ?, ?);",
            (run_id, selection_id, sym, _j.dumps(features), norm, created),
        )
        count += 1
    con.commit()
    log_cb(f"FeatureLayer: {count} vectors stored")
    return count


# ===========================================================================
# PREDICTION LAYER (Spec 6.5) -- v6.3a
# Rule-based signal generation from feature vectors.
# Spec 7.4: OnChain + Derivatives > Sentiment in decision weight.
# ===========================================================================

# Signal definitions
SIGNAL_STRONG_BUY  = "strong_buy"
SIGNAL_BUY         = "buy"
SIGNAL_NEUTRAL     = "neutral"
SIGNAL_SELL        = "sell"
SIGNAL_STRONG_SELL = "strong_sell"

# Structural layers (spec 7.4: higher weight than sentiment)
_STRUCTURAL_KEYS = ["onchain", "derivatives_funding", "technical", "open_interest"]
# Supplemental layers (lower priority)
_SUPPLEMENTAL_KEYS = ["sentiment_fng", "community"]


def predict_signal(features: dict) -> tuple:
    """
    Generate prediction signal from feature vector.
    Returns (signal, confidence, reasoning_dict).

    Rules (spec 7.4 compliant -- structural > sentiment):
      1. If structural average >= 70 and sentiment < 50 -> strong_buy (contrarian)
      2. If structural average >= 65 -> buy
      3. If structural average <= 35 -> sell
      4. If structural average <= 25 and sentiment > 70 -> strong_sell (contrarian)
      5. Otherwise -> neutral
    """
    # Compute structural average (only available features)
    structural_scores = [features[k] for k in _STRUCTURAL_KEYS
                         if features.get(k) is not None]
    supplemental_scores = [features[k] for k in _SUPPLEMENTAL_KEYS
                           if features.get(k) is not None]
    all_scores = [v for v in features.values() if v is not None]

    if not structural_scores:
        return SIGNAL_NEUTRAL, 0.0, {"reason": "insufficient_structural_data"}

    struct_avg = sum(structural_scores) / len(structural_scores)
    suppl_avg = sum(supplemental_scores) / len(supplemental_scores) if supplemental_scores else 50.0
    overall_avg = sum(all_scores) / len(all_scores) if all_scores else 50.0

    reasoning = {
        "structural_avg": round(struct_avg, 2),
        "supplemental_avg": round(suppl_avg, 2),
        "overall_avg": round(overall_avg, 2),
        "structural_layers_used": len(structural_scores),
        "total_layers_used": len(all_scores),
    }

    # Rule 1: Strong contrarian buy (structural strong + sentiment weak)
    if struct_avg >= 70 and suppl_avg < 50:
        confidence = min(0.95, struct_avg / 100.0)
        reasoning["rule"] = "structural_strong_sentiment_weak_contrarian"
        return SIGNAL_STRONG_BUY, round(confidence, 3), reasoning

    # Rule 2: Buy (structural above threshold)
    if struct_avg >= 65:
        confidence = min(0.85, struct_avg / 100.0)
        reasoning["rule"] = "structural_above_65"
        return SIGNAL_BUY, round(confidence, 3), reasoning

    # Rule 4: Strong contrarian sell (structural weak + sentiment euphoric)
    if struct_avg <= 25 and suppl_avg > 70:
        confidence = min(0.90, (100.0 - struct_avg) / 100.0)
        reasoning["rule"] = "structural_weak_sentiment_euphoric_contrarian"
        return SIGNAL_STRONG_SELL, round(confidence, 3), reasoning

    # Rule 3: Sell (structural below threshold)
    if struct_avg <= 35:
        confidence = min(0.80, (100.0 - struct_avg) / 100.0)
        reasoning["rule"] = "structural_below_35"
        return SIGNAL_SELL, round(confidence, 3), reasoning

    # Rule 5: Neutral
    confidence = 1.0 - abs(struct_avg - 50.0) / 50.0
    reasoning["rule"] = "neutral_zone"
    return SIGNAL_NEUTRAL, round(max(0.1, confidence), 3), reasoning


def persist_predictions(con, run_id: int, selection_id: int, log_cb=None):
    """
    Generate and store predictions for all symbols in a selection.
    Reads feature_vectors, applies predict_signal, stores in predictions.
    Returns count of predictions generated.
    """
    import json as _j
    if log_cb is None:
        log_cb = lambda m: None
    rows = con.execute(
        "SELECT unified_symbol, feature_json FROM feature_vectors "
        "WHERE run_id=? AND selection_id=? ORDER BY norm_score DESC;",
        (run_id, selection_id)
    ).fetchall()
    count = 0
    created = utc_now_iso()
    signal_summary = {}
    for sym, fj_str in rows:
        features = {}
        try:
            features = _j.loads(fj_str)
        except Exception:
            pass
        signal, confidence, reasoning = predict_signal(features)
        con.execute(
            "INSERT INTO predictions "
            "(run_id, selection_id, unified_symbol, signal, confidence, reasoning_json, created_utc) "
            "VALUES (?, ?, ?, ?, ?, ?, ?);",
            (run_id, selection_id, sym, signal, confidence, _j.dumps(reasoning), created),
        )
        signal_summary[signal] = signal_summary.get(signal, 0) + 1
        count += 1
    con.commit()
    log_cb(f"PredictionLayer: {count} predictions -- {signal_summary}")
    return count


# ===========================================================================
# TRADE PLAN LAYER (Spec 6.5) -- v6.3a
# Generates entry/exit zones, position sizing, and stop-loss levels.
# ===========================================================================

def generate_trade_plan(
    symbol: str,
    signal: str,
    confidence: float,
    current_price: float,
    features: dict,
    risk_pct: float = 2.0,
) -> dict:
    """
    Generate a trade plan for one symbol based on prediction + technical data.

    Args:
        signal: buy/sell/strong_buy/strong_sell/neutral
        confidence: 0-1
        current_price: latest spot price
        features: normalised feature vector
        risk_pct: max risk per position as % of portfolio (default 2%)

    Returns plan dict with entry zones, targets, stop-loss, position sizing.
    """
    if signal == SIGNAL_NEUTRAL or current_price <= 0:
        return {
            "direction": "hold", "entry_zone_low": None, "entry_zone_high": None,
            "stop_loss": None, "target_1": None, "target_2": None,
            "position_pct": 0.0, "risk_score": 0.0,
            "note": "Neutral signal -- no trade recommended",
        }

    is_long = signal in (SIGNAL_BUY, SIGNAL_STRONG_BUY)
    direction = "long" if is_long else "short"

    # Entry zone: ±1-3% from current price depending on confidence
    spread = (1.0 + (1.0 - confidence) * 2.0) / 100.0  # 1-3% spread
    if is_long:
        entry_low = round(current_price * (1.0 - spread * 1.5), 6)
        entry_high = round(current_price * (1.0 - spread * 0.3), 6)
    else:
        entry_low = round(current_price * (1.0 + spread * 0.3), 6)
        entry_high = round(current_price * (1.0 + spread * 1.5), 6)

    # Stop-loss based on technical data if available, otherwise fixed %
    tech_score = features.get("technical")
    volatility_factor = 1.0
    if tech_score is not None:
        # Higher tech score = more trend confirmation = tighter stop
        volatility_factor = max(0.5, 1.5 - tech_score / 100.0)

    stop_pct = 0.05 * volatility_factor  # 2.5-7.5% depending on tech
    if is_long:
        stop_loss = round(entry_low * (1.0 - stop_pct), 6)
    else:
        stop_loss = round(entry_high * (1.0 + stop_pct), 6)

    # Targets: 2:1 and 3:1 risk-reward
    risk_amount = abs(entry_low - stop_loss) if is_long else abs(entry_high - stop_loss)
    if is_long:
        target_1 = round(entry_high + risk_amount * 2.0, 6)
        target_2 = round(entry_high + risk_amount * 3.0, 6)
    else:
        target_1 = round(entry_low - risk_amount * 2.0, 6)
        target_2 = round(entry_low - risk_amount * 3.0, 6)

    # Position sizing: confidence-weighted, capped by risk_pct
    # Higher confidence + strong signal = larger position
    signal_mult = {"strong_buy": 1.5, "buy": 1.0, "sell": 1.0, "strong_sell": 1.5}
    raw_position = risk_pct * confidence * signal_mult.get(signal, 1.0)
    position_pct = round(min(raw_position, risk_pct * 2.0), 2)

    # Risk score: composite of position size and stop distance
    risk_score = round(position_pct * stop_pct * 100, 2)

    return {
        "direction": direction,
        "entry_zone_low": entry_low,
        "entry_zone_high": entry_high,
        "stop_loss": stop_loss,
        "target_1": target_1,
        "target_2": target_2,
        "position_pct": position_pct,
        "risk_score": risk_score,
        "confidence": confidence,
        "signal": signal,
        "volatility_factor": round(volatility_factor, 3),
        "stop_pct": round(stop_pct * 100, 2),
    }


def persist_trade_plans(con, run_id: int, selection_id: int, log_cb=None):
    """
    Generate and store trade plans for all predicted symbols.
    Reads predictions + feature_vectors + current prices.
    Returns count of trade plans generated.
    """
    import json as _j
    if log_cb is None:
        log_cb = lambda m: None

    # Load predictions for this run
    preds = con.execute(
        "SELECT unified_symbol, signal, confidence FROM predictions "
        "WHERE run_id=? AND selection_id=?;",
        (run_id, selection_id)
    ).fetchall()
    if not preds:
        log_cb("TradePlan: no predictions found")
        return 0

    # Load feature vectors
    fv_rows = con.execute(
        "SELECT unified_symbol, feature_json FROM feature_vectors "
        "WHERE run_id=? AND selection_id=?;",
        (run_id, selection_id)
    ).fetchall()
    fv_map = {}
    for sym, fj in fv_rows:
        try:
            fv_map[sym] = _j.loads(fj)
        except Exception:
            fv_map[sym] = {}

    # Load current prices from latest market_snapshots
    snap_row = con.execute(
        "SELECT snapshot_id FROM topnow_selection WHERE selection_id=?;",
        (selection_id,)
    ).fetchone()
    snap_key = snap_row[0] if snap_row else ""
    price_map = {}
    if snap_key:
        prices = con.execute(
            "SELECT unified_symbol, price FROM market_snapshots "
            "WHERE snapshot_id=? AND timeframe='spot';",
            (snap_key,)
        ).fetchall()
        price_map = {sym: float(p) for sym, p in prices if p}

    count = 0
    created = utc_now_iso()
    for sym, signal, confidence in preds:
        price = price_map.get(sym, 0)
        features = fv_map.get(sym, {})
        plan = generate_trade_plan(sym, signal, confidence or 0.0, price, features)
        con.execute(
            "INSERT INTO trade_plans "
            "(run_id, selection_id, unified_symbol, direction, "
            "entry_zone_low, entry_zone_high, stop_loss, target_1, target_2, "
            "position_pct, risk_score, plan_json, created_utc) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            (run_id, selection_id, sym, plan["direction"],
             plan["entry_zone_low"], plan["entry_zone_high"],
             plan["stop_loss"], plan["target_1"], plan["target_2"],
             plan["position_pct"], plan["risk_score"],
             _j.dumps(plan), created),
        )
        count += 1
    con.commit()
    log_cb(f"TradePlan: {count} plans generated")
    return count


def store_run_params(con, run_id: int, layer_configs: dict):
    """Store layer Setup configs into run for audit (spec C2.3)."""
    import json as _j
    params_json = _j.dumps(layer_configs, ensure_ascii=True)
    con.execute(
        "UPDATE runs SET params_json=? WHERE run_id=?;",
        (params_json, run_id))
    con.commit()


# Multi-layer composite preview

def prepare_and_store_composite_preview(
    con: sqlite3.Connection,
    selection_id: int,
    scopes: List[str],
    timeframe: str = "spot",
    run_id: Optional[int] = None,
    progress_cb=None,
) -> dict:
    """Compute composite_preview and reason_json for all items in selection."""
    _LAYERS_TOTAL = 7
    _layer_step   = [0]  # mutable counter
    def _prog(name):
        _layer_step[0] += 1
        if progress_cb:
            progress_cb(_layer_step[0], _LAYERS_TOTAL, name)
    scopes_set = {s.strip() for s in (scopes or []) if s and str(s).strip()}

    # --- Resolve snapshot_key from selection ---
    row = con.execute(
        "SELECT snapshot_id FROM topnow_selection WHERE selection_id=?;",
        (selection_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"selection_id not found: {selection_id}")
    snapshot_key: str = row[0]

    # --- Layer: spot_basic ---
    _prog("spot_basic")
    spot_ok = False
    if "crypto_spot" in scopes_set:
        n = compute_and_store_spot_basic(con, snapshot_key, timeframe)
        spot_ok = n > 0

    # --- Layer: sentiment -- READ from DB (ingested during pipeline v5.0a) ---
    _prog("sentiment")
    sentiment: Optional[dict] = None
    if "sentiment" in scopes_set:
        try:
            # Always load from DB -- function has internal fallback to latest run
            sent_db = load_sentiment_from_db(con, run_id if run_id else -1)
            fng_row = sent_db.get("fear_greed_index", {})
            fng_val = fng_row.get("value")
            if fng_val is not None:
                sentiment = {
                    "fng_value":          int(fng_val),
                    "fng_classification": fng_row.get("classification", ""),
                    "score": float(score_from_fng_value(int(fng_val))),
                    "source":             fng_row.get("source", "db"),
                }
            else:
                # Live fallback if DB empty (first run or ingestion failed)
                fng = fetch_fng_index()
                sentiment = {
                    "fng_value":          int(fng["value"]),
                    "fng_classification": fng.get("classification", ""),
                    "score":              float(score_from_fng_value(int(fng["value"]))),
                    "source":             fng.get("source"),
                }
        except Exception as exc:
            sentiment = {"error": str(exc)[:300], "score": None}

    # --- Layer: macro -- READ from DB (Yahoo + FRED, v5.0a) ---
    _prog("macro")
    macro: Optional[dict] = None
    if "macro" in scopes_set:
        try:
            # Always load from DB -- function has internal fallback to latest run
            macro_db = load_macro_from_db(con, run_id if run_id else -1)
            if macro_db:
                macro_score = score_macro_from_db(macro_db)
                macro = {
                    "SP500":    macro_db.get("SP500"),
                    "DXY":      macro_db.get("DXY"),
                    "VIX":      macro_db.get("VIX"),
                    "US10Y":    macro_db.get("US10Y"),
                    "FEDFUNDS": macro_db.get("FEDFUNDS"),
                    "score":    macro_score,
                    "source":   "yahoo_finance+fred",
                }
            else:
                # Fallback to CoinGecko global if DB empty
                g = fetch_coingecko_global()
                macro = {
                    "market_cap_change_pct_24h": float(g["market_cap_change_percentage_24h_usd"]),
                    "btc_dominance": float(g["btc_dominance"]),
                    "eth_dominance": float(g["eth_dominance"]),
                    "score": float(score_from_global(
                        float(g["market_cap_change_percentage_24h_usd"]),
                        float(g["btc_dominance"]),
                    )),
                    "source": g.get("source"),
                }
        except Exception as exc:
            macro = {"error": str(exc)[:300], "score": None}

    # --- Layer: derivatives funding rate -- CASCADE (Binance->Bybit->OKX) ---
    _prog("derivatives")
    funding_scores: Dict[str, dict] = {}
    if "crypto_derivatives" in scopes_set:
        syms = con.execute(
            "SELECT unified_symbol FROM topnow_selection_items "
            "WHERE selection_id=? ORDER BY rank_in_selection ASC;",
            (selection_id,),
        ).fetchall()
        _fund_cascade = make_funding_cascade(log_cb=lambda m: None)
        for (sym,) in syms:
            bsym = to_binance_symbol(sym)
            if not bsym:
                funding_scores[sym] = {"error": "no_perp_market", "score": None}
                continue
            try:
                result, src = _fund_cascade.fetch_one(sym)
                fr = result.get("funding_rate", 0.0)
                funding_scores[sym] = {
                    "funding_rate":   fr,
                    "score":          float(score_from_funding_rate(fr)),
                    "_source":        src,
                }
            except Exception as exc:
                funding_scores[sym] = {"error": str(exc)[:200], "score": None}
        # Persist cascade provenance
        _fund_cascade.log_to_db(con, run_id)
        con.commit()

    # --- Layer: technical (RSI + MACD from local OHLCV -- no API call) ---
    _prog("technical")
    technical_scores: Dict[str, dict] = {}
    if "technical" in scopes_set:
        syms = con.execute(
            "SELECT unified_symbol FROM topnow_selection_items "
            "WHERE selection_id=? ORDER BY rank_in_selection ASC;",
            (selection_id,),
        ).fetchall()
        for (sym,) in syms:
            try:
                result = fetch_technical_from_db(con, sym, timeframe="1d")
                technical_scores[sym] = result
            except Exception as exc:
                technical_scores[sym] = {"error": str(exc)[:200], "score": None}

    # --- Layer: community (CoinGecko /coins/{id} per symbol) ---
    _prog("community")
    community_scores: Dict[str, dict] = {}
    if "community" in scopes_set:
        import time as _tc
        # Need coin_id for each symbol -- look up from raw market data
        id_map = {}
        try:
            raw_rows = con.execute(
                "SELECT ms.unified_symbol, rs.response_json "
                "FROM market_snapshots ms "
                "JOIN raw_snapshots rs ON rs.snapshot_key=ms.snapshot_id "
                "WHERE ms.snapshot_id=? AND ms.timeframe=? "
                "LIMIT 200;",
                (snapshot_key, timeframe),
            ).fetchall()
            import json as _json
            for sym2, rj in raw_rows:
                if not rj:
                    continue
                try:
                    items = _json.loads(rj)
                    if isinstance(items, list):
                        for it in items:
                            s2 = (it.get("symbol") or "").upper()
                            if s2 == sym2 and it.get("id"):
                                id_map[sym2] = it["id"]
                                break
                except Exception:
                    pass
        except Exception:
            pass

        syms = con.execute(
            "SELECT unified_symbol FROM topnow_selection_items "
            "WHERE selection_id=? ORDER BY rank_in_selection ASC;",
            (selection_id,),
        ).fetchall()
        _comm_cascade = make_community_cascade(log_cb=lambda m: None)
        for (sym,) in syms:
            coin_id = id_map.get(sym, sym.lower())
            try:
                result, src = _comm_cascade.fetch_one(sym, coin_id=coin_id)
                result["_source"] = src
                community_scores[sym] = result
            except Exception as exc:
                community_scores[sym] = {"error": str(exc)[:200], "score": None}
            _tc.sleep(1.5)  # CoinGecko free rate limit

    # --- Layer: open interest -- CASCADE (Binance->Bybit) ---
    _prog("open")
    oi_scores: Dict[str, dict] = {}
    if "open_interest" in scopes_set:
        syms = con.execute(
            "SELECT unified_symbol FROM topnow_selection_items "
            "WHERE selection_id=? ORDER BY rank_in_selection ASC;",
            (selection_id,),
        ).fetchall()
        _oi_cascade = make_oi_cascade(log_cb=lambda m: None)
        for (sym,) in syms:
            bsym = to_binance_symbol(sym)
            if not bsym:
                oi_scores[sym] = {"error": "no_perp_market", "score": None}
                continue
            try:
                oi_data, src = _oi_cascade.fetch_one(sym)
                fr = funding_scores.get(sym, {}).get("funding_rate")
                oi_data["score"] = score_from_open_interest(oi_data, fr)
                oi_data["_source"] = src
                oi_scores[sym] = oi_data
            except Exception as exc:
                oi_scores[sym] = {"error": str(exc)[:200], "score": None}
        # Persist cascade provenance
        _oi_cascade.log_to_db(con, run_id)
        con.commit()

    # --- Compute composite per symbol ---
    items = con.execute(
        "SELECT rank_in_selection, unified_symbol "
        "FROM topnow_selection_items WHERE selection_id=? "
        "ORDER BY rank_in_selection ASC;",
        (selection_id,),
    ).fetchall()

    updated = 0
    for _rank, sym in items:
        # --- MERGE: Load existing reason_json so previous layer results are preserved ---
        # Bug fix: without this, each single-scope call overwrites all previous layers
        _existing = con.execute(
            "SELECT reason_json FROM topnow_selection_items "
            "WHERE selection_id=? AND unified_symbol=?;",
            (selection_id, sym),
        ).fetchone()
        reason: dict = {}
        if _existing and _existing[0]:
            try:
                reason = json.loads(_existing[0])
            except Exception:
                reason = {}
        # Union scopes: keep all previously run + new
        reason["scopes"] = sorted(set(reason.get("scopes", [])) | scopes_set)

        # --- Now update reason with new data from THIS call's scopes ---
        parts: List[float] = []

        # spot_basic
        if spot_ok:
            r2 = con.execute(
                "SELECT base_score FROM market_snapshots "
                "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
                (snapshot_key, timeframe, sym),
            ).fetchone()
            if r2 and r2[0] is not None:
                parts.append(float(r2[0]))
                reason["spot_basic"] = float(r2[0])

        # sentiment
        if sentiment:
            reason["sentiment_fng"] = sentiment
            if sentiment.get("score") is not None:
                parts.append(float(sentiment["score"]))

        # macro
        if macro:
            reason["macro_global"] = macro
            if macro.get("score") is not None:
                parts.append(float(macro["score"]))

        # funding
        if sym in funding_scores:
            fs = funding_scores[sym]
            reason["derivatives_funding"] = fs
            if fs.get("score") is not None:
                parts.append(float(fs["score"]))

        # onchain - real scoring (v6.1a: BTC blockchain.com + multi-asset Blockchair)
        if "onchain" in scopes_set:
            try:
                if run_id:
                    oc_score = load_onchain_score(con, run_id, sym)
                    if oc_score is not None:
                        reason["onchain"] = {"score": oc_score, "source": "blockchain.com+blockchair"}
                        parts.append(oc_score)
                    else:
                        reason["onchain"] = fetch_onchain_placeholder(sym)
                else:
                    reason["onchain"] = fetch_onchain_placeholder(sym)
            except Exception as _oe:
                reason["onchain"] = {"error": str(_oe)[:120], "score": None}

        # institutions - real scoring (v6.1a: CME futures + ETF via Yahoo Finance)
        if "institutions" in scopes_set:
            try:
                if run_id:
                    inst_score = load_institutional_score(con, run_id, sym)
                    if inst_score is not None:
                        reason["institutions"] = {"score": inst_score, "source": "yahoo_cme+etf"}
                        parts.append(inst_score)
                    else:
                        reason["institutions"] = fetch_institutional_placeholder(sym)
                else:
                    reason["institutions"] = fetch_institutional_placeholder(sym)
            except Exception as _ie:
                reason["institutions"] = {"error": str(_ie)[:120], "score": None}

        # technical (RSI + MACD from local OHLCV)
        if sym in technical_scores:
            td = technical_scores[sym]
            reason["technical"] = td
            if td.get("score") is not None:
                parts.append(float(td["score"]))

        # community (CoinGecko social/dev metrics)
        if sym in community_scores:
            cd = community_scores[sym]
            reason["community"] = cd
            if cd.get("score") is not None:
                parts.append(float(cd["score"]))

        # open interest (Binance futures OI)
        if sym in oi_scores:
            oi = oi_scores[sym]
            reason["open_interest"] = oi
            if oi.get("score") is not None:
                parts.append(float(oi["score"]))

        # fundamental (GitHub dev activity -- v5.0a)
        if "fundamental" in scopes_set:
            try:
                fund_score = score_from_fundamental_db(con, run_id, sym) if run_id else None
                if fund_score is not None:
                    reason["fundamental"] = {"score": fund_score, "source": "github"}
                    parts.append(fund_score)
                else:
                    reason["fundamental"] = {"status": "no_data", "score": None}
            except Exception as _fe:
                reason["fundamental"] = {"error": str(_fe)[:120], "score": None}

        # --- Recompute composite from ALL score-bearing fields in merged reason ---
        # Task 8: weighted average using COMPOSITE_WEIGHTS
        # Weights sum to ~1.0 across fully-scored run; partial runs use available layers only
        _SCORE_KEYS = [
            "spot_basic", "sentiment_fng", "macro_global", "derivatives_funding",
            "technical", "community", "open_interest", "onchain", "fundamental",
            "institutions",
        ]
        # Task 8: canonical weights (arch spec 10.2)
        COMPOSITE_WEIGHTS = {
            "spot_basic":            0.30,
            "derivatives_funding":   0.15,
            "onchain":               0.15,
            "technical":             0.12,
            "community":             0.08,
            "open_interest":         0.08,
            "fundamental":           0.05,
            "macro_global":          0.04,
            "sentiment_fng":         0.02,
            "institutions":          0.01,
        }
        weighted_sum = 0.0
        weight_total = 0.0
        weights_used = {}
        for _sk in _SCORE_KEYS:
            v = reason.get(_sk)
            s = None
            if isinstance(v, dict):
                s = v.get("score")
            elif isinstance(v, (int, float)):
                s = float(v)
            if s is not None and isinstance(s, (int, float)):
                w = COMPOSITE_WEIGHTS.get(_sk, 0.01)
                weighted_sum += float(s) * w
                weight_total += w
                weights_used[_sk] = round(w, 3)
        composite = round(weighted_sum / weight_total, 2) if weight_total > 0 else None
        # Store weights used so audit can verify how composite was computed
        reason["_weights"] = weights_used
        reason["_weight_total"] = round(weight_total, 3)

        con.execute(
            "UPDATE topnow_selection_items "
            "SET composite_preview=?, reason_json=? "
            "WHERE selection_id=? AND unified_symbol=?;",
            (composite, json.dumps(reason, ensure_ascii=True), selection_id, sym),
        )
        updated += 1

    # --- Persist layer_results for provenance ---
    if run_id is not None:
        snap_ref2 = None
        try:
            snap_ref2 = get_snapshot_ref(con, snapshot_key)
        except Exception:
            pass
        for _rank, sym in items:
            rj_row = con.execute(
                "SELECT reason_json FROM topnow_selection_items "
                "WHERE selection_id=? AND unified_symbol=?;",
                (selection_id, sym),
            ).fetchone()
            reason_data = {}
            if rj_row and rj_row[0]:
                try:
                    reason_data = json.loads(rj_row[0])
                except Exception:
                    pass
            comp_row = con.execute(
                "SELECT composite_preview FROM topnow_selection_items "
                "WHERE selection_id=? AND unified_symbol=?;",
                (selection_id, sym),
            ).fetchone()
            comp_val = comp_row[0] if comp_row else None
            for layer_name, layer_key in [
                ("spot_basic",             "spot_basic"),
                ("sentiment_fng",          "sentiment_fng"),
                ("macro_global",           "macro_global"),
                ("derivatives_funding",    "derivatives_funding"),
                ("onchain",                "onchain"),
                ("institutions",           "institutions"),
                ("technical",              "technical"),
                ("community",              "community"),
                ("open_interest",          "open_interest"),
            ]:
                ld = reason_data.get(layer_key)
                if ld is None:
                    continue
                ls = None
                if isinstance(ld, dict):
                    ls = ld.get("score")
                elif isinstance(ld, (int, float)):
                    ls = float(ld)
                status = "ok" if ls is not None else "degraded"
                upsert_layer_result(
                    con=con,
                    run_id=run_id,
                    snapshot_key=snapshot_key,
                    snapshot_ref=snap_ref2,
                    unified_symbol=sym,
                    layer_name=layer_name,
                    timeframe="spot",
                    layer_score=float(ls) if ls is not None else None,
                    layer_status=status,
                    raw_data_json=json.dumps(ld, ensure_ascii=True) if isinstance(ld, dict) else None,
                )
            # composite
            upsert_layer_result(
                con=con,
                run_id=run_id,
                snapshot_key=snapshot_key,
                snapshot_ref=snap_ref2,
                unified_symbol=sym,
                layer_name="composite",
                timeframe="spot",
                layer_score=float(comp_val) if comp_val is not None else None,
                layer_status="ok" if comp_val is not None else "degraded",
            )

    con.commit()

    # sentiment_ok / macro_ok tri-state:
    #   True  = in scope + scored successfully
    #   None  = not in scope (not relevant for this call)
    #   False = in scope + failed to score
    _sent_ok = (
        True  if (sentiment and sentiment.get("score") is not None) else
        None  if "sentiment" not in scopes_set else
        False
    )
    _macro_ok = (
        True  if (macro and macro.get("score") is not None) else
        None  if "macro" not in scopes_set else
        False
    )
    return {
        "selection_id":       selection_id,
        "snapshot_key":       snapshot_key,
        "updated_items":      updated,
        "scopes":             sorted(scopes_set),
        "sentiment_ok":       _sent_ok,
        "macro_ok":           _macro_ok,
        "funding_symbols":    len(funding_scores),
        "technical_symbols":  sum(1 for v in technical_scores.values() if v.get("score") is not None),
        "community_symbols":  sum(1 for v in community_scores.values() if v.get("score") is not None),
        "oi_symbols":         sum(1 for v in oi_scores.values()        if v.get("score") is not None),
        "fundamental_symbols": sum(
            1 for sym in (community_scores or funding_scores)
            if score_from_fundamental_db(con, run_id if run_id else -1, sym) is not None
        ) if run_id else 0,
    }

# ---- watchlist ----
"""
core_v4_0a/watchlist.py
NyoSig_Analysator v4.0a  --  Watchlist CRUD + metrics refresh + alerts.
Schema is managed by ensure_schema() in db.py.
"""

import sqlite3
from typing import List, Optional, Tuple



def add_watch(
    con: sqlite3.Connection,
    symbol: str,
    tag: str,
    stage: str,
    now_utc: str,
    entry_snapshot_id: str = "",
    entry_score: float = 0.0,
) -> int:
    sym = symbol.strip().upper()
    con.execute(
        """INSERT OR IGNORE INTO watchlist
           (unified_symbol, tag, stage, tracking_since_utc, entry_snapshot_id, entry_score)
           VALUES (?,?,?,?,?,?);""",
        (sym, tag or "", stage or "new", now_utc, entry_snapshot_id or "", float(entry_score)),
    )
    row = con.execute(
        "SELECT watch_id FROM watchlist WHERE unified_symbol=?;", (sym,)
    ).fetchone()
    if not row:
        raise RuntimeError(f"Failed to add {sym} to watchlist")
    wid = int(row[0])
    con.execute(
        "INSERT OR IGNORE INTO watchlist_metrics(watch_id) VALUES (?);", (wid,)
    )
    return wid


def remove_watch(con: sqlite3.Connection, watch_id: int) -> None:
    con.execute("DELETE FROM watchlist WHERE watch_id=?;", (int(watch_id),))


def list_watch(con: sqlite3.Connection) -> List[Tuple]:
    return con.execute(
        """SELECT w.watch_id, w.unified_symbol, w.tag, w.stage,
                  w.tracking_since_utc, w.entry_snapshot_id, w.entry_score,
                  m.last_refreshed_utc, m.last_price, m.last_chg24_pct, m.last_score
           FROM watchlist w
           LEFT JOIN watchlist_metrics m ON m.watch_id=w.watch_id
           ORDER BY w.tracking_since_utc DESC;"""
    ).fetchall()


def refresh_watch(
    con: sqlite3.Connection,
    snapshot_key: str,
    timeframe: str,
    now_utc: str,
    alert_drop_points: float = 15.0,
) -> int:
    """
    Refresh metrics for each watchlist symbol from market_snapshots.
    Creates a warning alert if score drops by >= alert_drop_points.
    Returns number of symbols refreshed.
    """
    items = con.execute(
        "SELECT watch_id, unified_symbol FROM watchlist;"
    ).fetchall()
    n = 0
    for wid, sym in items:
        row = con.execute(
            "SELECT rank, mcap, vol24, change_24h_pct, price "
            "FROM market_snapshots "
            "WHERE snapshot_id=? AND timeframe=? AND unified_symbol=?;",
            (snapshot_key, timeframe, sym),
        ).fetchone()
        if not row:
            continue
        rank, mcap, vol24, chg24, price = row
        new_score = float(spot_basic_score(rank, mcap, vol24, chg24))
        old = con.execute(
            "SELECT last_score FROM watchlist_metrics WHERE watch_id=?;", (wid,)
        ).fetchone()
        old_score: Optional[float] = float(old[0]) if old and old[0] is not None else None
        con.execute(
            """INSERT OR REPLACE INTO watchlist_metrics
               (watch_id, last_refreshed_utc, last_price, last_chg24_pct, last_score)
               VALUES (?,?,?,?,?);""",
            (wid, now_utc, price, chg24, new_score),
        )
        if old_score is not None and (old_score - new_score) >= float(alert_drop_points):
            con.execute(
                """INSERT INTO watchlist_alerts
                   (watch_id, created_utc, severity, message)
                   VALUES (?,?,?,?);""",
                (wid, now_utc, "warning",
                 f"Score drop {old_score:.1f} -> {new_score:.1f} (>= {alert_drop_points:.1f})"),
            )
        n += 1
    return n


def list_alerts(con: sqlite3.Connection, limit: int = 100) -> List[Tuple]:
    return con.execute(
        """SELECT a.alert_id, a.created_utc, a.severity, w.unified_symbol, a.message
           FROM watchlist_alerts a
           JOIN watchlist w ON w.watch_id=a.watch_id
           ORDER BY a.alert_id DESC LIMIT ?;""",
        (int(limit),),
    ).fetchall()

# ---- diff ----
"""
core_v4_0a/diff.py
NyoSig_Analysator v4.0a  --  Snapshot diff: compare two snapshots, return top movers.
"""

import sqlite3
from typing import Any, Dict



def snapshot_diff_summary(
    con: sqlite3.Connection,
    a_snapshot: str | int,
    b_snapshot: str | int,
    timeframe: str = "spot",
    limit: int = 30,
) -> Dict[str, Any]:
    """
    Compare two snapshots (b - a).  a = older, b = newer.
    Returns dict with top_movers sorted by abs(d_score) DESC.
    """
    _a_ref, a_key = resolve_snapshot(con, a_snapshot)
    _b_ref, b_key = resolve_snapshot(con, b_snapshot)
    a_key = a_key or str(a_snapshot)
    b_key = b_key or str(b_snapshot)

    rows = con.execute(
        """SELECT a.unified_symbol,
                  a.rank, b.rank,
                  a.price, b.price,
                  a.mcap, b.mcap,
                  a.vol24, b.vol24,
                  a.change_24h_pct, b.change_24h_pct,
                  a.base_score, b.base_score
           FROM market_snapshots a
           JOIN market_snapshots b
                ON b.unified_symbol=a.unified_symbol
                   AND b.snapshot_id=? AND b.timeframe=?
           WHERE a.snapshot_id=? AND a.timeframe=?;""",
        (b_key, timeframe, a_key, timeframe),
    ).fetchall()

    def _delta(x, y):
        if x is None or y is None:
            return None
        try:
            return float(y) - float(x)
        except Exception:
            return None

    movers = []
    for r in rows:
        sym, ar, br, ap, bp, am, bm, av, bv, ac, bc, ascr, bscr = r
        d_rank = None
        if ar is not None and br is not None:
            try:
                d_rank = int(ar) - int(br)   # positive = rank improved
            except Exception:
                pass
        movers.append({
            "symbol": sym,
            "rank_a": ar, "rank_b": br, "rank_improve": d_rank,
            "price_a": ap, "price_b": bp, "d_price": _delta(ap, bp),
            "mcap_a": am, "mcap_b": bm,
            "vol24_a": av, "vol24_b": bv, "d_vol24": _delta(av, bv),
            "chg24_a": ac, "chg24_b": bc, "d_chg24": _delta(ac, bc),
            "score_a": ascr, "score_b": bscr, "d_score": _delta(ascr, bscr),
        })

    movers.sort(
        key=lambda x: (
            x["d_score"] is None,
            -(abs(x["d_score"]) if x["d_score"] is not None else 0),
        )
    )

    return {
        "a_snapshot": a_key,
        "b_snapshot": b_key,
        "timeframe": timeframe,
        "symbols_compared": len(rows),
        "top_movers": movers[:max(1, int(limit))],
    }

# ---- export ----
"""
core_v4_0a/export.py
NyoSig_Analysator v4.0a  --  CSV export for TopNow selection.
"""

import csv
import os
import sqlite3


def export_selection_csv(
    con: sqlite3.Connection,
    selection_id: int,
    snapshot_key: str,
    timeframe: str,
    out_path: str,
) -> str:
    """
    Export selection items with market metrics and composite_preview to CSV.
    snapshot_key must be the TEXT key (e.g. snap_20260219T...).
    Returns out_path.
    """
    rows = con.execute(
        """SELECT i.rank_in_selection, i.unified_symbol, i.composite_preview,
                  m.rank, m.mcap, m.vol24, m.change_24h_pct, m.price, m.timestamp_utc
           FROM topnow_selection_items i
           LEFT JOIN market_snapshots m
                ON m.snapshot_id=? AND m.timeframe=? AND m.unified_symbol=i.unified_symbol
           WHERE i.selection_id=?
           ORDER BY i.rank_in_selection ASC;""",
        (snapshot_key, timeframe, int(selection_id)),
    ).fetchall()

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "rank_in_selection", "symbol", "composite_preview",
            "market_rank", "mcap", "vol24", "chg24_pct", "price", "timestamp_utc",
        ])
        w.writerows(rows)

    return out_path

# ---- GUI ----
# nyosig_cryptolytix_gui_v4.0a
# NyoSig_Analysator v4.0a -- clean GUI built on core_v4_0a




import json
import os
import sys

CORE_VERSION = "v7.5c"
APP_VERSION = "v7.5c"


# ---------------------------------------------------------------------------
# v6.0a compatibility layer
# Provides stable public API expected by tests and keeps legacy table names.
# ---------------------------------------------------------------------------

# Keep base ensure_schema
_ensure_schema_base = ensure_schema

def ensure_schema(con):
    """Ensure DB schema + legacy compatibility tables."""
    _ensure_schema_base(con)
    # Legacy compatibility tables expected by some test suites
    con.execute("""
        CREATE TABLE IF NOT EXISTS spot_basic_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            snapshot_id TEXT NOT NULL,
            unified_symbol TEXT NOT NULL,
            base_score REAL,
            created_utc TEXT
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS topnow_selections (
            selection_id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            snapshot_id TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            created_utc TEXT,
            items_count INTEGER
        );
    """)
    con.commit()


def build_topnow_selection(con, run_id, snapshot_id, timeframe='spot', limit=15, progress_cb=None, top_n=None, **_kw):
    """Public wrapper: build selection and return selection_id.

    Accepts both limit and top_n for compatibility.
    """
    if top_n is not None:
        limit = int(top_n)
    if progress_cb is None:
        progress_cb = (lambda _m: None)
    sel_id, n_items = _topnow_build(con, run_id, snapshot_id, timeframe, int(limit), progress_cb)
    # Mirror into legacy table name for compatibility
    try:
        created_utc = utc_now_iso()
        con.execute(
            "INSERT OR REPLACE INTO topnow_selections (selection_id, run_id, snapshot_id, timeframe, created_utc, items_count) VALUES (?, ?, ?, ?, ?, ?);",
            (sel_id, run_id, snapshot_id, timeframe, created_utc, int(n_items)),
        )
        con.commit()
    except Exception:
        pass
    return sel_id


def retention_prune(con, keep_runs=50, protected_run_ids=None):
    """Public wrapper for retention policy."""
    # Use existing retention policy implementation if present
    if protected_run_ids is None:
        protected_run_ids = []
    if 'apply_retention_policy' in globals():
        return apply_retention_policy(con, keep_runs=keep_runs, protected_run_ids=protected_run_ids)
    if 'apply_retention' in globals():
        return apply_retention(con, keep_runs=keep_runs, protected_run_ids=protected_run_ids)
    raise AttributeError('No retention policy function found')


def get_selection_items(con, selection_id):
    """Return selection items as list of dicts.

    Uses columns that exist in topnow_selection_items: composite_preview and reason_json.
    """
    rows = con.execute(
        "SELECT unified_symbol, rank_in_selection, composite_preview, reason_json FROM topnow_selection_items WHERE selection_id=? ORDER BY rank_in_selection ASC;",
        (int(selection_id),)
    ).fetchall()
    out = []
    for sym, rnk, comp, rj in rows:
        out.append({
            'unified_symbol': sym,
            'rank_in_selection': rnk,
            'score': comp,
            'composite_preview': comp,
            'reason_json': rj,
        })
    return out


# ===========================================================================
# QUERY FUNCTIONS FOR GUI VIEWS (v7.1a)
# ===========================================================================

def load_predictions(con, run_id, selection_id=None):
    """
    Load predictions for a run. Returns list of dicts.
    Each dict: unified_symbol, signal, confidence, reasoning (parsed JSON).
    """
    import json as _j
    if selection_id:
        rows = con.execute(
            "SELECT unified_symbol, signal, confidence, reasoning_json, created_utc "
            "FROM predictions WHERE run_id=? AND selection_id=? "
            "ORDER BY confidence DESC;",
            (run_id, selection_id)).fetchall()
    else:
        rows = con.execute(
            "SELECT unified_symbol, signal, confidence, reasoning_json, created_utc "
            "FROM predictions WHERE run_id=? ORDER BY confidence DESC;",
            (run_id,)).fetchall()
    results = []
    for sym, sig, conf, rj, ts in rows:
        reasoning = {}
        try:
            reasoning = _j.loads(rj) if rj else {}
        except Exception:
            pass
        results.append({
            "symbol": sym, "signal": sig, "confidence": conf,
            "reasoning": reasoning, "created_utc": ts,
            "structural_avg": reasoning.get("structural_avg"),
            "rule": reasoning.get("rule", ""),
        })
    return results


def load_trade_plans(con, run_id, selection_id=None):
    """
    Load trade plans for a run. Returns list of dicts with full plan details.
    """
    import json as _j
    if selection_id:
        rows = con.execute(
            "SELECT unified_symbol, direction, entry_zone_low, entry_zone_high, "
            "stop_loss, target_1, target_2, position_pct, risk_score, plan_json, created_utc "
            "FROM trade_plans WHERE run_id=? AND selection_id=? "
            "ORDER BY position_pct DESC;",
            (run_id, selection_id)).fetchall()
    else:
        rows = con.execute(
            "SELECT unified_symbol, direction, entry_zone_low, entry_zone_high, "
            "stop_loss, target_1, target_2, position_pct, risk_score, plan_json, created_utc "
            "FROM trade_plans WHERE run_id=? ORDER BY position_pct DESC;",
            (run_id,)).fetchall()
    results = []
    for sym, d, el, eh, sl, t1, t2, pp, rs, pj, ts in rows:
        plan = {}
        try:
            plan = _j.loads(pj) if pj else {}
        except Exception:
            pass
        results.append({
            "symbol": sym, "direction": d,
            "entry_low": el, "entry_high": eh,
            "stop_loss": sl, "target_1": t1, "target_2": t2,
            "position_pct": pp, "risk_score": rs,
            "confidence": plan.get("confidence"),
            "signal": plan.get("signal"),
            "stop_pct": plan.get("stop_pct"),
            "volatility_factor": plan.get("volatility_factor"),
            "created_utc": ts,
        })
    return results


def load_feature_vectors_for_view(con, run_id, selection_id=None):
    """Load feature vectors for a run. Returns list of dicts."""
    import json as _j
    if selection_id:
        rows = con.execute(
            "SELECT unified_symbol, feature_json, norm_score, created_utc "
            "FROM feature_vectors WHERE run_id=? AND selection_id=? "
            "ORDER BY norm_score DESC;",
            (run_id, selection_id)).fetchall()
    else:
        rows = con.execute(
            "SELECT unified_symbol, feature_json, norm_score, created_utc "
            "FROM feature_vectors WHERE run_id=? ORDER BY norm_score DESC;",
            (run_id,)).fetchall()
    results = []
    for sym, fj, ns, ts in rows:
        features = {}
        try:
            features = _j.loads(fj) if fj else {}
        except Exception:
            pass
        results.append({
            "symbol": sym, "norm_score": ns, "features": features, "created_utc": ts,
            "layers_scored": sum(1 for v in features.values() if v is not None),
            "layers_total": len(features),
        })
    return results


def run_summary(con, run_id, selection_id=None):
    """
    Build a post-analysis summary for the dashboard.
    Returns dict with counts, signal distribution, top picks, and warnings.
    """
    summary = {"run_id": run_id}

    # Run metadata
    r = con.execute("SELECT app_version, scope, status, created_utc FROM runs WHERE run_id=?;",
                    (run_id,)).fetchone()
    if r:
        summary["app_version"] = r[0]
        summary["scope"] = r[1]
        summary["status"] = r[2]
        summary["created_utc"] = r[3]

    # Selection count
    if selection_id:
        cnt = con.execute("SELECT COUNT(*) FROM topnow_selection_items WHERE selection_id=?;",
                          (selection_id,)).fetchone()
        summary["candidates"] = cnt[0] if cnt else 0
    else:
        summary["candidates"] = 0

    # Predictions summary
    preds = load_predictions(con, run_id, selection_id)
    signal_counts = {}
    for p in preds:
        signal_counts[p["signal"]] = signal_counts.get(p["signal"], 0) + 1
    summary["predictions_count"] = len(preds)
    summary["signal_distribution"] = signal_counts

    # Top picks (strong_buy + buy with highest confidence)
    top_picks = [p for p in preds if p["signal"] in ("strong_buy", "buy")]
    top_picks.sort(key=lambda x: -(x["confidence"] or 0))
    summary["top_picks"] = top_picks[:5]

    # Trade plans summary
    plans = load_trade_plans(con, run_id, selection_id)
    summary["trade_plans_count"] = len(plans)
    summary["long_count"] = sum(1 for p in plans if p["direction"] == "long")
    summary["short_count"] = sum(1 for p in plans if p["direction"] == "short")
    summary["hold_count"] = sum(1 for p in plans if p["direction"] == "hold")
    summary["total_position_pct"] = round(sum(p["position_pct"] or 0 for p in plans), 2)

    # Feature vector coverage
    fvs = load_feature_vectors_for_view(con, run_id, selection_id)
    if fvs:
        avg_layers = sum(f["layers_scored"] for f in fvs) / len(fvs)
        summary["avg_layers_scored"] = round(avg_layers, 1)
        summary["feature_vectors_count"] = len(fvs)
    else:
        summary["avg_layers_scored"] = 0
        summary["feature_vectors_count"] = 0

    # Warnings
    warnings = []
    if summary.get("status") != "completed":
        warnings.append(f"Run status is '{summary.get('status')}', not completed")
    if summary["candidates"] == 0:
        warnings.append("No candidates in selection")
    if summary["predictions_count"] == 0:
        warnings.append("No predictions generated")
    if summary.get("avg_layers_scored", 0) < 3:
        warnings.append("Low layer coverage (avg < 3 layers scored)")
    if summary["total_position_pct"] > 10:
        warnings.append(f"High total exposure: {summary['total_position_pct']}%")
    summary["warnings"] = warnings

    return summary


def cross_scope_correlation(con, run_id):
    """
    Find cross-scope correlations between assets in the same run.
    Looks for BTC vs macro instruments (DXY, Gold, VIX) if both exist.
    Returns list of correlation observations.
    """
    observations = []

    # Check if we have both crypto and macro data in this run
    scopes = con.execute(
        "SELECT DISTINCT scope FROM market_snapshots WHERE run_id=?;",
        (run_id,)).fetchall()
    scope_set = {s[0] for s in scopes}

    # BTC price
    btc = con.execute(
        "SELECT price, change_24h_pct FROM market_snapshots "
        "WHERE run_id=? AND unified_symbol='BTC' AND timeframe='spot' LIMIT 1;",
        (run_id,)).fetchone()

    if not btc:
        return [{"note": "No BTC data in this run for cross-correlation"}]

    btc_price, btc_chg = btc

    # Cross-reference pairs
    CROSS_REFS = [
        ("DXY", "inverse", "DXY up -> BTC tends down (risk-off)"),
        ("VIX", "inverse", "VIX up (fear) -> BTC tends down"),
        ("GC", "positive", "Gold up -> BTC may follow (inflation hedge)"),
        ("GSPC", "positive", "S&P up -> BTC tends up (risk-on)"),
        ("TNX", "inverse", "Yields up -> BTC pressure (tighter liquidity)"),
    ]

    for symbol, expected, note in CROSS_REFS:
        row = con.execute(
            "SELECT price, change_24h_pct FROM market_snapshots "
            "WHERE run_id=? AND unified_symbol=? AND timeframe='spot' LIMIT 1;",
            (run_id, symbol)).fetchone()
        if row:
            ref_price, ref_chg = row
            # Simple same-direction check
            if btc_chg and ref_chg:
                same_dir = (btc_chg > 0) == (ref_chg > 0)
                if expected == "inverse":
                    aligned = not same_dir
                else:
                    aligned = same_dir
                observations.append({
                    "btc_change": round(btc_chg, 2) if btc_chg else None,
                    "reference": symbol,
                    "ref_price": ref_price,
                    "ref_change": round(ref_chg, 2) if ref_chg else None,
                    "expected_correlation": expected,
                    "currently_aligned": aligned,
                    "note": note,
                })

    return observations


def backtest_from_trade_plans(con, run_id, selection_id=None):
    """
    Simple backtest: compare trade plan entry prices to current prices.
    Returns list of plan evaluations with P&L estimates.
    This connects BacktestWindow to the trade_plans table.
    """
    plans = load_trade_plans(con, run_id, selection_id)
    results = []
    for plan in plans:
        sym = plan["symbol"]
        if plan["direction"] == "hold":
            results.append({**plan, "status": "no_trade", "pnl_pct": 0})
            continue

        # Get latest price for this symbol
        latest = con.execute(
            "SELECT price FROM market_snapshots "
            "WHERE unified_symbol=? AND timeframe='spot' "
            "ORDER BY id DESC LIMIT 1;",
            (sym,)).fetchone()
        if not latest or not latest[0]:
            results.append({**plan, "status": "no_price_data", "pnl_pct": None})
            continue

        current_price = latest[0]
        entry_mid = ((plan["entry_low"] or 0) + (plan["entry_high"] or 0)) / 2
        if entry_mid <= 0:
            results.append({**plan, "status": "invalid_entry", "pnl_pct": None})
            continue

        if plan["direction"] == "long":
            pnl_pct = round((current_price - entry_mid) / entry_mid * 100, 2)
            hit_stop = current_price <= (plan["stop_loss"] or 0)
            hit_t1 = current_price >= (plan["target_1"] or float("inf"))
            hit_t2 = current_price >= (plan["target_2"] or float("inf"))
        else:  # short
            pnl_pct = round((entry_mid - current_price) / entry_mid * 100, 2)
            hit_stop = current_price >= (plan["stop_loss"] or float("inf"))
            hit_t1 = current_price <= (plan["target_1"] or 0)
            hit_t2 = current_price <= (plan["target_2"] or 0)

        status = "open"
        if hit_stop:
            status = "stopped_out"
        elif hit_t2:
            status = "target_2_hit"
        elif hit_t1:
            status = "target_1_hit"

        results.append({
            **plan,
            "current_price": current_price,
            "entry_mid": round(entry_mid, 6),
            "pnl_pct": pnl_pct,
            "status": status,
            "hit_stop": hit_stop,
            "hit_t1": hit_t1,
            "hit_t2": hit_t2,
        })
    return results


# ===========================================================================
# ALERT ENGINE (v7.2a, OPS-1)
# Scans active trade plans against latest prices, generates alerts.
# ===========================================================================

def check_trade_plan_alerts(con, run_id, selection_id=None, log_cb=None):
    """
    Check all trade plans for stop-loss hits, target hits, and significant moves.
    Inserts alerts into trade_plan_alerts table. Returns list of new alerts.
    """
    import json as _j
    if log_cb is None:
        log_cb = lambda m: None

    plans = load_trade_plans(con, run_id, selection_id)
    new_alerts = []
    created = utc_now_iso()

    for plan in plans:
        sym = plan["symbol"]
        if plan["direction"] == "hold":
            continue

        # Get latest price
        row = con.execute(
            "SELECT price FROM market_snapshots "
            "WHERE unified_symbol=? AND timeframe='spot' "
            "ORDER BY id DESC LIMIT 1;", (sym,)).fetchone()
        if not row or not row[0]:
            continue
        price = float(row[0])

        plan_id_row = con.execute(
            "SELECT id FROM trade_plans WHERE run_id=? AND unified_symbol=? "
            "ORDER BY id DESC LIMIT 1;", (run_id, sym)).fetchone()
        plan_id = plan_id_row[0] if plan_id_row else None

        alerts_for_sym = []

        # Stop-loss check
        sl = plan.get("stop_loss")
        if sl:
            if plan["direction"] == "long" and price <= sl:
                alerts_for_sym.append(("stop_hit",
                    f"{sym} STOP HIT: price {price:.2f} <= stop {sl:.2f}", "critical"))
            elif plan["direction"] == "short" and price >= sl:
                alerts_for_sym.append(("stop_hit",
                    f"{sym} STOP HIT: price {price:.2f} >= stop {sl:.2f}", "critical"))

        # Target checks
        t1, t2 = plan.get("target_1"), plan.get("target_2")
        if plan["direction"] == "long":
            if t2 and price >= t2:
                alerts_for_sym.append(("target_2_hit",
                    f"{sym} TARGET 2 HIT: price {price:.2f} >= T2 {t2:.2f}", "success"))
            elif t1 and price >= t1:
                alerts_for_sym.append(("target_1_hit",
                    f"{sym} TARGET 1 HIT: price {price:.2f} >= T1 {t1:.2f}", "success"))
        elif plan["direction"] == "short":
            if t2 and price <= t2:
                alerts_for_sym.append(("target_2_hit",
                    f"{sym} TARGET 2 HIT: price {price:.2f} <= T2 {t2:.2f}", "success"))
            elif t1 and price <= t1:
                alerts_for_sym.append(("target_1_hit",
                    f"{sym} TARGET 1 HIT: price {price:.2f} <= T1 {t1:.2f}", "success"))

        # Entry zone alert (price entered buy zone)
        el, eh = plan.get("entry_low"), plan.get("entry_high")
        if el and eh:
            if plan["direction"] == "long" and el <= price <= eh:
                alerts_for_sym.append(("entry_zone",
                    f"{sym} IN ENTRY ZONE: {el:.2f} <= {price:.2f} <= {eh:.2f}", "warning"))
            elif plan["direction"] == "short" and el <= price <= eh:
                alerts_for_sym.append(("entry_zone",
                    f"{sym} IN ENTRY ZONE: {el:.2f} <= {price:.2f} <= {eh:.2f}", "warning"))

        # Deduplicate: don't re-alert same type for same symbol in this run
        for alert_type, msg, severity in alerts_for_sym:
            existing = con.execute(
                "SELECT id FROM trade_plan_alerts WHERE run_id=? AND unified_symbol=? "
                "AND alert_type=? AND acknowledged=0;",
                (run_id, sym, alert_type)).fetchone()
            if not existing:
                con.execute(
                    "INSERT INTO trade_plan_alerts "
                    "(run_id, plan_id, unified_symbol, alert_type, message, "
                    "current_price, trigger_price, severity, created_utc) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                    (run_id, plan_id, sym, alert_type, msg,
                     price, sl or t1 or t2, severity, created))
                new_alerts.append({"symbol": sym, "type": alert_type,
                                   "message": msg, "severity": severity, "price": price})
                log_cb(f"ALERT [{severity}] {msg}")

    con.commit()
    return new_alerts


def load_alerts(con, run_id=None, unacknowledged_only=True, limit=50):
    """Load alerts from trade_plan_alerts table."""
    where = []
    params = []
    if run_id:
        where.append("run_id=?")
        params.append(run_id)
    if unacknowledged_only:
        where.append("acknowledged=0")
    clause = " AND ".join(where) if where else "1=1"
    rows = con.execute(
        f"SELECT id, run_id, unified_symbol, alert_type, message, current_price, "
        f"severity, acknowledged, created_utc "
        f"FROM trade_plan_alerts WHERE {clause} "
        f"ORDER BY id DESC LIMIT ?;",
        (*params, limit)).fetchall()
    return [{"id": r[0], "run_id": r[1], "symbol": r[2], "type": r[3],
             "message": r[4], "price": r[5], "severity": r[6],
             "acknowledged": bool(r[7]), "created_utc": r[8]} for r in rows]


def acknowledge_alert(con, alert_id):
    """Mark an alert as acknowledged."""
    con.execute("UPDATE trade_plan_alerts SET acknowledged=1 WHERE id=?;", (alert_id,))
    con.commit()


# ===========================================================================
# PREDICTION PERFORMANCE TRACKER (v7.2a, OPS-2)
# ===========================================================================

def evaluate_prediction_history(con, log_cb=None):
    """
    Evaluate all past predictions against current prices.
    Stores results in prediction_performance table.
    Returns summary dict with hit rates per signal type.
    """
    import json as _j
    if log_cb is None:
        log_cb = lambda m: None

    # Get all predictions not yet evaluated
    already = set()
    try:
        existing = con.execute(
            "SELECT run_id, unified_symbol FROM prediction_performance;").fetchall()
        already = {(r[0], r[1]) for r in existing}
    except Exception:
        pass

    preds = con.execute(
        "SELECT p.run_id, p.unified_symbol, p.signal, p.confidence, "
        "m.price, p.created_utc "
        "FROM predictions p "
        "LEFT JOIN market_snapshots m ON m.run_id=p.run_id "
        "AND m.unified_symbol=p.unified_symbol AND m.timeframe='spot' "
        "ORDER BY p.run_id, p.unified_symbol;").fetchall()

    eval_utc = utc_now_iso()
    count = 0
    outcomes = {"correct": 0, "incorrect": 0, "neutral_skip": 0, "no_data": 0}

    for run_id, sym, signal, conf, pred_price, ts in preds:
        if (run_id, sym) in already:
            continue
        if signal == "neutral":
            outcomes["neutral_skip"] += 1
            continue

        # Get latest price for this symbol
        latest = con.execute(
            "SELECT price FROM market_snapshots "
            "WHERE unified_symbol=? AND timeframe='spot' "
            "ORDER BY id DESC LIMIT 1;", (sym,)).fetchone()
        if not latest or not latest[0] or not pred_price:
            outcomes["no_data"] += 1
            continue

        current = float(latest[0])
        pnl_pct = round((current - pred_price) / pred_price * 100, 2) if pred_price > 0 else 0

        # Evaluate: buy signals correct if price went up, sell if down
        if signal in ("buy", "strong_buy"):
            correct = pnl_pct > 0
        elif signal in ("sell", "strong_sell"):
            correct = pnl_pct < 0
        else:
            correct = None

        outcome = "correct" if correct else ("incorrect" if correct is False else "unknown")
        outcomes[outcome] = outcomes.get(outcome, 0) + 1

        con.execute(
            "INSERT INTO prediction_performance "
            "(run_id, unified_symbol, signal, confidence, price_at_prediction, "
            "price_at_eval, pnl_pct, outcome, eval_utc) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
            (run_id, sym, signal, conf, pred_price, current, pnl_pct, outcome, eval_utc))
        count += 1

    con.commit()

    # Summary stats
    total_evaluated = outcomes.get("correct", 0) + outcomes.get("incorrect", 0)
    hit_rate = round(outcomes["correct"] / total_evaluated * 100, 1) if total_evaluated > 0 else 0
    summary = {
        "evaluated": count,
        "outcomes": outcomes,
        "hit_rate_pct": hit_rate,
        "total_with_outcome": total_evaluated,
    }
    log_cb(f"PredictionPerf: evaluated={count} hit_rate={hit_rate}%")
    return summary


def load_prediction_performance(con, limit=100):
    """Load prediction performance history."""
    rows = con.execute(
        "SELECT run_id, unified_symbol, signal, confidence, "
        "price_at_prediction, price_at_eval, pnl_pct, outcome, eval_utc "
        "FROM prediction_performance ORDER BY id DESC LIMIT ?;", (limit,)).fetchall()
    return [{"run_id": r[0], "symbol": r[1], "signal": r[2], "confidence": r[3],
             "price_at_pred": r[4], "price_at_eval": r[5], "pnl_pct": r[6],
             "outcome": r[7], "eval_utc": r[8]} for r in rows]


# ===========================================================================
# EXPORT FUNCTIONS (v7.2a, OPS-3)
# ===========================================================================

def export_analysis_csv(con, run_id, selection_id, output_path, log_cb=None):
    """
    Export complete analysis results to CSV.
    Includes: symbol, composite, signal, confidence, direction, entry, stop, targets.
    """
    if log_cb is None:
        log_cb = lambda m: None

    preds = {p["symbol"]: p for p in load_predictions(con, run_id, selection_id)}
    plans = {p["symbol"]: p for p in load_trade_plans(con, run_id, selection_id)}
    fvs = {f["symbol"]: f for f in load_feature_vectors_for_view(con, run_id, selection_id)}

    items = con.execute(
        "SELECT unified_symbol, rank_in_selection, composite_preview "
        "FROM topnow_selection_items WHERE selection_id=? "
        "ORDER BY rank_in_selection;", (selection_id,)).fetchall()

    lines = ["symbol,rank,composite,signal,confidence,direction,"
             "entry_low,entry_high,stop_loss,target_1,target_2,"
             "position_pct,risk_score,norm_score,layers_scored"]

    for sym, rank, comp in items:
        pred = preds.get(sym, {})
        plan = plans.get(sym, {})
        fv = fvs.get(sym, {})
        lines.append(",".join([
            sym, str(rank or ""), _fmt(comp, 2),
            pred.get("signal", ""), _fmt(pred.get("confidence"), 3),
            plan.get("direction", ""),
            _fmt(plan.get("entry_low"), 2), _fmt(plan.get("entry_high"), 2),
            _fmt(plan.get("stop_loss"), 2), _fmt(plan.get("target_1"), 2),
            _fmt(plan.get("target_2"), 2), _fmt(plan.get("position_pct"), 1),
            _fmt(plan.get("risk_score"), 2), _fmt(fv.get("norm_score"), 2),
            str(fv.get("layers_scored", "")),
        ]))

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log_cb(f"Exported {len(items)} rows to {output_path}")
    return len(items)


def export_summary_text(con, run_id, selection_id, output_path, log_cb=None):
    """Export run summary as human-readable text file."""
    if log_cb is None:
        log_cb = lambda m: None
    s = run_summary(con, run_id, selection_id)
    lines = [
        "=" * 60,
        "NyoSig Analysator -- Analysis Summary",
        "=" * 60,
        f"Run: {s['run_id']}  Version: {s.get('app_version', '?')}  "
        f"Scope: {s.get('scope', '?')}",
        f"Status: {s.get('status')}  Candidates: {s.get('candidates', 0)}",
        f"Avg layers scored: {s.get('avg_layers_scored', 0)}",
        "",
        "--- SIGNALS ---",
    ]
    for sig in ["strong_buy", "buy", "neutral", "sell", "strong_sell"]:
        cnt = s.get("signal_distribution", {}).get(sig, 0)
        lines.append(f"  {sig:15s}  {cnt}")
    lines.append("")
    lines.append("--- TOP PICKS ---")
    for p in s.get("top_picks", []):
        lines.append(f"  {p['symbol']:8s}  {p['signal']:12s}  conf={p.get('confidence', 0):.3f}")
    lines.append("")
    lines.append(f"--- TRADE PLANS: {s.get('long_count', 0)} long, "
                 f"{s.get('short_count', 0)} short, {s.get('hold_count', 0)} hold ---")
    lines.append(f"Total position: {s.get('total_position_pct', 0):.1f}%")
    if s.get("warnings"):
        lines.append("")
        lines.append("--- WARNINGS ---")
        for w in s["warnings"]:
            lines.append(f"  [!] {w}")
    lines.append("")
    lines.append("Generated: " + utc_now_iso())

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    log_cb(f"Summary exported to {output_path}")


# ===========================================================================
# WATCHLIST ENRICHMENT (v7.2a, OPS-4)
# ===========================================================================

def enrich_watchlist_with_plans(con):
    """
    Enrich active watchlist items with their latest trade plan and prediction.
    Returns list of enriched watchlist dicts.
    """
    items = con.execute(
        "SELECT w.id, w.unified_symbol, w.tag, w.stage, "
        "w.tracking_since_utc, w.entry_score "
        "FROM watchlist w WHERE w.exit_timestamp_utc IS NULL "
        "ORDER BY w.id;").fetchall()

    enriched = []
    for wid, sym, tag, stage, since, entry_score in items:
        item = {
            "id": wid, "symbol": sym, "tag": tag, "stage": stage,
            "since": since, "entry_score": entry_score,
            "plan": None, "prediction": None, "current_price": None, "pnl_pct": None,
        }

        # Latest trade plan for this symbol
        plan_row = con.execute(
            "SELECT direction, entry_zone_low, entry_zone_high, stop_loss, "
            "target_1, target_2, position_pct, risk_score "
            "FROM trade_plans WHERE unified_symbol=? "
            "ORDER BY id DESC LIMIT 1;", (sym,)).fetchone()
        if plan_row:
            item["plan"] = {
                "direction": plan_row[0], "entry_low": plan_row[1],
                "entry_high": plan_row[2], "stop_loss": plan_row[3],
                "target_1": plan_row[4], "target_2": plan_row[5],
                "position_pct": plan_row[6], "risk_score": plan_row[7],
            }

        # Latest prediction
        pred_row = con.execute(
            "SELECT signal, confidence FROM predictions "
            "WHERE unified_symbol=? ORDER BY id DESC LIMIT 1;", (sym,)).fetchone()
        if pred_row:
            item["prediction"] = {"signal": pred_row[0], "confidence": pred_row[1]}

        # Current price + P&L from entry
        price_row = con.execute(
            "SELECT price FROM market_snapshots "
            "WHERE unified_symbol=? AND timeframe='spot' "
            "ORDER BY id DESC LIMIT 1;", (sym,)).fetchone()
        if price_row and price_row[0]:
            item["current_price"] = price_row[0]
            if entry_score and entry_score > 0:
                item["pnl_pct"] = round(
                    (price_row[0] - entry_score) / entry_score * 100, 2)

        # Unacknowledged alerts
        alert_cnt = con.execute(
            "SELECT COUNT(*) FROM trade_plan_alerts "
            "WHERE unified_symbol=? AND acknowledged=0;", (sym,)).fetchone()
        item["active_alerts"] = alert_cnt[0] if alert_cnt else 0

        enriched.append(item)
    return enriched


# Helper for CSV export
def _fmt(v, decimals=2):
    if v is None:
        return ""
    try:
        return ("{:." + str(decimals) + "f}").format(float(v))
    except Exception:
        return str(v)


# ===========================================================================
# TRACKED-ONLY REFRESH (v7.3a, DAILY-1)
# Fast re-scan: fetches latest prices only for watchlist assets,
# re-checks alerts, and updates enrichment. No full market scan needed.
# ===========================================================================

def tracked_only_refresh(con, app_version, log_cb=None):
    """
    Quick refresh for tracked (watchlist) assets only.
    Steps:
      1. Get active watchlist symbols
      2. Fetch latest prices via Yahoo (works for all asset classes)
      3. Update market_snapshots with fresh prices
      4. Re-run predictions for tracked set
      5. Check alerts against new prices
    Returns dict with refresh summary.
    """
    import time as _t
    if log_cb is None:
        log_cb = lambda m: None

    # 1. Active watchlist
    tracked = con.execute(
        "SELECT unified_symbol FROM watchlist "
        "WHERE exit_timestamp_utc IS NULL;").fetchall()
    symbols = [r[0] for r in tracked]
    if not symbols:
        log_cb("tracked_refresh: no active watchlist items")
        return {"refreshed": 0, "alerts": []}

    log_cb(f"tracked_refresh: {len(symbols)} symbols: {', '.join(symbols[:10])}")

    # 2. Create a lightweight run for this refresh
    created = utc_now_iso()
    run_id = con.execute(
        "INSERT INTO runs (created_utc, app_version, scope, status) "
        "VALUES (?, ?, 'tracked_refresh', 'running');",
        (created, app_version)).lastrowid
    state_log(con, run_id, "tracked_refresh", "created", "Quick tracked refresh")
    con.commit()

    # 3. Fetch latest prices per symbol
    refreshed = 0
    snap_key = "tracked_" + created.replace(":", "").replace("-", "")[:15]
    for sym in symbols:
        # Try Yahoo first (universal), skip if no ticker mapping
        ticker = sym  # Many crypto symbols work directly on Yahoo as SYM-USD
        if not any(c in sym for c in ["=", "^", "."]):
            ticker = sym + "-USD"
        try:
            price, date_str = _fetch_yahoo_quote(ticker, log_cb=log_cb)
            if price is not None:
                con.execute(
                    "INSERT INTO market_snapshots "
                    "(run_id, snapshot_id, unified_symbol, timeframe, price, "
                    "mcap, vol24, rank, change_24h_pct, base_score, pair, "
                    "source, scope, fetched_utc, snapshot_ref) "
                    "VALUES (?,?,?,?,?,0,0,0,0,NULL,?,?,?,?,NULL);",
                    (run_id, snap_key, sym, "spot", price,
                     f"{sym}/USD", "yahoo_finance", "tracked_refresh", created))
                refreshed += 1
            _t.sleep(0.6)
        except Exception as exc:
            log_cb(f"tracked_refresh SKIP {sym}: {str(exc)[:60]}")
    con.commit()
    log_cb(f"tracked_refresh: prices updated for {refreshed}/{len(symbols)}")

    # 4. Check alerts against updated prices (use latest non-refresh run for plans)
    latest_run = con.execute(
        "SELECT run_id FROM runs WHERE scope != 'tracked_refresh' "
        "AND status='completed' ORDER BY run_id DESC LIMIT 1;").fetchone()
    alerts = []
    if latest_run:
        alerts = check_trade_plan_alerts(con, latest_run[0], log_cb=log_cb)

    # 5. Finalize
    con.execute("UPDATE runs SET status='completed' WHERE run_id=?;", (run_id,))
    state_log(con, run_id, "completed", "tracked_refresh")
    con.commit()

    return {
        "run_id": run_id,
        "refreshed": refreshed,
        "total_tracked": len(symbols),
        "new_alerts": len(alerts),
        "alerts": alerts,
    }


# ===========================================================================
# RUN HISTORY COMPARE (v7.3a, DAILY-2)
# Shows how signals evolved across runs for the same symbols.
# ===========================================================================

def run_history_compare(con, unified_symbol, limit=10):
    """
    Show prediction history for one symbol across recent runs.
    Returns list of dicts sorted newest-first.
    """
    rows = con.execute(
        "SELECT p.run_id, r.created_utc, r.scope, p.signal, p.confidence, "
        "m.price, r.app_version "
        "FROM predictions p "
        "JOIN runs r ON r.run_id = p.run_id "
        "LEFT JOIN market_snapshots m ON m.run_id = p.run_id "
        "  AND m.unified_symbol = p.unified_symbol AND m.timeframe = 'spot' "
        "WHERE p.unified_symbol = ? "
        "ORDER BY p.run_id DESC LIMIT ?;",
        (unified_symbol, limit)).fetchall()

    history = []
    prev_signal = None
    for run_id, ts, scope, signal, conf, price, ver in rows:
        changed = prev_signal is not None and signal != prev_signal
        history.append({
            "run_id": run_id,
            "created_utc": ts,
            "scope": scope,
            "signal": signal,
            "confidence": conf,
            "price": price,
            "app_version": ver,
            "signal_changed": changed,
        })
        prev_signal = signal
    return history


def run_history_multi(con, symbols, limit=5):
    """
    Compare latest signal for multiple symbols across their recent runs.
    Returns dict of symbol -> [history entries].
    """
    result = {}
    for sym in symbols:
        result[sym] = run_history_compare(con, sym, limit=limit)
    return result


# ===========================================================================
# LIGHTWEIGHT SCHEDULER (v7.4a, PROD-1)
# Periodic auto-refresh for tracked assets. Runs in background thread.
# Non-blocking, cancellable, logs to state_log.
# ===========================================================================

import threading as _sched_threading


class TrackedRefreshScheduler:
    """
    Periodically calls tracked_only_refresh() in a background thread.
    Safe for Pydroid3 (single-process, no multiprocessing).
    """

    def __init__(self, db_path, app_version, interval_minutes=30, log_cb=None):
        self.db_path = db_path
        self.app_version = app_version
        self.interval_s = interval_minutes * 60
        self.log_cb = log_cb or (lambda m: None)
        self._running = False
        self._thread = None
        self._stop_event = _sched_threading.Event()
        self._last_result = None
        self._run_count = 0

    def start(self):
        if self._running:
            self.log_cb("Scheduler: already running")
            return
        self._running = True
        self._stop_event.clear()
        self._thread = _sched_threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        self.log_cb(f"Scheduler: started (interval={self.interval_s}s)")

    def stop(self):
        self._running = False
        self._stop_event.set()
        self.log_cb("Scheduler: stopped")

    def is_running(self):
        return self._running and self._thread and self._thread.is_alive()

    def last_result(self):
        return self._last_result

    def status(self):
        return {
            "running": self.is_running(),
            "interval_minutes": self.interval_s // 60,
            "run_count": self._run_count,
            "last_result": self._last_result,
        }

    def _loop(self):
        while self._running:
            try:
                con = db_connect(self.db_path)
                ensure_schema(con)
                result = tracked_only_refresh(con, self.app_version, log_cb=self.log_cb)
                con.close()
                self._last_result = result
                self._run_count += 1
                self.log_cb(f"Scheduler: refresh #{self._run_count} done -- "
                            f"refreshed={result.get('refreshed', 0)} alerts={result.get('new_alerts', 0)}")
            except Exception as exc:
                self.log_cb(f"Scheduler: ERROR {str(exc)[:120]}")
                self._last_result = {"error": str(exc)[:200]}
            # Wait for interval or stop signal
            self._stop_event.wait(timeout=self.interval_s)
            if self._stop_event.is_set():
                break
        self._running = False


# ===========================================================================
# HEALTH CHECK DASHBOARD (v7.4a, PROD-4)
# Quick system status: DB health, last run, scheduler, alerts.
# ===========================================================================

def system_health_check(con, scheduler=None):
    """
    Run a quick health check on the system.
    Returns dict with DB status, last run info, pending alerts, and scheduler state.
    """
    health = {"status": "ok", "checks": {}, "warnings": []}

    # DB connectivity
    try:
        con.execute("SELECT 1;")
        health["checks"]["db_connected"] = True
    except Exception:
        health["checks"]["db_connected"] = False
        health["status"] = "critical"
        health["warnings"].append("Database connection failed")
        return health

    # Latest run
    latest = con.execute(
        "SELECT run_id, status, app_version, created_utc, scope "
        "FROM runs ORDER BY run_id DESC LIMIT 1;").fetchone()
    if latest:
        health["checks"]["latest_run"] = {
            "run_id": latest[0], "status": latest[1], "version": latest[2],
            "created": latest[3], "scope": latest[4],
        }
        if latest[1] == "failed":
            health["warnings"].append(f"Latest run {latest[0]} has status 'failed'")
    else:
        health["checks"]["latest_run"] = None
        health["warnings"].append("No runs found in database")

    # Total runs
    cnt = con.execute("SELECT COUNT(*) FROM runs;").fetchone()
    health["checks"]["total_runs"] = cnt[0] if cnt else 0

    # Pending alerts
    try:
        alert_cnt = con.execute(
            "SELECT COUNT(*) FROM trade_plan_alerts WHERE acknowledged=0;").fetchone()
        health["checks"]["pending_alerts"] = alert_cnt[0] if alert_cnt else 0
        if alert_cnt and alert_cnt[0] > 5:
            health["warnings"].append(f"{alert_cnt[0]} unacknowledged alerts pending")
    except Exception:
        health["checks"]["pending_alerts"] = "table_missing"

    # Active watchlist
    try:
        watch_cnt = con.execute(
            "SELECT COUNT(*) FROM watchlist WHERE exit_timestamp_utc IS NULL;").fetchone()
        health["checks"]["active_watchlist"] = watch_cnt[0] if watch_cnt else 0
    except Exception:
        health["checks"]["active_watchlist"] = 0

    # Predictions count
    try:
        pred_cnt = con.execute("SELECT COUNT(*) FROM predictions;").fetchone()
        health["checks"]["total_predictions"] = pred_cnt[0] if pred_cnt else 0
    except Exception:
        health["checks"]["total_predictions"] = "table_missing"

    # Trade plans count
    try:
        plan_cnt = con.execute("SELECT COUNT(*) FROM trade_plans;").fetchone()
        health["checks"]["total_trade_plans"] = plan_cnt[0] if plan_cnt else 0
    except Exception:
        health["checks"]["total_trade_plans"] = "table_missing"

    # DB size
    try:
        import os as _hc_os
        db_path = con.execute("PRAGMA database_list;").fetchone()[2]
        if db_path and _hc_os.path.isfile(db_path):
            size_mb = _hc_os.path.getsize(db_path) / (1024 * 1024)
            health["checks"]["db_size_mb"] = round(size_mb, 1)
            if size_mb > 100:
                health["warnings"].append(f"Database size is {size_mb:.0f} MB -- consider retention pruning")
    except Exception:
        pass

    # Scheduler
    if scheduler:
        health["checks"]["scheduler"] = scheduler.status()
    else:
        health["checks"]["scheduler"] = {"running": False}

    # Overall status
    if health["warnings"]:
        health["status"] = "warning"
    for w in health["warnings"]:
        if "critical" in w.lower() or "failed" in w.lower():
            health["status"] = "critical"
            break

    return health


# ===========================================================================
# PORTFOLIO POSITIONS (v7.5a, PORT-1)
# Track actual positions opened from trade plans or manually.
# ===========================================================================

def open_position(con, symbol, direction, entry_price, entry_size=1.0,
                  stop_loss=None, target_price=None, source_run_id=None,
                  source_plan_id=None, notes="", scope="crypto_spot"):
    """Open a new portfolio position. Returns position id."""
    created = utc_now_iso()
    cur = con.execute(
        "INSERT INTO portfolio_positions "
        "(unified_symbol, direction, entry_price, entry_size, stop_loss, "
        "target_price, status, source_run_id, source_plan_id, notes, "
        "opened_utc, scope) VALUES (?,?,?,?,?,?,?,?,?,?,?,?);",
        (symbol, direction, entry_price, entry_size, stop_loss,
         target_price, "open", source_run_id, source_plan_id, notes,
         created, scope))
    con.commit()
    return cur.lastrowid


def close_position(con, position_id, exit_price, notes=""):
    """Close an open position. Computes P&L."""
    row = con.execute(
        "SELECT direction, entry_price, entry_size FROM portfolio_positions "
        "WHERE id=? AND status='open';", (position_id,)).fetchone()
    if not row:
        return None
    direction, entry, size = row
    if direction == "long":
        pnl_pct = round((exit_price - entry) / entry * 100, 2) if entry > 0 else 0
    else:
        pnl_pct = round((entry - exit_price) / entry * 100, 2) if entry > 0 else 0
    pnl_abs = round(pnl_pct / 100 * size, 4)
    closed = utc_now_iso()
    con.execute(
        "UPDATE portfolio_positions SET status='closed', exit_price=?, "
        "pnl_pct=?, pnl_abs=?, closed_utc=?, notes=notes||? WHERE id=?;",
        (exit_price, pnl_pct, pnl_abs, closed, f" | Closed: {notes}" if notes else "", position_id))
    con.commit()
    return {"pnl_pct": pnl_pct, "pnl_abs": pnl_abs}


def list_positions(con, status="open"):
    """List portfolio positions. status: 'open', 'closed', or 'all'."""
    if status == "all":
        rows = con.execute(
            "SELECT id, unified_symbol, direction, entry_price, entry_size, "
            "stop_loss, target_price, status, exit_price, pnl_pct, pnl_abs, "
            "source_run_id, opened_utc, closed_utc, scope, notes "
            "FROM portfolio_positions ORDER BY id DESC;").fetchall()
    else:
        rows = con.execute(
            "SELECT id, unified_symbol, direction, entry_price, entry_size, "
            "stop_loss, target_price, status, exit_price, pnl_pct, pnl_abs, "
            "source_run_id, opened_utc, closed_utc, scope, notes "
            "FROM portfolio_positions WHERE status=? ORDER BY id DESC;",
            (status,)).fetchall()
    return [{"id": r[0], "symbol": r[1], "direction": r[2], "entry_price": r[3],
             "size": r[4], "stop_loss": r[5], "target": r[6], "status": r[7],
             "exit_price": r[8], "pnl_pct": r[9], "pnl_abs": r[10],
             "run_id": r[11], "opened": r[12], "closed": r[13],
             "scope": r[14], "notes": r[15]} for r in rows]


def open_position_from_trade_plan(con, run_id, symbol, selection_id=None):
    """
    Open a position directly from the latest trade plan for a symbol.
    Uses trade plan's entry_mid, stop_loss, target_1 as defaults.
    Returns position id or None.
    """
    plan = con.execute(
        "SELECT id, direction, entry_zone_low, entry_zone_high, stop_loss, "
        "target_1, position_pct FROM trade_plans "
        "WHERE run_id=? AND unified_symbol=? ORDER BY id DESC LIMIT 1;",
        (run_id, symbol)).fetchone()
    if not plan or plan[1] == "hold":
        return None
    plan_id, direction, el, eh, sl, t1, pos_pct = plan
    entry_mid = ((el or 0) + (eh or 0)) / 2
    if entry_mid <= 0:
        return None
    return open_position(con, symbol, direction, entry_mid, entry_size=pos_pct or 1.0,
                         stop_loss=sl, target_price=t1,
                         source_run_id=run_id, source_plan_id=plan_id)


# ===========================================================================
# RISK METRICS (v7.5a, PORT-2)
# Portfolio-level risk calculations.
# ===========================================================================

def compute_portfolio_risk(con):
    """
    Compute portfolio-level risk metrics for all open positions.
    Returns dict with: total_exposure, max_position, concentration,
    portfolio_heat, positions_at_risk, diversification_score.
    """
    positions = list_positions(con, "open")
    if not positions:
        return {"total_exposure": 0, "positions": 0, "status": "no_positions"}

    total_size = sum(p["size"] or 0 for p in positions)
    sizes = [p["size"] or 0 for p in positions]
    max_pos = max(sizes) if sizes else 0
    n_positions = len(positions)

    # Concentration: Herfindahl index (0=diversified, 1=concentrated)
    if total_size > 0:
        shares = [s / total_size for s in sizes]
        hhi = sum(s ** 2 for s in shares)
    else:
        hhi = 0

    # Portfolio heat: sum of risk per position (distance to stop in %)
    heat = 0
    at_risk = 0
    for p in positions:
        if p["stop_loss"] and p["entry_price"] and p["entry_price"] > 0:
            risk_pct = abs(p["entry_price"] - p["stop_loss"]) / p["entry_price"] * 100
            heat += risk_pct * (p["size"] or 1)
            # Get current price to check if position is at risk
            row = con.execute(
                "SELECT price FROM market_snapshots "
                "WHERE unified_symbol=? AND timeframe='spot' "
                "ORDER BY id DESC LIMIT 1;", (p["symbol"],)).fetchone()
            if row and row[0]:
                current = row[0]
                if p["direction"] == "long" and current < p["entry_price"]:
                    at_risk += 1
                elif p["direction"] == "short" and current > p["entry_price"]:
                    at_risk += 1

    # P&L summary for closed positions
    closed = list_positions(con, "closed")
    closed_pnl = sum(p["pnl_pct"] or 0 for p in closed)
    wins = sum(1 for p in closed if (p["pnl_pct"] or 0) > 0)
    losses = sum(1 for p in closed if (p["pnl_pct"] or 0) < 0)

    # Diversification: unique scopes
    scopes = set(p.get("scope", "") for p in positions)

    return {
        "positions_open": n_positions,
        "total_exposure_pct": round(total_size, 2),
        "max_single_position_pct": round(max_pos, 2),
        "concentration_hhi": round(hhi, 3),
        "portfolio_heat": round(heat, 2),
        "positions_at_risk": at_risk,
        "scopes_diversified": len(scopes),
        "closed_total": len(closed),
        "closed_pnl_pct": round(closed_pnl, 2),
        "closed_win_rate": round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else 0,
        "warnings": _risk_warnings(n_positions, total_size, max_pos, hhi, heat, at_risk),
    }


def _risk_warnings(n, total, max_pos, hhi, heat, at_risk):
    """Generate risk warnings."""
    warnings = []
    if total > 15:
        warnings.append(f"High total exposure: {total:.1f}% (recommended < 15%)")
    if max_pos > 5:
        warnings.append(f"Single position too large: {max_pos:.1f}% (recommended < 5%)")
    if hhi > 0.5 and n > 1:
        warnings.append(f"Portfolio concentrated: HHI={hhi:.2f} (target < 0.3)")
    if heat > 20:
        warnings.append(f"Portfolio heat high: {heat:.1f}% total risk to stops")
    if at_risk > n * 0.5 and n > 2:
        warnings.append(f"{at_risk}/{n} positions underwater")
    return warnings


# ===========================================================================
# AUTO-RETENTION IN SCHEDULER (v7.5a, PORT-3)
# ===========================================================================

def auto_retention_prune(con, keep_runs=50, log_cb=None):
    """
    Automatic DB maintenance: prune old runs, vacuum if needed.
    Called by scheduler after each refresh cycle.
    """
    if log_cb is None:
        log_cb = lambda m: None
    try:
        # Protect runs that have tracked watchlist items
        protected = set()
        try:
            tracked = con.execute(
                "SELECT DISTINCT source_run_id FROM portfolio_positions WHERE status='open' "
                "AND source_run_id IS NOT NULL;").fetchall()
            protected.update(r[0] for r in tracked)
        except Exception:
            pass
        try:
            wl = con.execute(
                "SELECT DISTINCT entry_score FROM watchlist WHERE exit_timestamp_utc IS NULL;"
            ).fetchall()
        except Exception:
            pass

        # Count runs
        total = con.execute("SELECT COUNT(*) FROM runs;").fetchone()[0]
        if total <= keep_runs:
            return {"pruned": 0, "total": total}

        # Find runs to prune (oldest, not protected)
        to_prune = total - keep_runs
        old_runs = con.execute(
            "SELECT run_id FROM runs ORDER BY run_id ASC LIMIT ?;",
            (to_prune,)).fetchall()
        pruned = 0
        for (rid,) in old_runs:
            if rid in protected:
                continue
            # Delete cascading data
            for table in ["market_snapshots", "ohlcv_snapshots", "raw_snapshots",
                          "layer_results", "state_log", "feature_vectors",
                          "predictions", "trade_plans", "trade_plan_alerts",
                          "prediction_performance"]:
                try:
                    con.execute(f"DELETE FROM {table} WHERE run_id=?;", (rid,))
                except Exception:
                    pass
            try:
                con.execute("DELETE FROM snapshots WHERE run_id=?;", (rid,))
                con.execute("DELETE FROM run_scopes WHERE run_id=?;", (rid,))
                con.execute("DELETE FROM topnow_selection WHERE run_id=?;", (rid,))
            except Exception:
                pass
            con.execute("DELETE FROM runs WHERE run_id=?;", (rid,))
            pruned += 1
        con.commit()
        log_cb(f"auto_retention: pruned {pruned} old runs (kept {keep_runs})")
        return {"pruned": pruned, "total": total - pruned}
    except Exception as exc:
        log_cb(f"auto_retention ERROR: {str(exc)[:120]}")
        return {"error": str(exc)[:200]}


# ===========================================================================
# CONFIG PERSISTENCE (v7.5a, PORT-4)
# Save/load analysis profiles (scope + layer configs).
# ===========================================================================

def save_config_profile(con, profile_name, scope, config_dict):
    """Save or update a named configuration profile."""
    import json as _j
    now = utc_now_iso()
    existing = con.execute(
        "SELECT id FROM config_profiles WHERE profile_name=?;",
        (profile_name,)).fetchone()
    if existing:
        con.execute(
            "UPDATE config_profiles SET scope=?, config_json=?, updated_utc=? "
            "WHERE profile_name=?;",
            (scope, _j.dumps(config_dict), now, profile_name))
    else:
        con.execute(
            "INSERT INTO config_profiles (profile_name, scope, config_json, created_utc) "
            "VALUES (?, ?, ?, ?);",
            (profile_name, scope, _j.dumps(config_dict), now))
    con.commit()


def load_config_profile(con, profile_name):
    """Load a named configuration profile. Returns (scope, config_dict) or None."""
    import json as _j
    row = con.execute(
        "SELECT scope, config_json FROM config_profiles WHERE profile_name=?;",
        (profile_name,)).fetchone()
    if not row:
        return None
    try:
        return row[0], _j.loads(row[1])
    except Exception:
        return row[0], {}


def list_config_profiles(con):
    """List all saved profiles. Returns list of (name, scope, created, updated)."""
    return con.execute(
        "SELECT profile_name, scope, created_utc, updated_utc "
        "FROM config_profiles ORDER BY profile_name;").fetchall()


def delete_config_profile(con, profile_name):
    """Delete a named profile."""
    con.execute("DELETE FROM config_profiles WHERE profile_name=?;", (profile_name,))
    con.commit()


# ===========================================================================
# PORTFOLIO DASHBOARD QUERY (v7.5a, PORT-5)
# ===========================================================================

def portfolio_dashboard(con):
    """
    Build complete portfolio dashboard data.
    Returns dict with positions, risk metrics, and recent activity.
    """
    positions = list_positions(con, "open")
    risk = compute_portfolio_risk(con)

    # Enrich open positions with current prices
    for pos in positions:
        row = con.execute(
            "SELECT price FROM market_snapshots "
            "WHERE unified_symbol=? AND timeframe='spot' "
            "ORDER BY id DESC LIMIT 1;", (pos["symbol"],)).fetchone()
        if row and row[0]:
            pos["current_price"] = row[0]
            if pos["direction"] == "long":
                pos["unrealised_pnl"] = round(
                    (row[0] - pos["entry_price"]) / pos["entry_price"] * 100, 2
                ) if pos["entry_price"] > 0 else 0
            else:
                pos["unrealised_pnl"] = round(
                    (pos["entry_price"] - row[0]) / pos["entry_price"] * 100, 2
                ) if pos["entry_price"] > 0 else 0
        else:
            pos["current_price"] = None
            pos["unrealised_pnl"] = None

    # Recent closed
    closed = list_positions(con, "closed")[:10]

    return {
        "open_positions": positions,
        "risk_metrics": risk,
        "recent_closed": closed,
        "total_unrealised_pnl": round(
            sum(p.get("unrealised_pnl", 0) or 0 for p in positions), 2),
    }
