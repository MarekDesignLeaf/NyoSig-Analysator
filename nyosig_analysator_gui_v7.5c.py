#!/usr/bin/env python3
# -*- coding: utf-8-sig -*-
# nyosig_analysator_gui_v7.5c -- GUI
# NyoSig_Analysator -- GUI shell importing CORE v7.5a
# v7.5c: v7.5a GUI shell updated to prefer CORE v7.5c

from __future__ import annotations
import os as _os, sys as _sys, importlib.util as _ilu

APP_VERSION = "v7.5c"

def _load_core():
    _dir = _os.path.dirname(_os.path.abspath(__file__))
    _candidates = [
        "nyosig_analysator_core_v7.5c.py",
        "nyosig_analysator_core_v7.5a.py",
        "nyosig_analysator_core_v6.2a.py",
        "nyosig_analysator_core_v6.1a.py",
        "nyosig_analysator_core_v6.0f.py",
    ]
    _core_path = None
    for _name in _candidates:
        _p = _os.path.join(_dir, _name)
        if _os.path.isfile(_p):
            _core_path = _p
            break
    if _core_path is None:
        raise FileNotFoundError("Cannot find core module in " + _dir)
    if not _os.path.isfile(_core_path):
        raise FileNotFoundError("Missing CORE file: " + _core_path)
    _spec = _ilu.spec_from_file_location("nyosig_core_analysator_v6_0a", _core_path)
    _mod = _ilu.module_from_spec(_spec)
    _sys.modules["nyosig_core_analysator_v6_0a"] = _mod
    _spec.loader.exec_module(_mod)
    return _mod

_CORE = _load_core()
# Export core symbols into this module namespace for backwards compatibility
for _k, _v in _CORE.__dict__.items():
    if _k.startswith("__") and _k not in ("__doc__",):
        continue
    if _k in ("APP_VERSION",):
        continue
    globals().setdefault(_k, _v)

import tkinter as tk
import tkinter.font as tkfont
from tkinter import messagebox, ttk


# Spinner animation frames (ASCII, compatible with Pydroid3/Android)
_SPIN_FRAMES = ["|", "/", "-", "\\"]

# OHLCV DATA RULES -- binding constants

# 1. Minimum candle counts per use case
OHLCV_CANDLE_RULES = {
    "rsi_14":              28,
    "macd_12_26_9":        60,
    "technical_layer":     60,
    "backtest_7d":         30,
    "backtest_30d":        60,
    "trend_detection":     14,
    "support_resistance":  60,
    "volume_anomaly":      30,
    "absolute_minimum":    14,
}

# 2. CoinGecko rate limit -- free tier
CG_RATE_LIMIT = {
    "safe_interval_s":        6.0,
    "first_429_cooldown_s":   60,
    "repeat_429_cooldown_s":  120,
    "max_retries_per_symbol": 2,
    "backoff_sequence_s":     [2, 8, 30],
    "cache_ttl_spot_s":       300,
    "cache_ttl_ohlcv_s":      3600,
    "cache_ttl_global_s":     600,
    "cache_ttl_community_s":  3600,
}


# 3. Exchange rate limits
EXCHANGE_RATE_LIMITS = {
    "binance_public":  {"safe_interval_s": 0.1, "req_per_min": 1200, "ban_threshold": 2400, "ban_cooldown_s": 600, "cooldown_429_s": 30},
    "bybit_public":    {"safe_interval_s": 0.1, "req_per_min": 1200, "cooldown_429_s": 30},
    "okx_public":      {"safe_interval_s": 0.2, "req_per_min": 300,  "cooldown_429_s": 30},
    "coinpaprika":     {"safe_interval_s": 0.2, "req_per_min": 9999, "cooldown_429_s": 30},
    "coincap":         {"safe_interval_s": 0.3, "req_per_min": 200,  "cooldown_429_s": 30},
    "cryptocompare":   {"safe_interval_s": 0.5, "req_per_month": 100_000, "cooldown_429_s": 60},
}

# 4. Stablecoins and wrapped tokens -- never have perp markets
STABLECOIN_SYMBOLS = frozenset({
    "USDT", "USDC", "BUSD", "DAI", "USDS", "TUSD", "USDP",
    "FDUSD", "PYUSD", "USDE", "USDD", "LUSD", "GUSD",
    "USDG", "FRAX", "CRVUSD", "RLUSD", "EURC", "EURS",
    "USD1", "RUSD", "USDX", "USDM", "BUIDL", "PAXG", "XAUT",
    "USDY", "USDV", "UXD", "ZUSD", "USDH",
})

WRAPPED_TO_ORIGINAL = {
    "WBTC": "BTC", "WETH": "ETH", "WBNB": "BNB",
    "WSOL": "SOL", "WAVAX": "AVAX", "WMATIC": "MATIC",
}

# 5. Binding Binance symbol -> perp pair mapping
# Only authoritative reference. Unknown symbols: try {SYM}USDT, log as "unmapped_symbol".
BINANCE_PERP_MAP = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT",
    "SOL": "SOLUSDT", "ADA": "ADAUSDT", "AVAX": "AVAXUSDT",
    "DOT": "DOTUSDT", "ATOM": "ATOMUSDT", "NEAR": "NEARUSDT",
    "APT": "APTUSDT", "SUI": "SUIUSDT", "TON": "TONUSDT",
    "ICP": "ICPUSDT", "FIL": "FILUSDT", "ETC": "ETCUSDT",
    "XTZ": "XTZUSDT", "EGLD": "EGLDUSDT", "XLM": "XLMUSDT",
    "ALGO": "ALGOUSDT", "HBAR": "HBARUSDT", "VET": "VETUSDT",
    "OP": "OPUSDT", "ARB": "ARBUSDT", "MATIC": "MATICUSDT",
    "IMX": "IMXUSDT", "STRK": "STRKUSDT", "ZRO": "ZROUSDT",
    "LINK": "LINKUSDT", "UNI": "UNIUSDT", "AAVE": "AAVEUSDT",
    "MKR": "MKRUSDT", "SNX": "SNXUSDT", "COMP": "COMPUSDT",
    "CRV": "CRVUSDT", "LDO": "LDOUSDT", "GRT": "GRTUSDT",
    "BAL": "BALUSDT", "SUSHI": "SUSHIUSDT", "1INCH": "1INCHUSDT",
    "DYDX": "DYDXUSDT", "GMX": "GMXUSDT", "PENDLE": "PENDLEUSDT",
    "EIGEN": "EIGENUSDT", "ETHFI": "ETHFIUSDT",
    "XRP": "XRPUSDT", "LTC": "LTCUSDT", "BCH": "BCHUSDT",
    "TRX": "TRXUSDT", "XMR": "XMRUSDT", "ZEC": "ZECUSDT", "DCR": "DCRUSDT",
    "DOGE": "DOGEUSDT", "SHIB": "SHIBUSDT", "BONK": "BONKUSDT",
    "WIF": "WIFUSDT", "PEPE": "PEPEUSDT", "FLOKI": "FLOKIUSDT", "NOT": "NOTUSDT",
    "FET": "FETUSDT", "RENDER": "RENDERUSDT", "INJ": "INJUSDT",
    "SEI": "SEIUSDT", "TIA": "TIAUSDT", "PYTH": "PYTHUSDT",
    "JTO": "JTOUSDT", "HYPE": "HYPEUSDT", "MOVE": "MOVEUSDT",
    "WLD": "WLDUSDT", "ONDO": "ONDOUSDT", "TAO": "TAOUSDT", "IO": "IOUSDT",
    "STX": "STXUSDT", "ENS": "ENSUSDT", "CHZ": "CHZUSDT",
    "SAND": "SANDUSDT", "MANA": "MANAUSDT", "AXS": "AXSUSDT",
    "GMT": "GMTUSDT", "GALA": "GALAUSDT",
}



PROJECT_ROOT = get_project_root()
EXPANDER_THRESHOLD = 72.0


def _apply_tree_font(widget, style_name, size=4):
    try:
        st = ttk.Style(widget)
        st.configure(style_name, rowheight=28)
        st.configure(style_name + ".Heading", font=("TkDefaultFont", size))
    except Exception:
        pass


def _fmt(v, decimals=2):
    if v is None:
        return "-"
    try:
        return ("{:." + str(decimals) + "f}").format(float(v))
    except Exception:
        return str(v)


def _fmt_signed(v, decimals=2):
    if v is None:
        return "-"
    try:
        return ("{:+." + str(decimals) + "f}").format(float(v))
    except Exception:
        return str(v)


# ReplayWindow -- spec B2.2, 9.3: historicky replay run

def _setup_fullscreen(win):
    """Fullscreen + rotation-aware via after() polling. Works on Android/Pydroid3."""
    _fs_state = [0, 0]

    def _poll():
        if not win.winfo_exists():
            return
        try:
            sw = win.winfo_screenwidth()
            sh = win.winfo_screenheight()
            if sw != _fs_state[0] or sh != _fs_state[1]:
                _fs_state[0] = sw
                _fs_state[1] = sh
                win.geometry(str(sw) + "x" + str(sh) + "+0+0")
        except Exception:
            pass
        win.after(400, _poll)

    _poll()



class ReplayWindow(tk.Toplevel):
    """
    Umozni vyber historickeho run_id a spusti analyzu
    nad ulozenym top_set. Explicitne oznaceno jako REPLAY.
    Spec B2.2: historicky TOP pouze explicitne jako replay run.
    """

    def __init__(self, master, paths):
        super(ReplayWindow, self).__init__(master)
        self.title("[!] REPLAY MODE -- Historical Analysis")
        _setup_fullscreen(self)
        self.paths = paths
        self._sel_id = None
        self._snap_id = None
        self._run_id = None
        self._build()
        self._load_history()

    def _build(self):
        tb = ttk.Frame(self, padding=4)
        tb.pack(fill="x")
        ttk.Button(tb, text="Close", command=self.destroy).pack(side="right", padx=2)
        ttk.Button(tb, text="Refresh", command=self._load_history).pack(side="right", padx=2)
        ttk.Button(tb, text="Analysis Detail", command=self._open_detail).pack(side="right", padx=2)
        self._open_btn = ttk.Button(tb, text="Open Candidate Selection [REPLAY]",
                                     command=self._open_replay, state="disabled")
        self._open_btn.pack(side="left", padx=2)
        ttk.Separator(self).pack(fill="x")
        warn = ttk.Frame(self, padding=4)
        warn.pack(fill="x")
        ttk.Label(warn,
            text="[!]  REPLAY MODE -- working with HISTORICAL data, not current market state.",
            foreground="red").pack(side="left")
        ttk.Separator(self).pack(fill="x", pady=2)
        ttk.Label(self, text="Select historical run:").pack(anchor="w", padx=8)

        cols = ("run_id", "created_utc", "app_version", "scope",
                "vs_currency", "coins_limit", "status", "sel_id", "candidates")
        _apply_tree_font(self, "Rp.Treeview", 4)
        self.tree = ttk.Treeview(self, columns=cols, show="headings",
                                  style="Rp.Treeview", selectmode="browse")
        for c in cols:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=90, anchor="w")
        self.tree.column("run_id", width=60, anchor="e")
        self.tree.column("created_utc", width=145)
        sb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y", padx=(0, 3))
        self.tree.pack(fill="both", expand=True, padx=8, pady=4)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        self.info_var = tk.StringVar(value="")
        ttk.Label(self, textvariable=self.info_var, foreground="navy").pack(anchor="w", padx=8)


    def _load_history(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        con = None
        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            rows = con.execute(
                """SELECT r.run_id, r.created_utc, r.app_version, r.scope,
                          r.vs_currency, r.coins_limit, r.status,
                          t.selection_id,
                          COUNT(ti.unified_symbol) AS candidates
                   FROM runs r
                   LEFT JOIN topnow_selection t ON t.run_id=r.run_id
                   LEFT JOIN topnow_selection_items ti ON ti.selection_id=t.selection_id
                   GROUP BY r.run_id, t.selection_id
                   ORDER BY r.run_id DESC LIMIT 100;"""
            ).fetchall()
            for r in rows:
                self.tree.insert("", "end", values=r)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:800], parent=self)
        finally:
            if con:
                con.close()

    def _on_select(self, _event):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        run_id, created, _, scope, _, _, _, sel_id, cands = vals
        if not sel_id or str(sel_id) in ("", "None"):
            self.info_var.set(f"run_id={run_id} -- no selection found")
            self._open_btn.configure(state="disabled")
            return
        self._run_id = int(run_id)
        self._sel_id = int(sel_id)
        self.info_var.set(
            f"[REPLAY] run_id={run_id}  sel={sel_id}  "
            f"candidates={cands}  created={created}"
        )
        self._open_btn.configure(state="normal")
        con = None
        try:
            con = db_connect(self.paths.db_path)
            row = con.execute(
                "SELECT snapshot_id FROM topnow_selection WHERE selection_id=?;",
                (self._sel_id,)
            ).fetchone()
            self._snap_id = row[0] if row else None
        except Exception:
            self._snap_id = None
        finally:
            if con:
                con.close()

    def _open_replay(self):
        if not self._sel_id or not self._snap_id:
            messagebox.showinfo("Info", "Select a run with valid selection.", parent=self)
            return
        run_id = self._run_id
        CandidateSelectionWindow(
            self.master, self.paths,
            self._sel_id, self._snap_id,
            lambda: str(run_id),
        )

    def _open_detail(self):
        if not self._sel_id or not self._snap_id:
            messagebox.showinfo("Info", "Select a run first.", parent=self)
            return
        AnalyseWindow(self.master, self.paths, self._sel_id, self._snap_id)



# AnalyseWorkflowWindow -- Faze C: workflow s Run All / STEP mode
# Spec: C2, C3, C4

LAYER_REGISTRY = [
    {
        "name": "SpotBasic",
        "scope_key": "crypto_spot",
        "description": (
            "Spot data layer. Computes base_score from rank, market cap, volume and 24h change. "
            "Primary screening signal. Source: CoinGecko /coins/markets (free)."
        ),
    },
    {
        "name": "Derivatives",
        "scope_key": "crypto_derivatives",
        "description": (
            "Derivatives layer. Fetches perpetual funding rate from Binance. "
            "Positive funding = longs pay shorts (overbought signal). "
            "Source: Binance /fapi/v1/fundingRate (free, public)."
        ),
    },
    {
        "name": "OnChain",
        "scope_key": "onchain",
        "description": (
            "On-chain layer. BTC: hash rate, tx count, mempool, difficulty, miner revenue "
            "from blockchain.com (free). ETH + multi-asset (LTC, DOGE, BCH, XRP, ADA, SOL): "
            "tx volume 24h, avg fee, blocks, dominance from Blockchair (free, no key). "
            "Score: activity normalised per chain expected volume + fee health + dominance. "
            "Key decision layer (weight 0.15) per architecture spec 7.4."
        ),
    },
    {
        "name": "Institutional",
        "scope_key": "institutions",
        "description": (
            "Institutional layer. CME BTC/ETH futures price from Yahoo Finance (BTC=F, ETH=F). "
            "BTC ETF proxies: IBIT (iShares), FBTC (Fidelity), GBTC (Grayscale). "
            "ETH ETF proxies: ETHA (iShares), FETH (Fidelity). "
            "Score: CME presence boost + ETF product count signal. "
            "All sources free, public, no API key. Weight 0.01 (conservative)."
        ),
    },
    {
        "name": "Macro",
        "scope_key": "macro",
        "description": (
            "Macro layer. Global crypto market cap change 24h and BTC dominance. "
            "Provides market-wide context. Source: CoinGecko /global (free)."
        ),
    },
    {
        "name": "Sentiment",
        "scope_key": "sentiment",
        "description": (
            "Sentiment layer (supplemental only). Fear & Greed index from Alternative.me. "
            "Must NOT override structural layers in final scoring. Source: alternative.me (free)."
        ),
    },
    {
        "name": "Technical",
        "scope_key": "technical",
        "description": (
            "Technical: RSI(35%) MACD(25%) EMA-slope(20%) MeanReversion(12%) RVOL(8%). Local OHLCV, min 60 candles."
        ),
    },
    {
        "name": "Community",
        "scope_key": "community",
        "description": (
            "Community/Social layer. Fetches Reddit subscribers, Twitter followers, "
            "GitHub commit activity and CoinGecko watchlist count per coin. "
            "Higher engagement relative to market cap = stronger community signal. "
            "Source: CoinGecko /coins/{id} (free, no key). Rate: ~1 req per coin."
        ),
    },
    {
        "name": "OpenInterest",
        "scope_key": "open_interest",
        "description": (
            "Open Interest layer. Fetches USD-M futures open interest from Binance. "
            "Rising OI + rising price = trend confirmation. "
            "Falling OI + rising price = potential reversal. "
            "Source: Binance /fapi/v1/openInterest (public, no key). "
            "Stablecoins and tokens without perp markets are skipped (DEGRADED)."
        ),
    },
    {
        "name": "Fundamental",
        "scope_key": "fundamental",
        "description": (
            "Fundamental layer. GitHub developer activity: stars, forks, commit frequency. "
            "Strong dev activity = healthy ecosystem signal. "
            "Source: GitHub public API (free, 60 req/hr unauthenticated). "
            "Coverage: BTC, ETH, SOL, ADA, DOT, AVAX, LINK, ATOM, NEAR, APT, SUI, OP, ARB "
            "and 10+ more. Planned: whitepaper parser, roadmap tracker."
        ),
    },
]


class AnalyseWorkflowWindow(tk.Toplevel):
    """
    Specifikace C: Analyse workflow okno.
    Zobrazi seznam vrstev, umozni Run All nebo STEP mode.
    Kazda vrstva ma Info a Setup (disabled) tlacitka.
    """

    def __init__(self, master, paths, selection_id, snapshot_id, run_id_getter):
        super(AnalyseWorkflowWindow, self).__init__(master)
        self.title("Analyse Workflow  sel=" + str(selection_id) + "  " + APP_VERSION)
        _setup_fullscreen(self)
        self.paths = paths
        self.selection_id = int(selection_id)
        self.snapshot_id = snapshot_id
        self.run_id_getter = run_id_getter
        self._mode = tk.StringVar(value="run_all")
        self._layer_vars = {}   # scope_key -> BooleanVar (only active in STEP mode)
        self._result = {}
        self._running = False
        self._build()

    def _build(self):
        # ===== ROW 1: Mode + >EXECUTE< button at TOP (always visible on mobile) =====
        top = ttk.Frame(self, padding=6)
        top.pack(fill="x", side="top")
        ttk.Label(top, text="Mode:").pack(side="left")
        ttk.Radiobutton(top, text="Run All", variable=self._mode,
                        value="run_all", command=self._on_mode_change).pack(side="left", padx=6)
        ttk.Radiobutton(top, text="STEP", variable=self._mode,
                        value="step", command=self._on_mode_change).pack(side="left", padx=6)
        ttk.Label(top, text="  sel=" + str(self.selection_id) +
                  "  snap=" + self.snapshot_id[-16:]).pack(side="left", padx=8)
        ttk.Button(top, text="Close", command=self.destroy).pack(side="right", padx=2)
        ttk.Button(top, text="Open Results", command=self._open_results).pack(side="right", padx=2)
        # Execute button is HERE in top bar -- always visible, never scrolls off-screen
        self._run_btn = ttk.Button(top, text="  > EXECUTE <  ", command=self._execute)
        self._run_btn.pack(side="right", padx=8)
        ttk.Separator(self).pack(fill="x", side="top")

        # ===== ROW 2: Animated running banner (always visible below mode bar) =====
        banner = ttk.Frame(self, padding=(6, 4))
        banner.pack(fill="x", side="top")
        # Large spinner character -- animates in the banner
        self._banner_spin_var = tk.StringVar(value=" ")
        self._banner_spin_lbl = ttk.Label(
            banner, textvariable=self._banner_spin_var,
            font=("Courier", 13, "bold"), foreground="navy", width=2)
        self._banner_spin_lbl.pack(side="left")
        self._banner_msg_var = tk.StringVar(value="Ready -- press EXECUTE to start")
        ttk.Label(banner, textvariable=self._banner_msg_var,
                  foreground="navy").pack(side="left", padx=6)
        self._pct_var = tk.StringVar(value="")
        ttk.Label(banner, textvariable=self._pct_var,
                  foreground="darkgreen", font=("Arial", 10, "bold"),
                  width=7).pack(side="right", padx=8)
        self._eta_var = tk.StringVar(value="")
        ttk.Label(banner, textvariable=self._eta_var,
                  foreground="gray", width=10).pack(side="right", padx=4)
        # Thin global progress bar right under the banner
        self._progress = ttk.Progressbar(self, orient="horizontal", mode="determinate")
        self._progress.pack(fill="x", side="top", padx=6, pady=(0, 2))
        ttk.Separator(self).pack(fill="x", side="top")

        # ===== BOTTOM: compact log (3 lines) -- packed BEFORE layer list so it stays visible =====
        ttk.Separator(self).pack(fill="x", side="bottom")
        self.log_box = tk.Text(self, height=3, wrap="word")
        self.log_box.pack(fill="x", side="bottom", padx=6, pady=(0, 2))
        ttk.Label(self, text="Log:", anchor="w").pack(side="bottom", anchor="w", padx=6)

        # ===== MIDDLE: Layer list (fills remaining space) =====
        lf = ttk.LabelFrame(self, text="Layers (execution order)", padding=4)
        lf.pack(fill="both", expand=True, padx=6, pady=2, side="top")

        self._layer_rows = []
        for i, layer in enumerate(LAYER_REGISTRY):
            sk = layer["scope_key"]
            var = tk.BooleanVar(value=True)
            self._layer_vars[sk] = var

            row_frame = ttk.Frame(lf)
            row_frame.pack(fill="x", pady=1)

            cb = ttk.Checkbutton(row_frame, variable=var)
            cb.pack(side="left")
            cb.state(["disabled"])

            ttk.Label(row_frame, text=str(i+1) + ". " + layer["name"],
                      width=18).pack(side="left", padx=4)

            # Status label: animation is embedded HERE in the status text
            # Position: immediately after layer name, before Info/Setup
            # This is always visible on screen even on narrow mobile
            status_var = tk.StringVar(value="PENDING")
            status_lbl = ttk.Label(row_frame, textvariable=status_var, width=14, anchor="w")
            status_lbl.pack(side="left", padx=4)

            # Pct label next to status -- also in always-visible area
            pct_var = tk.StringVar(value="")
            ttk.Label(row_frame, textvariable=pct_var,
                      foreground="navy", width=5,
                      font=("Courier", 9)).pack(side="left")

            def _make_info_cmd(lyr=layer):
                def _show():
                    win = tk.Toplevel(self)
                    win.title("Layer Info: " + lyr["name"])
                    _setup_fullscreen(win)
                    tb2 = ttk.Frame(win, padding=4)
                    tb2.pack(fill="x", side="top")
                    ttk.Button(tb2, text="Close", command=win.destroy).pack(side="right")
                    txt = tk.Text(win, wrap="word", padx=8, pady=8)
                    txt.insert("end", lyr["description"])
                    # v6.2a: show capabilities from LayerAdapter
                    try:
                        adapter = get_layer_adapter(lyr["scope_key"])
                        if adapter:
                            caps = adapter.capabilities()
                            txt.insert("end", "\n\n--- Layer Contract (spec 7.1) ---\n")
                            txt.insert("end", f"Name: {caps.name}\n")
                            txt.insert("end", f"Version: {caps.version}\n")
                            txt.insert("end", f"Primary source: {caps.primary_source}\n")
                            txt.insert("end", f"Fallbacks: {', '.join(caps.fallback_sources) or 'none'}\n")
                            txt.insert("end", f"Free tier: {caps.free_tier}\n")
                            txt.insert("end", f"Requires API key: {caps.requires_api_key}\n")
                            txt.insert("end", f"Timeframes: {', '.join(caps.supported_timeframes)}\n")
                            if caps.configurable_params:
                                txt.insert("end", f"Config params: {caps.configurable_params}\n")
                    except Exception:
                        pass
                    txt.configure(state="disabled")
                    txt.pack(fill="both", expand=True)
                return _show
            ttk.Button(row_frame, text="Info", command=_make_info_cmd(layer),
                       width=6).pack(side="left", padx=4)
            # v6.3a: Setup button is now ACTIVE -- opens config dialog
            def _make_setup_cmd(lyr=layer):
                def _show_setup():
                    adapter = get_layer_adapter(lyr["scope_key"])
                    if not adapter:
                        return
                    caps = adapter.capabilities()
                    cfg = adapter.get_config()
                    win = tk.Toplevel(self)
                    win.title("Setup: " + lyr["name"])
                    _setup_fullscreen(win)
                    tb = ttk.Frame(win, padding=4)
                    tb.pack(fill="x", side="top")
                    ttk.Button(tb, text="Close", command=win.destroy).pack(side="right")
                    ttk.Button(tb, text="Reset defaults", command=lambda: (
                        adapter.set_config(dict(caps.configurable_params)),
                        win.destroy())).pack(side="right", padx=4)

                    body = ttk.Frame(win, padding=8)
                    body.pack(fill="both", expand=True)
                    ttk.Label(body, text=f"Layer: {caps.name}  v{caps.version}",
                              font=("TkDefaultFont", 5, "bold")).pack(anchor="w")
                    ttk.Label(body, text=f"Source: {caps.primary_source} + {', '.join(caps.fallback_sources) or 'none'}").pack(anchor="w")
                    ttk.Separator(body).pack(fill="x", pady=6)

                    entries = {}
                    if not cfg:
                        ttk.Label(body, text="No configurable parameters for this layer.").pack(anchor="w")
                    else:
                        for key, default_val in cfg.items():
                            row = ttk.Frame(body)
                            row.pack(fill="x", pady=2)
                            ttk.Label(row, text=key, width=30, anchor="w").pack(side="left")
                            sv = tk.StringVar(value=str(default_val))
                            ttk.Entry(row, textvariable=sv, width=20).pack(side="left", padx=4)
                            entries[key] = sv

                    def _apply():
                        overrides = {}
                        for key, sv in entries.items():
                            val = sv.get().strip()
                            try:
                                if val.lower() in ("true", "false"):
                                    overrides[key] = val.lower() == "true"
                                elif "." in val:
                                    overrides[key] = float(val)
                                else:
                                    overrides[key] = int(val)
                            except ValueError:
                                overrides[key] = val
                        adapter.set_config(overrides)
                        self._log(f"Setup [{lyr['name']}]: {overrides}")
                        win.destroy()

                    ttk.Separator(body).pack(fill="x", pady=6)
                    ttk.Button(body, text="Apply", command=_apply).pack(anchor="e")
                return _show_setup
            setup_btn = ttk.Button(row_frame, text="Setup", width=7,
                                   command=_make_setup_cmd(layer))
            setup_btn.pack(side="left", padx=2)

            self._layer_rows.append({
                "layer": layer, "cb": cb,
                "status_var": status_var, "status_lbl": status_lbl,
                "pct_var": pct_var,
                "_spin_frame": 0, "_spinning": False, "_spin_id": None,
            })


    def _on_mode_change(self):
        mode = self._mode.get()
        for row in self._layer_rows:
            cb = row["cb"]
            if mode == "step":
                cb.state(["!disabled"])
            else:
                cb.state(["disabled"])
                self._layer_vars[row["layer"]["scope_key"]].set(True)

    def _log(self, msg):
        self.log_box.insert("end", utc_now_iso() + " " + msg + "\n")
        self.log_box.see("end")
        self.update_idletasks()

    def _set_status_color(self, row, status):
        """Update status label text and foreground color (thread-safe via after)."""
        # Status colors -- RUNNING X handled by startswith("RUNNING")
        if status.startswith("RUNNING"):
            row["status_var"].set(status)  # keep animated text set by spinner
            try:
                row["status_lbl"].configure(foreground="navy")
            except Exception:
                pass
            return
        color_map = {
            "PENDING":  "gray",
            "OK":        "darkgreen",
            "SKIPPED":  "gray",
            "DEGRADED": "darkorange",
            "ERROR":    "red",
        }
        row["status_var"].set(status)
        fg = color_map.get(status, "black")
        try:
            row["status_lbl"].configure(foreground=fg)
        except Exception:
            pass

    def _start_spin(self, row):
        """Start spinner animation on a layer row (main thread only).
        Animation runs in the status_var text AND the top banner.
        """
        row["_spin_frame"] = 0
        row["_spinning"] = True
        self._tick_spin(row)

    def _tick_spin(self, row):
        """Advance one spinner frame -- updates status text AND banner."""
        if not row.get("_spinning"):
            return
        frame = row["_spin_frame"] % len(_SPIN_FRAMES)
        ch = _SPIN_FRAMES[frame]
        # Animate the STATUS LABEL text (always visible between name and Info)
        try:
            row["status_var"].set("RUNNING " + ch)
        except Exception:
            return
        # Also animate the top banner spinner
        try:
            self._banner_spin_var.set(ch)
        except Exception:
            pass
        row["_spin_frame"] = frame + 1
        try:
            sid = self.after(140, lambda r=row: self._tick_spin(r))
            row["_spin_id"] = sid
        except Exception:
            pass

    def _stop_spin(self, row, pct_final=None):
        """Stop spinner and set final percentage label."""
        row["_spinning"] = False
        if row.get("_spin_id"):
            try:
                self.after_cancel(row["_spin_id"])
            except Exception:
                pass
            row["_spin_id"] = None
        try:
            row["pct_var"].set("" if pct_final is None else str(pct_final) + "%")
        except Exception:
            pass
        # Reset banner spinner only when all rows finished
        still_spinning = any(r.get("_spinning") for r in self._layer_rows)
        if not still_spinning:
            try:
                self._banner_spin_var.set(" ")
            except Exception:
                pass

    def _execute(self):
        if self._running:
            return
        self._running = True
        self._run_btn.configure(state="disabled")
        for row in self._layer_rows:
            self._set_status_color(row, "PENDING")
            self._stop_spin(row)
            row["pct_var"].set("")
        self._progress["value"] = 0
        self._pct_var.set("0%")
        self._eta_var.set("")
        self._banner_msg_var.set("Starting...")
        self._banner_spin_var.set(" ")
        self.log_box.delete("1.0", "end")

        mode = self._mode.get()
        active_scopes = []
        for row in self._layer_rows:
            sk = row["layer"]["scope_key"]
            if mode == "run_all" or self._layer_vars[sk].get():
                active_scopes.append(sk)

        self._log(f"MODE={mode}  layers={len(active_scopes)}  sel={self.selection_id}")

        import threading
        t = threading.Thread(
            target=self._execute_worker,
            args=(active_scopes,),
            daemon=True,
        )
        t.start()

    def _execute_worker(self, active_scopes):
        import time as _t
        con = None
        n_total   = len(active_scopes)
        n_done    = 0
        t_start   = _t.time()

        def _ui(fn):
            """Schedule fn() on main thread."""
            try:
                self.after(0, fn)
            except Exception:
                pass

        def _log_ui(msg):
            _ui(lambda m=msg: (
                self.log_box.insert("end", utc_now_iso() + " " + m + "\n"),
                self.log_box.see("end"),
            ))

        def _update_progress(done, total, layer_name):
            pct = int(done / total * 100) if total > 0 else 0
            elapsed = _t.time() - t_start
            if done > 0 and elapsed > 0:
                rate  = done / elapsed
                remaining = (total - done) / rate if rate > 0 else 0
                eta_str = "ETA ~" + str(int(remaining)) + "s"
            else:
                eta_str = ""
            _ui(lambda p=pct, e=eta_str: (
                self._progress.configure(value=p),
                self._pct_var.set(str(p) + "%"),
                self._eta_var.set(e),
            ))

        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            run_id_val = None
            try:
                rv = self.run_id_getter()
                run_id_val = int(rv) if rv and rv not in ("-", "") else None
            except Exception:
                pass

            for row in self._layer_rows:
                sk    = row["layer"]["scope_key"]
                lname = row["layer"]["name"]

                if sk not in active_scopes:
                    _ui(lambda r=row: self._set_status_color(r, "SKIPPED"))
                    _log_ui(f"[{lname}] SKIPPED")
                    continue

                # Mark RUNNING + start spinner
                _ui(lambda r=row, ln=lname: (
                    self._set_status_color(r, "RUNNING |"),
                    r["pct_var"].set(""),
                    self._banner_msg_var.set("Running: " + ln),
                    self._start_spin(r),
                ))
                _log_ui(f"[{lname}] starting...")

                try:
                    res = prepare_and_store_composite_preview(
                        con, self.selection_id, [sk],
                        run_id=run_id_val,
                    )
                    n_done += 1
                    pct_now = int(n_done / n_total * 100) if n_total > 0 else 100
                    updated = res.get("updated_items", 0)
                    # Detect degraded: updated but some scores None
                    tech_ok  = res.get("technical_symbols",  -1)
                    comm_ok  = res.get("community_symbols",  -1)
                    oi_ok    = res.get("oi_symbols",         -1)
                    sent_ok  = res.get("sentiment_ok",       None)
                    macro_ok = res.get("macro_ok",           None)

                    # sent_ok/macro_ok tri-state: True=ok, None=not_in_scope, False=failed
                    # None means layer wasn't requested -- not DEGRADED, just SKIPPED implicitly
                    is_degraded = (
                        (sk == "technical"      and tech_ok  == 0) or
                        (sk == "community"      and comm_ok  == 0) or
                        (sk == "open_interest"  and oi_ok    == 0) or
                        (sk == "sentiment"      and sent_ok  is False) or
                        (sk == "macro"          and macro_ok is False)
                    )
                    # If sentiment/macro returned None (not in scope), treat as OK for this layer
                    if sk == "sentiment" and sent_ok  is None: is_degraded = False
                    if sk == "macro"     and macro_ok is None: is_degraded = False
                    final_status = "DEGRADED" if is_degraded else "OK"
                    _ui(lambda r=row, s=final_status: (
                        self._stop_spin(r, 100),
                        self._set_status_color(r, s),
                    ))
                    _log_ui(f"[{lname}] {final_status}  updated={updated}")

                except Exception as exc:
                    n_done += 1
                    pct_now = int(n_done / n_total * 100) if n_total > 0 else 100
                    _ui(lambda r=row: (
                        self._stop_spin(r, 100),
                        self._set_status_color(r, "ERROR"),
                    ))
                    _log_ui(f"[{lname}] ERROR: {str(exc)[:180]}")

                _update_progress(n_done, n_total, lname)

            # --- v6.3a: Post-analysis pipeline (Feature → Prediction → TradePlan) ---
            _log_ui("Post-analysis: building feature vectors...")
            _ui(lambda: self._banner_msg_var.set("Building feature vectors..."))
            try:
                n_fv = persist_feature_vectors(con, run_id_val, self.selection_id, log_cb=_log_ui)
                _log_ui(f"FeatureLayer: {n_fv} vectors stored")
            except Exception as exc:
                _log_ui(f"FeatureLayer ERROR: {str(exc)[:120]}")

            _ui(lambda: self._banner_msg_var.set("Generating predictions..."))
            try:
                n_pred = persist_predictions(con, run_id_val, self.selection_id, log_cb=_log_ui)
                _log_ui(f"PredictionLayer: {n_pred} predictions generated")
            except Exception as exc:
                _log_ui(f"PredictionLayer ERROR: {str(exc)[:120]}")

            _ui(lambda: self._banner_msg_var.set("Generating trade plans..."))
            try:
                n_tp = persist_trade_plans(con, run_id_val, self.selection_id, log_cb=_log_ui)
                _log_ui(f"TradePlan: {n_tp} plans generated")
            except Exception as exc:
                _log_ui(f"TradePlan ERROR: {str(exc)[:120]}")

            # Store layer setup configs for audit (spec C2.3)
            try:
                configs = {}
                for sk, adapter in LAYER_ADAPTERS.items():
                    cfg = adapter.get_config()
                    if cfg:
                        configs[sk] = cfg
                if configs and run_id_val:
                    store_run_params(con, run_id_val, configs)
            except Exception:
                pass

            # Final 100%
            _ui(lambda: (
                self._progress.configure(value=100),
                self._pct_var.set("100%"),
                self._eta_var.set("Done"),
                self._banner_msg_var.set("DONE -- all layers + predictions + trade plans"),
                self._banner_spin_var.set(" "),
            ))
            self._result = {"sel": self.selection_id, "snap": self.snapshot_id}
            elapsed_s = int(_t.time() - t_start)
            _log_ui(f"DONE -- {n_done}/{n_total} layers in {elapsed_s}s")

        except Exception as e:
            _log_ui("FATAL: " + str(e)[:300])
        finally:
            if con:
                con.close()
            _ui(lambda: (
                [self._stop_spin(r) for r in self._layer_rows],
                self._run_btn.configure(state="normal"),
                setattr(self, "_running", False),
            ))

    def _open_results(self):
        AnalyseWindow(self.master, self.paths, self.selection_id, self.snapshot_id)


# CandidateSelectionWindow -- Faze B: vyber kandidatu s checkboxy
# Spec: B2.3

class CandidateSelectionWindow(tk.Toplevel):
    """
    Zobrazi TOP kandidaty ze selection_id s checkboxy.
    Uzivatel muze deselektovat jednotlive symboly pred analyzou.
    Vysledek: podmnozina symbolu predana do AnalyseWorkflowWindow.
    """

    def __init__(self, master, paths, selection_id, snapshot_id, run_id_getter):
        super(CandidateSelectionWindow, self).__init__(master)
        self.title("Candidate Selection  sel=" + str(selection_id))
        _setup_fullscreen(self)
        self.paths = paths
        self.selection_id = int(selection_id)
        self.snapshot_id = snapshot_id
        self.run_id_getter = run_id_getter
        self._sym_vars = {}
        self._build()
        self._load()

    def _build(self):
        top = ttk.Frame(self, padding=6)
        top.pack(fill="x")
        ttk.Button(top, text="Close", command=self.destroy).pack(side="right", padx=2)
        ttk.Button(top, text="Analyse selected", command=self._analyse_selected).pack(side="right", padx=2)
        ttk.Label(top, text="Select candidates for analysis (uncheck to exclude):").pack(side="left")
        ttk.Button(top, text="All", command=self._all).pack(side="left", padx=4)
        ttk.Button(top, text="None", command=self._none).pack(side="left", padx=2)

        self.canvas = tk.Canvas(self)
        sb = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y", padx=(0, 3))
        self.canvas.pack(fill="both", expand=True, padx=6, pady=4)
        self.inner = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.inner.bind("<Configure>", lambda e: self.canvas.configure(
            scrollregion=self.canvas.bbox("all")))




    def _load(self):
        for w in self.inner.winfo_children():
            w.destroy()
        self._sym_vars.clear()
        con = None
        try:
            con = db_connect(self.paths.db_path)
            rows = con.execute(
                """SELECT i.rank_in_selection, i.unified_symbol, m.rank, m.price,
                          m.change_24h_pct, m.base_score
                   FROM topnow_selection_items i
                   LEFT JOIN market_snapshots m
                        ON m.snapshot_id=? AND m.timeframe='spot'
                        AND m.unified_symbol=i.unified_symbol
                   WHERE i.selection_id=?
                   ORDER BY i.rank_in_selection ASC;""",
                (self.snapshot_id, self.selection_id)
            ).fetchall()
            # Header
            hf = ttk.Frame(self.inner)
            hf.pack(fill="x", padx=4)
            for col, w in [("", 3), ("#", 5), ("Symbol", 8), ("MktRank", 8),
                            ("Price", 12), ("Chg24%", 8), ("Score", 7)]:
                ttk.Label(hf, text=col, width=w, anchor="w").pack(side="left")
            ttk.Separator(self.inner, orient="horizontal").pack(fill="x", padx=4, pady=2)

            for sel_rank, sym, mkt_rank, price, chg24, score in rows:
                var = tk.BooleanVar(value=True)
                self._sym_vars[sym] = var
                rf = ttk.Frame(self.inner)
                rf.pack(fill="x", padx=4, pady=1)
                ttk.Checkbutton(rf, variable=var, width=3).pack(side="left")
                ttk.Label(rf, text=str(sel_rank), width=5, anchor="e").pack(side="left")
                ttk.Label(rf, text=sym, width=8, anchor="w").pack(side="left")
                ttk.Label(rf, text=str(mkt_rank or "-"), width=8, anchor="e").pack(side="left")
                ttk.Label(rf, text=_fmt(price, 4), width=12, anchor="e").pack(side="left")
                ttk.Label(rf, text=_fmt(chg24, 2), width=8, anchor="e").pack(side="left")
                ttk.Label(rf, text=_fmt(score, 1), width=7, anchor="e").pack(side="left")
        except Exception as e:
            ttk.Label(self.inner, text="Error: " + str(e)[:200]).pack()
        finally:
            if con:
                con.close()

    def _all(self):
        for v in self._sym_vars.values():
            v.set(True)

    def _none(self):
        for v in self._sym_vars.values():
            v.set(False)

    def _analyse_selected(self):
        selected = [sym for sym, var in self._sym_vars.items() if var.get()]
        if not selected:
            messagebox.showinfo("Info", "No candidates selected.", parent=self)
            return
        self.destroy()
        AnalyseWorkflowWindow(
            self.master, self.paths,
            self.selection_id, self.snapshot_id,
            self.run_id_getter,
        )


class AnalyseWindow(tk.Toplevel):
    def __init__(self, master, paths, selection_id, snapshot_id):
        super(AnalyseWindow, self).__init__(master)
        self.title("Analysis Detail  sel=" + str(selection_id))
        _setup_fullscreen(self)
        self.paths = paths
        self.selection_id = int(selection_id)
        self.snapshot_id = snapshot_id
        self._build()
        self._load()

    def _build(self):
        top = ttk.Frame(self, padding=6)
        top.pack(fill="x")
        ttk.Button(top, text="Close", command=self.destroy).pack(side="right", padx=2)
        ttk.Button(top, text="Refresh", command=self._load).pack(side="right", padx=2)
        ttk.Label(top, text="selection_id: " + str(self.selection_id)).pack(side="left")
        ttk.Label(top, text="snap: " + self.snapshot_id[-20:]).pack(side="left", padx=8)
        ttk.Label(top, text="Expander >= " + str(EXPANDER_THRESHOLD)).pack(side="left", padx=8)
        cols = ("rank", "symbol", "composite", "spot_basic",
                "technical", "community", "funding", "expander")
        _apply_tree_font(self, "An.Treeview", 4)
        self.tree = ttk.Treeview(self, columns=cols, show="headings", style="An.Treeview")
        for c in cols:
            self.tree.heading(c, text=c, command=lambda _c=c: self._sort(_c, False))
        self.tree.column("rank",       width=46,  anchor="e")
        self.tree.column("symbol",     width=74,  anchor="w")
        self.tree.column("composite",  width=74,  anchor="e")
        self.tree.column("spot_basic", width=74,  anchor="e")
        self.tree.column("technical",  width=74,  anchor="e")
        self.tree.column("community",  width=74,  anchor="e")
        self.tree.column("funding",    width=74,  anchor="e")
        self.tree.column("expander",   width=60,  anchor="center")
        # Market environment label (Sentiment + Macro shown in header)
        self._env_var = tk.StringVar(value="")
        ttk.Label(top, textvariable=self._env_var, foreground="navy").pack(side="left", padx=12)
        self.tree.tag_configure("exp", background="#d4edda")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)
        self.detail = tk.Text(self, height=6, wrap="word")
        self.detail.pack(fill="x", padx=6)



    def _sort(self, col, desc):
        def _key(k):
            v = self.tree.set(k, col)
            try:
                return (1, float(v))
            except Exception:
                return (0, v.lower()) if v and v != "-" else (2, "")
        items = sorted(self.tree.get_children(""), key=_key, reverse=desc)
        for i, k in enumerate(items):
            self.tree.move(k, "", i)
        self.tree.heading(col, command=lambda _c=col: self._sort(_c, not desc))

    def _load(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        con = None
        try:
            con = db_connect(self.paths.db_path)
            rows = con.execute(
                "SELECT rank_in_selection, unified_symbol, composite_preview, reason_json "
                "FROM topnow_selection_items WHERE selection_id=? "
                "ORDER BY rank_in_selection ASC;",
                (self.selection_id,),
            ).fetchall()
            for rank, sym, comp, rj in rows:
                r = {}
                if rj:
                    try:
                        r = json.loads(rj)
                    except Exception:
                        pass
                def _sc(key):
                    v = r.get(key)
                    if isinstance(v, dict):
                        s = v.get("score")
                        return _fmt(s, 1) if s is not None else "-"
                    return _fmt(v, 1) if isinstance(v, (int, float)) else "-"
                comp_s = _fmt(comp, 1)
                is_exp = isinstance(comp, (int, float)) and comp >= EXPANDER_THRESHOLD
                self.tree.insert("", "end",
                    values=(rank, sym, comp_s,
                            _sc("spot_basic"),
                            _sc("technical"),
                            _sc("community"),
                            _sc("derivatives_funding"),
                            "YES" if is_exp else "-"),
                    tags=("exp",) if is_exp else ())
            # Build environment label from first row that has sentiment/macro
            env_parts = []
            for _, _, _, rj in rows:
                if not rj:
                    continue
                try:
                    r2 = json.loads(rj)
                    s = r2.get("sentiment_fng", {})
                    m = r2.get("macro_global", {})
                    sv = s.get("score") if isinstance(s, dict) else None
                    mv = m.get("score") if isinstance(m, dict) else None
                    if sv is not None:
                        fng = s.get("fng_value", "?")
                        fng_cls = s.get("fng_classification", "")
                        env_parts.append("Sentiment: " + str(int(sv)) + " (FNG=" + str(fng) + " " + fng_cls + ")")
                    if mv is not None:
                        env_parts.append("Macro: " + str(int(mv)))
                    if env_parts:
                        break
                except Exception:
                    pass
            self._env_var.set("  |  ".join(env_parts) if env_parts else "")
        except Exception as e:
            messagebox.showerror("Error", str(e)[:1200], parent=self)
        finally:
            if con:
                con.close()

    def _on_select(self, _event):
        sel = self.tree.selection()
        if not sel:
            return
        sym = self.tree.item(sel[0], "values")[1]
        con = None
        try:
            con = db_connect(self.paths.db_path)
            row = con.execute(
                "SELECT reason_json FROM topnow_selection_items "
                "WHERE selection_id=? AND unified_symbol=?;",
                (self.selection_id, sym),
            ).fetchone()
            self.detail.delete("1.0", "end")
            if row and row[0]:
                try:
                    d = json.loads(row[0])
                    self.detail.insert("end",
                        "[" + sym + "]\n" + json.dumps(d, indent=2, ensure_ascii=True))
                except Exception:
                    self.detail.insert("end", row[0])
            else:
                self.detail.insert("end", "[" + sym + "] -- no reason data")
        except Exception as e:
            self.detail.delete("1.0", "end")
            self.detail.insert("end", "Error: " + str(e))
        finally:
            if con:
                con.close()


class DiffWindow(tk.Toplevel):
    def __init__(self, master, paths):
        super(DiffWindow, self).__init__(master)
        self.title("Snapshot Diff")
        _setup_fullscreen(self)
        self.paths = paths
        self._build()
        self._run()

    def _build(self):
        top = ttk.Frame(self, padding=6)
        top.pack(fill="x")
        self.info = tk.StringVar(value="")
        ttk.Button(top, text="Close", command=self.destroy).pack(side="right", padx=2)
        ttk.Button(top, text="Add selected to Watchlist", command=self._add_to_watchlist).pack(side="right", padx=2)
        ttk.Button(top, text="Unselect all", command=self._unselect_all).pack(side="right", padx=2)
        ttk.Button(top, text="Select all", command=self._select_all).pack(side="right", padx=2)
        ttk.Label(top, textvariable=self.info).pack(side="left")
        ttk.Button(top, text="Refresh", command=self._run).pack(side="left", padx=8)
        cols = ("symbol", "d_score", "d_price", "d_chg24", "rank_improve")
        _apply_tree_font(self, "Df.Treeview", 4)
        self.tree = ttk.Treeview(self, columns=cols, show="headings",
                                  style="Df.Treeview", selectmode="extended")
        for c in cols:
            self.tree.heading(c, text=c, command=lambda _c=c: self._sort(_c, False))
        self.tree.column("symbol",       width=90,  anchor="w")
        self.tree.column("d_score",      width=110, anchor="center")
        self.tree.column("d_price",      width=110, anchor="center")
        self.tree.column("d_chg24",      width=110, anchor="center")
        self.tree.column("rank_improve", width=110, anchor="center")
        self.tree.tag_configure("up",   background="#d4edda")
        self.tree.tag_configure("down", background="#f8d7da")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)



    def _sort(self, col, desc):
        def _key(k):
            v = self.tree.set(k, col)
            try:
                return (1, float(str(v).replace(",", "").replace("+", "")))
            except Exception:
                return (0, str(v).lower()) if v and v != "-" else (2, "")
        items = sorted(self.tree.get_children(""), key=_key, reverse=desc)
        for i, k in enumerate(items):
            self.tree.move(k, "", i)
        self.tree.heading(col, command=lambda _c=col: self._sort(_c, not desc))

    def _select_all(self):
        self.tree.selection_set(self.tree.get_children())

    def _unselect_all(self):
        self.tree.selection_remove(self.tree.get_children())

    def _add_to_watchlist(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Select symbols first.", parent=self)
            return
        con = None
        added = 0
        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            try:
                con.execute("ALTER TABLE watchlist ADD COLUMN exit_timestamp_utc TEXT;")
            except Exception:
                pass
            try:
                con.execute("ALTER TABLE watchlist ADD COLUMN exit_reason TEXT;")
            except Exception:
                pass
            existing = set(r[0] for r in con.execute(
                "SELECT unified_symbol FROM watchlist WHERE exit_timestamp_utc IS NULL;"
            ).fetchall())
            snap = self.info.get().split("b=")[-1].strip() if hasattr(self, "info") else ""
            snap_id = "snap_" + snap if snap and not snap.startswith("snap_") else snap
            for item in sel:
                sym = self.tree.item(item, "values")[0]
                if sym in existing:
                    continue
                con.execute(
                    "INSERT OR IGNORE INTO watchlist (unified_symbol, tag, stage,"
                    " tracking_since_utc, entry_snapshot_id, entry_score)"
                    " VALUES (?,?,?,?,?,?);",
                    (sym, "diff", "0", utc_now_iso(), snap_id, 0.0)
                )
                added += 1
            con.commit()
            messagebox.showinfo("Watchlist",
                "Added " + str(added) + " symbols to Watchlist.\n"
                + str(len(sel) - added) + " already in Watchlist.", parent=self)
        except Exception as e:
            if con:
                con.rollback()
            messagebox.showerror("Error", str(e)[:800], parent=self)
        finally:
            if con:
                con.close()

    def _run(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        con = None
        try:
            con = db_connect(self.paths.db_path)
            snaps = con.execute(
                "SELECT snapshot_key FROM snapshots ORDER BY created_utc DESC LIMIT 2;"
            ).fetchall()
            if len(snaps) < 2:
                self.info.set("Need at least 2 snapshots")
                return
            b, a = snaps[0][0], snaps[1][0]
            self.info.set("a=" + a[-16:] + "  ->  b=" + b[-16:])
            rep = snapshot_diff_summary(con, a, b, timeframe="spot", limit=60)
            for it in rep.get("top_movers", []):
                sym = it.get("symbol", "")
                ds = it.get("d_score")
                tag = "up" if isinstance(ds, float) and ds > 0 else "down"
                self.tree.insert("", "end", values=(
                    sym,
                    _fmt_signed(ds, 1),
                    _fmt_signed(it.get("d_price"), 4),
                    _fmt_signed(it.get("d_chg24"), 2),
                    _fmt_signed(it.get("rank_improve"), 0),
                ), tags=(tag,))
        except Exception as e:
            self.info.set("Error: " + str(e)[:100])
        finally:
            if con:
                con.close()


class ResultWindow(tk.Toplevel):
    def __init__(self, master, paths, selection_id, snapshot_id):
        super(ResultWindow, self).__init__(master)
        self.title("Results  sel=" + str(selection_id) + "  -- NyoSig " + APP_VERSION)
        _setup_fullscreen(self)
        self.paths = paths
        self.selection_id = int(selection_id)
        self.snapshot_id = snapshot_id
        self._build()
        self._load()

    def _build(self):
        top = ttk.Frame(self, padding=6)
        top.pack(fill="x")
        ttk.Button(top, text="Close", command=self.destroy).pack(side="right", padx=2)
        ttk.Button(top, text="Refresh", command=self._load).pack(side="right", padx=2)
        ttk.Label(top, text="snap: " + self.snapshot_id[-20:]).pack(side="left")
        ttk.Label(top, text="sel: " + str(self.selection_id)).pack(side="left", padx=8)
        ttk.Label(top, text="Expander >= " + str(EXPANDER_THRESHOLD)).pack(side="left", padx=8)
        cols = ("rank", "symbol", "composite", "mrank", "mcap", "vol24", "chg24", "price")
        _apply_tree_font(self, "Res.Treeview", 4)
        self.tree = ttk.Treeview(self, columns=cols, show="headings", style="Res.Treeview")
        for c in cols:
            self.tree.heading(c, text=c, command=lambda _c=c: self._sort(_c, False))
        self.tree.column("rank",      width=40,  anchor="e", minwidth=40)
        self.tree.column("symbol",    width=70,  anchor="w", minwidth=60)
        self.tree.column("composite", width=65,  anchor="e", minwidth=50)
        self.tree.column("mrank",     width=50,  anchor="e", minwidth=40)
        self.tree.column("mcap",      width=100, anchor="e", minwidth=80)
        self.tree.column("vol24",     width=100, anchor="e", minwidth=80)
        self.tree.column("chg24",     width=60,  anchor="e", minwidth=50)
        self.tree.column("price",     width=90,  anchor="e", minwidth=70)
        self.tree.tag_configure("exp", background="#d4edda")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)


    def _sort(self, col, desc):
        def _key(k):
            v = self.tree.set(k, col)
            try:
                return (1, float(str(v).replace(",", "")))
            except Exception:
                return (0, str(v).lower()) if v and v != "-" else (2, "")
        items = sorted(self.tree.get_children(""), key=_key, reverse=desc)
        for i, k in enumerate(items):
            self.tree.move(k, "", i)
        self.tree.heading(col, command=lambda _c=col: self._sort(_c, not desc))

    def _load(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        con = None
        try:
            con = db_connect(self.paths.db_path)
            rows = con.execute(
                "SELECT rank_in_selection, unified_symbol, composite_preview"
                " FROM topnow_selection_items"
                " WHERE selection_id=?"
                " ORDER BY rank_in_selection ASC;",
                (self.selection_id,),
            ).fetchall()
            if not rows:
                messagebox.showinfo("Debug", "No items for selection_id=" + str(self.selection_id), parent=self)
                return
            # enrich with market data
            market = {}
            for mrow in con.execute(
                "SELECT unified_symbol, rank, mcap, vol24, change_24h_pct, price"
                " FROM market_snapshots WHERE snapshot_id=? AND timeframe='spot';",
                (self.snapshot_id,)
            ).fetchall():
                market[mrow[0]] = mrow[1:]
            rows2 = []
            for r in rows:
                m2 = market.get(r[1], (None, None, None, None, None))
                rows2.append((r[0], r[1], r[2], m2[0], m2[1], m2[2], m2[3], m2[4]))
            rows = rows2
            self.title("Results sel=" + str(self.selection_id) + " rows=" + str(len(rows)))
            for r in rows:
                rank, sym, comp = r[0], r[1], r[2]
                is_exp = isinstance(comp, (int, float)) and comp >= EXPANDER_THRESHOLD
                tag = ("exp",) if is_exp else ("normal",)
                self.tree.insert("", "end", iid=str(rank) + sym, values=(
                    rank, sym, _fmt(comp, 1),
                    r[3] or "-",
                    _fmt(r[4], 0), _fmt(r[5], 0),
                    _fmt(r[6], 2), _fmt(r[7], 4),
                ), tags=tag)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:1200], parent=self)
        finally:
            if con:
                con.close()


class WatchlistWindow(tk.Toplevel):
    def __init__(self, master, paths, snapshot_id_getter):
        super(WatchlistWindow, self).__init__(master)
        self.title("Watchlist")
        _setup_fullscreen(self)
        self.paths = paths
        self.snapshot_id_getter = snapshot_id_getter
        self._build()
        self._load()

    def _db(self):
        con = db_connect(self.paths.db_path)
        ensure_schema(con)
        # safety: ensure exit columns exist regardless
        try:
            con.execute("ALTER TABLE watchlist ADD COLUMN exit_timestamp_utc TEXT;")
        except Exception:
            pass
        try:
            con.execute("ALTER TABLE watchlist ADD COLUMN exit_reason TEXT;")
        except Exception:
            pass
        return con

    def _build(self):
        # --- ROW 1: Add asset ---
        top = ttk.Frame(self, padding=6)
        top.pack(fill="x")
        ttk.Button(top, text="Close", command=self.destroy).pack(side="right", padx=2)
        ttk.Label(top, text="Symbol").pack(side="left")
        self.sym   = tk.StringVar()
        self.tag   = tk.StringVar()
        self.stage = tk.StringVar(value="new")
        self._sym_entry = ttk.Entry(top, textvariable=self.sym, width=10)
        self._sym_entry.pack(side="left", padx=4)
        self.sym.trace_add("write", self._on_sym_change)
        self._ac_win = None
        ttk.Label(top, text="Tag").pack(side="left")
        ttk.Entry(top, textvariable=self.tag,   width=12).pack(side="left", padx=4)
        ttk.Label(top, text="Stage").pack(side="left")
        ttk.Entry(top, textvariable=self.stage, width=10).pack(side="left", padx=4)
        ttk.Button(top, text="Add", command=self._add).pack(side="left", padx=4)
        ttk.Button(top, text="Refresh metrics", command=self._refresh).pack(side="left", padx=4)
        ttk.Separator(self).pack(fill="x")
        # --- ROW 2: Actions ---
        r2 = ttk.Frame(self, padding=4)
        r2.pack(fill="x")
        ttk.Button(r2, text="Exit position",  command=self._exit_position).pack(side="left", padx=2)
        ttk.Button(r2, text="Change stage",   command=self._change_stage).pack(side="left", padx=2)
        ttk.Button(r2, text="Show alerts",    command=self._alerts).pack(side="left", padx=2)
        ttk.Button(r2, text="Enriched View",  command=self._show_enriched).pack(side="left", padx=2)
        ttk.Button(r2, text="Signal History", command=self._show_history).pack(side="left", padx=2)
        ttk.Separator(r2, orient="vertical").pack(side="left", fill="y", padx=6)
        ttk.Button(r2, text="Remove selected",  command=self._remove).pack(side="left", padx=2)
        ttk.Button(r2, text="Remove ALL",        command=self._remove_all).pack(side="left", padx=2)
        ttk.Button(r2, text="Remove by score...", command=self._remove_by_score).pack(side="left", padx=2)
        ttk.Button(r2, text="Remove by stage...", command=self._remove_by_stage).pack(side="left", padx=2)
        ttk.Separator(self).pack(fill="x")
        cols = ("watch_id", "symbol", "tag", "stage", "since",
                "entry_snap", "entry_score",
                "last_utc", "last_price", "last_chg24", "last_score")
        _apply_tree_font(self, "Wl.Treeview", 4)
        self.tree = ttk.Treeview(self, columns=cols, show="headings", style="Wl.Treeview")
        for c in cols:
            self.tree.heading(c, text=c, command=lambda _c=c: self._sort(_c, False))
            self.tree.column(c, width=100, anchor="w")
        self.tree.column("watch_id", width=65)
        self.tree.column("symbol",   width=80)
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)


    def _sort(self, col, desc):
        def _key(k):
            v = self.tree.set(k, col)
            try:
                return (1, float(str(v).replace(",", "").replace("+", "")))
            except Exception:
                return (0, str(v).lower()) if v and v != "-" else (2, "")
        items = sorted(self.tree.get_children(""), key=_key, reverse=desc)
        for i, k in enumerate(items):
            self.tree.move(k, "", i)
        self.tree.heading(col, command=lambda _c=col: self._sort(_c, not desc))

    def _on_sym_change(self, *args):
        txt = self.sym.get().strip().upper()
        if self._ac_win:
            try:
                self._ac_win.destroy()
            except Exception:
                pass
            self._ac_win = None
        if len(txt) < 1:
            return
        con = None
        try:
            con = self._db()
            rows = con.execute(
                "SELECT DISTINCT unified_symbol FROM market_snapshots"
                " WHERE unified_symbol LIKE ? ORDER BY unified_symbol LIMIT 20;",
                (txt + "%",)
            ).fetchall()
        except Exception:
            rows = []
        finally:
            if con:
                con.close()
        if not rows:
            return
        # show dropdown
        x = self._sym_entry.winfo_rootx()
        y = self._sym_entry.winfo_rooty() + self._sym_entry.winfo_height()
        win = tk.Toplevel(self)
        win.wm_overrideredirect(True)
        win.geometry("120x" + str(min(len(rows), 10) * 22) + "+" + str(x) + "+" + str(y))
        self._ac_win = win
        lb = tk.Listbox(win, font=("TkDefaultFont", 4), activestyle="dotbox")
        lb.pack(fill="both", expand=True)
        for r in rows:
            lb.insert("end", r[0])
        def _pick(evt):
            sel = lb.curselection()
            if sel:
                self.sym.set(lb.get(sel[0]))
            try:
                win.destroy()
            except Exception:
                pass
            self._ac_win = None
        lb.bind("<ButtonRelease-1>", _pick)
        lb.bind("<Return>", _pick)
        # close on focus loss
        win.bind("<FocusOut>", lambda e: win.destroy())

    def _load(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        con = None
        try:
            con = self._db()
            for r in list_watch(con):
                self.tree.insert("", "end", values=r)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:1200], parent=self)
        finally:
            if con:
                con.close()

    def _add(self):
        sym = self.sym.get().strip().upper()
        if not sym:
            return
        con = None
        try:
            con = self._db()
            add_watch(con, sym, self.tag.get().strip(), self.stage.get().strip(),
                      utc_now_iso(), self.snapshot_id_getter() or "", 0.0)
            con.commit()
            self.sym.set("")
            self._load()
        except Exception as e:
            if con:
                con.rollback()
            messagebox.showerror("Error", str(e)[:1200], parent=self)
        finally:
            if con:
                con.close()

    def _remove(self):
        """Remove selected rows from watchlist."""
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Watchlist", "Select items to remove first.", parent=self)
            return
        if not messagebox.askyesno(
                "Remove selected",
                "Remove " + str(len(sel)) + " selected item(s) from watchlist?",
                parent=self):
            return
        con = None
        try:
            con = self._db()
            for item in sel:
                wid = int(self.tree.item(item, "values")[0])
                remove_watch(con, wid)
            con.commit()
            self._load()
        except Exception as e:
            if con:
                con.rollback()
            messagebox.showerror("Error", str(e)[:1200], parent=self)
        finally:
            if con:
                con.close()

    def _remove_all(self):
        """Remove ALL items from watchlist after double confirmation."""
        n = len(self.tree.get_children(""))
        if n == 0:
            messagebox.showinfo("Watchlist", "Watchlist is already empty.", parent=self)
            return
        if not messagebox.askyesno(
                "Remove ALL",
                "This will remove ALL " + str(n) + " items from the watchlist.\n\nAre you sure?",
                parent=self):
            return
        # Second confirmation for safety
        if not messagebox.askyesno(
                "Remove ALL -- confirm",
                "FINAL CONFIRMATION: delete all " + str(n) + " watchlist items?",
                parent=self):
            return
        con = None
        try:
            con = self._db()
            rows = con.execute("SELECT watch_id FROM watchlist;").fetchall()
            for (wid,) in rows:
                remove_watch(con, wid)
            con.commit()
            self._load()
            messagebox.showinfo("Watchlist", "Removed all " + str(len(rows)) + " items.", parent=self)
        except Exception as e:
            if con:
                con.rollback()
            messagebox.showerror("Error", str(e)[:1200], parent=self)
        finally:
            if con:
                con.close()

    def _remove_by_score(self):
        """Remove items where last_score < threshold (or all with no score)."""
        # Dialog: score threshold input
        win = tk.Toplevel(self)
        win.title("Remove by score")
        win.geometry("340x220")
        win.grab_set()
        ttk.Label(win, text="Remove all watchlist items with score below:").pack(pady=(16, 4), padx=16)
        score_var = tk.DoubleVar(value=40.0)
        ttk.Spinbox(win, from_=0, to=100, increment=5.0,
                    textvariable=score_var, width=8).pack(pady=4)
        include_no_score = tk.BooleanVar(value=True)
        ttk.Checkbutton(win, text="Also remove items with NO score (never refreshed)",
                        variable=include_no_score).pack(pady=4, padx=16, anchor="w")
        info_var = tk.StringVar(value="")
        ttk.Label(win, textvariable=info_var, foreground="navy").pack(pady=2)

        def _preview():
            try:
                thr = float(score_var.get())
                con = self._db()
                rows = con.execute(
                    "SELECT w.watch_id, w.unified_symbol, wm.last_score "
                    "FROM watchlist w "
                    "LEFT JOIN watchlist_metrics wm ON wm.watch_id=w.watch_id;"
                ).fetchall()
                con.close()
                hits = [r for r in rows if
                        (r[2] is not None and r[2] < thr) or
                        (r[2] is None and include_no_score.get())]
                info_var.set("Will remove " + str(len(hits)) + " of " + str(len(rows)) + " items")
            except Exception as exc:
                info_var.set("Error: " + str(exc)[:80])
        score_var.trace_add("write", lambda *_: _preview())
        include_no_score.trace_add("write", lambda *_: _preview())
        _preview()

        def _execute():
            try:
                thr = float(score_var.get())
                con = self._db()
                rows = con.execute(
                    "SELECT w.watch_id, wm.last_score "
                    "FROM watchlist w "
                    "LEFT JOIN watchlist_metrics wm ON wm.watch_id=w.watch_id;"
                ).fetchall()
                to_remove = [r[0] for r in rows if
                             (r[1] is not None and r[1] < thr) or
                             (r[1] is None and include_no_score.get())]
                if not to_remove:
                    messagebox.showinfo("Remove by score", "No items match the criteria.", parent=win)
                    return
                if not messagebox.askyesno(
                        "Confirm",
                        "Remove " + str(len(to_remove)) + " items with score < " + str(thr) + "?",
                        parent=win):
                    return
                for wid in to_remove:
                    remove_watch(con, wid)
                con.commit()
                con.close()
                win.destroy()
                self._load()
                messagebox.showinfo("Watchlist", "Removed " + str(len(to_remove)) + " items.", parent=self)
            except Exception as e:
                messagebox.showerror("Error", str(e)[:1200], parent=win)

        bf = ttk.Frame(win)
        bf.pack(fill="x", pady=8, padx=16)
        ttk.Button(bf, text="Remove",  command=_execute).pack(side="left")
        ttk.Button(bf, text="Cancel",  command=win.destroy).pack(side="right")

    def _remove_by_stage(self):
        """Remove all items matching a specific stage."""
        win = tk.Toplevel(self)
        win.title("Remove by stage")
        win.geometry("320x190")
        win.grab_set()
        ttk.Label(win, text="Remove all items in stage:").pack(pady=(16, 4), padx=16)
        # Get existing stages from tree
        stages_in_use = sorted(set(
            self.tree.item(k, "values")[3]
            for k in self.tree.get_children("")
            if self.tree.item(k, "values")
        ))
        if not stages_in_use:
            stages_in_use = ["new", "watching", "active", "exited"]
        stage_var = tk.StringVar(value=stages_in_use[0] if stages_in_use else "new")
        stage_cb = ttk.Combobox(win, textvariable=stage_var,
                                values=stages_in_use, state="normal", width=16)
        stage_cb.pack(pady=4)
        info_var = tk.StringVar(value="")
        ttk.Label(win, textvariable=info_var, foreground="navy").pack(pady=2)

        def _preview(*_):
            stage = stage_var.get().strip()
            hits = [k for k in self.tree.get_children("")
                    if self.tree.item(k, "values") and
                    self.tree.item(k, "values")[3] == stage]
            info_var.set("Will remove " + str(len(hits)) + " items in stage '" + stage + "'")
        stage_var.trace_add("write", _preview)
        _preview()

        def _execute():
            stage = stage_var.get().strip()
            if not stage:
                return
            hits = [k for k in self.tree.get_children("")
                    if self.tree.item(k, "values") and
                    self.tree.item(k, "values")[3] == stage]
            if not hits:
                messagebox.showinfo("Remove by stage", "No items in stage '" + stage + "'.", parent=win)
                return
            if not messagebox.askyesno(
                    "Confirm",
                    "Remove " + str(len(hits)) + " items in stage '" + stage + "'?",
                    parent=win):
                return
            con = None
            try:
                con = self._db()
                for iid in hits:
                    wid = int(self.tree.item(iid, "values")[0])
                    remove_watch(con, wid)
                con.commit()
                win.destroy()
                self._load()
                messagebox.showinfo("Watchlist",
                                    "Removed " + str(len(hits)) + " items.", parent=self)
            except Exception as e:
                if con:
                    con.rollback()
                messagebox.showerror("Error", str(e)[:1200], parent=win)
            finally:
                if con:
                    con.close()

        bf = ttk.Frame(win)
        bf.pack(fill="x", pady=8, padx=16)
        ttk.Button(bf, text="Remove", command=_execute).pack(side="left")
        ttk.Button(bf, text="Cancel", command=win.destroy).pack(side="right")

    def _refresh(self):
        snap = self.snapshot_id_getter()
        if not snap or snap == "-":
            messagebox.showinfo("Info", "Run pipeline first.", parent=self)
            return
        con = None
        try:
            con = self._db()
            n = refresh_watch(con, snap, "spot", utc_now_iso())
            con.commit()
            messagebox.showinfo("Watchlist", "Refreshed " + str(n) + " items.", parent=self)
            self._load()
        except Exception as e:
            if con:
                con.rollback()
            messagebox.showerror("Error", str(e)[:1200], parent=self)
        finally:
            if con:
                con.close()

    def _change_stage(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Select a watchlist item first.", parent=self)
            return
        win = tk.Toplevel(self)
        win.title("Change Stage")
        _setup_fullscreen(win)
        ttk.Label(win, text="New stage (0=detected, 1=confirmed, 2=monitored, 3=exit):").pack(padx=10, pady=8)
        stage_var = tk.StringVar(value="1")
        ttk.Combobox(win, textvariable=stage_var, values=["0","1","2","3"],
                     state="readonly", width=6).pack(padx=10)
        ttk.Label(win, text="Note (optional):").pack(padx=10, pady=(8,2))
        note_var = tk.StringVar()
        ttk.Entry(win, textvariable=note_var, width=30).pack(padx=10)
        def _apply():
            con2 = None
            try:
                con2 = self._db()
                for item in sel:
                    wid = int(self.tree.item(item, "values")[0])
                    con2.execute("UPDATE watchlist SET stage=? WHERE watch_id=?;",
                                 (stage_var.get(), wid))
                con2.commit()
                win.destroy()
                self._load()
            except Exception as e:
                if con2: con2.rollback()
                messagebox.showerror("Error", str(e)[:800], parent=win)
            finally:
                if con2: con2.close()
        ttk.Button(win, text="Apply", command=_apply).pack(pady=8)

    def _exit_position(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Select a watchlist item first.", parent=self)
            return
        win = tk.Toplevel(self)
        win.title("Exit Position")
        _setup_fullscreen(win)
        ttk.Label(win, text="Exit reason:").pack(padx=10, pady=8)
        reason_var = tk.StringVar(value="manual_exit")
        ttk.Entry(win, textvariable=reason_var, width=30).pack(padx=10)
        def _apply():
            con2 = None
            try:
                con2 = self._db()
                for item in sel:
                    wid = int(self.tree.item(item, "values")[0])
                    con2.execute(
                        "UPDATE watchlist SET stage='3', exit_timestamp_utc=?,"                        " exit_reason=? WHERE watch_id=?;",
                        (utc_now_iso(), reason_var.get(), wid))
                con2.commit()
                win.destroy()
                self._load()
            except Exception as e:
                if con2: con2.rollback()
                messagebox.showerror("Error", str(e)[:800], parent=win)
            finally:
                if con2: con2.close()
        ttk.Button(win, text="Exit", command=_apply).pack(pady=8)

    def _alerts(self):
        con = None
        try:
            con = self._db()
            rows = list_alerts(con, 200)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:1200], parent=self)
            return
        finally:
            if con:
                con.close()
        win = tk.Toplevel(self)
        win.title("Watchlist Alerts")
        _setup_fullscreen(win)
        cols = ("alert_id", "created_utc", "severity", "symbol", "message")
        _apply_tree_font(win, "Al.Treeview", 4)
        tree = ttk.Treeview(win, columns=cols, show="headings", style="Al.Treeview")
        def _asort(col, desc, t=tree):
            def _key(k):
                v = t.set(k, col)
                try:
                    return (1, float(str(v).replace(",","")))
                except Exception:
                    return (0, str(v).lower()) if v else (2, "")
            items = sorted(t.get_children(""), key=_key, reverse=desc)
            for i, k in enumerate(items):
                t.move(k, "", i)
            t.heading(col, command=lambda _c=col: _asort(_c, not desc))
        for c in cols:
            tree.heading(c, text=c, command=lambda _c=c: _asort(_c, False))
            tree.column(c, width=150, anchor="w")
        tree.column("alert_id", width=65)
        tree.pack(fill="both", expand=True, padx=6, pady=6)
        bfa = ttk.Frame(win)
        bfa.pack(fill="x", padx=4, pady=4)
        ttk.Button(bfa, text="Close", command=win.destroy).pack(side="right")
        for r in rows:
            tree.insert("", "end", values=r)

    def _show_enriched(self):
        """v7.3a: Show enriched watchlist with trade plans, predictions, P&L."""
        con = None
        try:
            con = self._db()
            enriched = enrich_watchlist_with_plans(con)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:500], parent=self)
            return
        finally:
            if con:
                con.close()
        win = tk.Toplevel(self)
        win.title("Enriched Watchlist  " + APP_VERSION)
        _setup_fullscreen(win)
        ttk.Button(win, text="Close", command=win.destroy).pack(side="top", anchor="e", padx=8, pady=4)
        cols = ("symbol", "stage", "signal", "conf", "direction", "entry", "stop", "current", "pnl%", "alerts")
        tree = ttk.Treeview(win, columns=cols, show="headings", height=20)
        for c, w in zip(cols, (70, 50, 90, 50, 50, 80, 80, 80, 60, 50)):
            tree.heading(c, text=c)
            tree.column(c, width=w, anchor="center")
        tree.pack(fill="both", expand=True, padx=6, pady=4)
        for item in enriched:
            pred = item.get("prediction") or {}
            plan = item.get("plan") or {}
            tree.insert("", "end", values=(
                item["symbol"], item.get("stage", ""),
                pred.get("signal", "-"), _fmt(pred.get("confidence"), 2),
                plan.get("direction", "-"),
                _fmt(plan.get("entry_low"), 2),
                _fmt(plan.get("stop_loss"), 2),
                _fmt(item.get("current_price"), 2),
                _fmt_signed(item.get("pnl_pct"), 1),
                str(item.get("active_alerts", 0)),
            ))

    def _show_history(self):
        """v7.3a: Show signal evolution for selected watchlist symbol."""
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Select a watchlist item first.", parent=self)
            return
        vals = self.tree.item(sel[0], "values")
        sym = vals[1]  # symbol column
        con = None
        try:
            con = self._db()
            history = run_history_compare(con, sym, limit=15)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:500], parent=self)
            return
        finally:
            if con:
                con.close()
        win = tk.Toplevel(self)
        win.title(f"Signal History: {sym}  " + APP_VERSION)
        _setup_fullscreen(win)
        ttk.Button(win, text="Close", command=win.destroy).pack(side="top", anchor="e", padx=8, pady=4)
        cols = ("run_id", "date", "signal", "confidence", "price", "changed")
        tree = ttk.Treeview(win, columns=cols, show="headings", height=18)
        for c, w in zip(cols, (60, 140, 90, 70, 80, 60)):
            tree.heading(c, text=c)
            tree.column(c, width=w, anchor="center")
        tree.pack(fill="both", expand=True, padx=6, pady=4)
        SIG_COLORS = {"strong_buy": "#006400", "buy": "#228B22",
                      "neutral": "#666", "sell": "#CC3300", "strong_sell": "#8B0000"}
        for h in history:
            changed_mark = ">>>" if h.get("signal_changed") else ""
            iid = tree.insert("", "end", values=(
                h["run_id"], (h.get("created_utc") or "")[:19],
                h["signal"], _fmt(h.get("confidence"), 3),
                _fmt(h.get("price"), 2), changed_mark,
            ))
            try:
                sig = h["signal"]
                tree.tag_configure(sig, foreground=SIG_COLORS.get(sig, "#000"))
                tree.item(iid, tags=(sig,))
            except Exception:
                pass


# LogViewerWindow -- Screen E: state_log + run list
class LogViewerWindow(tk.Toplevel):
    def __init__(self, master, paths):
        super(LogViewerWindow, self).__init__(master)
        self.title("Log Viewer")
        _setup_fullscreen(self)
        self.paths = paths
        self._build()
        self._load_runs()


    def _build(self):
        top = ttk.Frame(self, padding=6)
        top.pack(fill="x")
        ttk.Label(top, text="Severity filter:").pack(side="left")
        self.sev = tk.StringVar(value="all")
        for v in ("all", "info", "warning", "error"):
            ttk.Radiobutton(top, text=v, variable=self.sev, value=v,
                command=self._load_logs).pack(side="left", padx=2)
        ttk.Button(top, text="Close", command=self.destroy).pack(side="right", padx=2)
        ttk.Button(top, text="Refresh", command=self._load_runs).pack(side="left", padx=8)

        pane = ttk.PanedWindow(self, orient="horizontal")
        pane.pack(fill="both", expand=True, padx=6, pady=6)

        # Left: run list
        lf = ttk.Frame(pane, width=220)
        pane.add(lf, weight=1)
        ttk.Label(lf, text="Runs").pack(anchor="w")
        rcols = ("run_id", "created_utc", "status")
        _apply_tree_font(lf, "Rl.Treeview", 4)
        self.run_tree = ttk.Treeview(lf, columns=rcols, show="headings",
                                     style="Rl.Treeview", selectmode="browse")
        for c in rcols:
            self.run_tree.heading(c, text=c)
        self.run_tree.column("run_id",      width=55, anchor="e")
        self.run_tree.column("created_utc", width=140, anchor="w")
        self.run_tree.column("status",      width=90, anchor="w")
        rs = ttk.Scrollbar(lf, orient="vertical", command=self.run_tree.yview)
        self.run_tree.configure(yscrollcommand=rs.set)
        rs.pack(side="right", fill="y", padx=(0, 3))
        self.run_tree.pack(fill="both", expand=True)
        self.run_tree.bind("<<TreeviewSelect>>", lambda _e: self._load_logs())

        # Right: state_log
        rf = ttk.Frame(pane)
        pane.add(rf, weight=3)
        ttk.Label(rf, text="State log").pack(anchor="w")
        lcols = ("event_id", "timestamp_utc", "from_status", "to_status", "severity", "message")
        _apply_tree_font(rf, "Lg.Treeview", 4)
        self.log_tree = ttk.Treeview(rf, columns=lcols, show="headings",
                                     style="Lg.Treeview")
        for c in lcols:
            self.log_tree.heading(c, text=c, command=lambda _c=c: self._sort(_c, False))
        self.log_tree.column("event_id",     width=55,  anchor="e")
        self.log_tree.column("timestamp_utc",width=140, anchor="w")
        self.log_tree.column("from_status",  width=100, anchor="w")
        self.log_tree.column("to_status",    width=100, anchor="w")
        self.log_tree.column("severity",     width=70,  anchor="w")
        self.log_tree.column("message",      width=300, anchor="w")
        self.log_tree.tag_configure("error",   background="#f8d7da")
        self.log_tree.tag_configure("warning", background="#fff3cd")
        ls = ttk.Scrollbar(rf, orient="vertical", command=self.log_tree.yview)
        self.log_tree.configure(yscrollcommand=ls.set)
        ls.pack(side="right", fill="y", padx=(0, 3))
        self.log_tree.pack(fill="both", expand=True)

    def _sort(self, col, desc):
        def _key(k):
            v = self.log_tree.set(k, col)
            try:
                return (1, float(str(v).replace(",","")))
            except Exception:
                return (0, str(v).lower()) if v else (2, "")
        items = sorted(self.log_tree.get_children(""), key=_key, reverse=desc)
        for i, k in enumerate(items):
            self.log_tree.move(k, "", i)
        self.log_tree.heading(col, command=lambda _c=col: self._sort(_c, not desc))

    def _load_runs(self):
        for i in self.run_tree.get_children():
            self.run_tree.delete(i)
        con = None
        try:
            con = db_connect(self.paths.db_path)
            rows = con.execute(
                "SELECT run_id, created_utc, status FROM runs ORDER BY run_id DESC LIMIT 200;"
            ).fetchall()
            for r in rows:
                self.run_tree.insert("", "end", iid=str(r[0]), values=r)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:800], parent=self)
        finally:
            if con:
                con.close()
        self._load_logs()

    def _load_logs(self):
        for i in self.log_tree.get_children():
            self.log_tree.delete(i)
        sel = self.run_tree.selection()
        run_id = int(self.run_tree.item(sel[0], "values")[0]) if sel else None
        sev = self.sev.get()
        con = None
        try:
            con = db_connect(self.paths.db_path)
            q = "SELECT rowid AS event_id, timestamp_utc, from_status, to_status, severity, message FROM state_log"
            params = []
            clauses = []
            if run_id is not None:
                clauses.append("run_id=?")
                params.append(run_id)
            if sev != "all":
                clauses.append("severity=?")
                params.append(sev)
            if clauses:
                q += " WHERE " + " AND ".join(clauses)
            q += " ORDER BY event_id DESC LIMIT 500;"
            rows = con.execute(q, params).fetchall()
            for r in rows:
                tag = r[4] if r[4] in ("error", "warning") else ""
                self.log_tree.insert("", "end", values=r, tags=(tag,) if tag else ())
        except Exception as e:
            messagebox.showerror("Error", str(e)[:800], parent=self)
        finally:
            if con:
                con.close()


# PromotionWindow -- shows expanders and allows watchlist promotion
class PromotionWindow(tk.Toplevel):
    def __init__(self, master, paths, selection_id, snapshot_id):
        super(PromotionWindow, self).__init__(master)
        self.title("Expander Promotion  sel=" + str(selection_id))
        _setup_fullscreen(self)
        self.paths = paths
        self.selection_id = int(selection_id)
        self.snapshot_id = snapshot_id
        self._build()
        self._load()


    def _build(self):
        top = ttk.Frame(self, padding=6)
        top.pack(fill="x")
        ttk.Label(top, text="Expanders (composite >= ").pack(side="left")
        self._thr_var = tk.DoubleVar(value=EXPANDER_THRESHOLD)
        ttk.Spinbox(top, from_=0, to=100, increment=1.0, textvariable=self._thr_var,
                    width=6).pack(side="left")
        ttk.Label(top, text=")").pack(side="left")
        ttk.Button(top, text="Close", command=self.destroy).pack(side="right", padx=2)
        ttk.Button(top, text="Refresh", command=self._load).pack(side="left", padx=4)
        ttk.Button(top, text="Promote selected to Watchlist", command=self._promote).pack(side="left", padx=4)
        ttk.Button(top, text="Auto-promote ALL expanders", command=self._auto_promote).pack(side="left", padx=4)
        cols = ("symbol", "composite", "rank", "vol24", "chg24", "in_watchlist")
        _apply_tree_font(self, "Pr.Treeview", 4)
        self.tree = ttk.Treeview(self, columns=cols, show="headings", style="Pr.Treeview")
        for c in cols:
            self.tree.heading(c, text=c, command=lambda _c=c: self._sort(_c, False))
        self.tree.column("symbol",      width=80,  anchor="w")
        self.tree.column("composite",   width=80,  anchor="e")
        self.tree.column("rank",        width=60,  anchor="e")
        self.tree.column("vol24",       width=110, anchor="e")
        self.tree.column("chg24",       width=70,  anchor="e")
        self.tree.column("in_watchlist",width=80,  anchor="center")
        self.tree.tag_configure("exp", background="#d4edda")
        self.tree.tag_configure("wl",  background="#cce5ff")
        self.tree.pack(fill="both", expand=True, padx=6, pady=6)


    def _sort(self, col, desc):
        def _key(k):
            v = self.tree.set(k, col)
            try:
                return (1, float(str(v).replace(",","")))
            except Exception:
                return (0, str(v).lower()) if v and v != "-" else (2, "")
        items = sorted(self.tree.get_children(""), key=_key, reverse=desc)
        for i, k in enumerate(items):
            self.tree.move(k, "", i)
        self.tree.heading(col, command=lambda _c=col: self._sort(_c, not desc))

    def _load(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        con = None
        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            # safety: ensure exit columns exist
            try:
                con.execute("ALTER TABLE watchlist ADD COLUMN exit_timestamp_utc TEXT;")
            except Exception:
                pass
            try:
                con.execute("ALTER TABLE watchlist ADD COLUMN exit_reason TEXT;")
            except Exception:
                pass
            # get watchlist symbols
            wl_syms = set(r[0] for r in con.execute(
                "SELECT unified_symbol FROM watchlist WHERE exit_timestamp_utc IS NULL;"
            ).fetchall())
            # Resolve snapshot_id from selection if not set
            snap_for_join = self.snapshot_id
            if not snap_for_join:
                row0 = con.execute(
                    "SELECT snapshot_id FROM topnow_selection WHERE selection_id=?;",
                    (self.selection_id,)
                ).fetchone()
                snap_for_join = row0[0] if row0 else ""
            thr = self._thr_var.get() if hasattr(self, "_thr_var") else EXPANDER_THRESHOLD
            rows = con.execute(
                "SELECT i.unified_symbol, i.composite_preview,"
                " m.rank, m.vol24, m.change_24h_pct"
                " FROM topnow_selection_items i"
                " LEFT JOIN market_snapshots m"
                "  ON m.snapshot_id=? AND m.timeframe='spot'"
                "  AND m.unified_symbol=i.unified_symbol"
                " WHERE i.selection_id=?"
                "  AND (i.composite_preview >= ? OR i.composite_preview IS NULL)"
                " ORDER BY i.composite_preview DESC NULLS LAST;",
                (snap_for_join, self.selection_id, thr)
            ).fetchall()
            for sym, comp, rank, vol24, chg24 in rows:
                in_wl = "YES" if sym in wl_syms else "-"
                tag = "wl" if sym in wl_syms else "exp"
                try:
                    self.tree.insert("", "end", iid=sym, values=(
                        sym, _fmt(comp, 1), rank or "-",
                        _fmt(vol24, 0), _fmt(chg24, 2), in_wl
                    ), tags=(tag,))
                except Exception:
                    # iid already exists - update values instead
                    self.tree.item(sym, values=(
                        sym, _fmt(comp, 1), rank or "-",
                        _fmt(vol24, 0), _fmt(chg24, 2), in_wl
                    ), tags=(tag,))
        except Exception as e:
            messagebox.showerror("Error", str(e)[:800], parent=self)
        finally:
            if con:
                con.close()

    def _promote_symbols(self, symbols):
        if not symbols:
            return 0
        con = None
        promoted = 0
        try:
            con = db_connect(self.paths.db_path)
            existing = set(r[0] for r in con.execute(
                "SELECT unified_symbol FROM watchlist WHERE exit_timestamp_utc IS NULL;"
            ).fetchall())
            for sym in symbols:
                if sym in existing:
                    continue
                comp = self.tree.set(sym, "composite")
                try:
                    score = float(comp)
                except Exception:
                    score = 0.0
                con.execute(
                    "INSERT OR IGNORE INTO watchlist (unified_symbol, tag, stage, tracking_since_utc,"
                    " entry_snapshot_id, entry_score) VALUES (?,?,?,?,?,?);",
                    (sym, "expander", "0", utc_now_iso(), self.snapshot_id, score)
                )
                promoted += 1
            con.commit()
        except Exception as e:
            if con:
                con.rollback()
            messagebox.showerror("Error", str(e)[:800], parent=self)
        finally:
            if con:
                con.close()
        return promoted

    def _promote(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Select symbols first.", parent=self)
            return
        n = self._promote_symbols(list(sel))
        messagebox.showinfo("Promotion", "Promoted " + str(n) + " new symbols to Watchlist.", parent=self)
        self._load()

    def _auto_promote(self):
        symbols = list(self.tree.get_children(""))
        n = self._promote_symbols(symbols)
        messagebox.showinfo("Auto-promote", "Promoted " + str(n) + " new expanders to Watchlist.", parent=self)
        self._load()


# ===========================================================================
# PREDICTIONS VIEWER (v7.1a)
# ===========================================================================

class PredictionsWindow(tk.Toplevel):
    """Display prediction signals with confidence and reasoning per symbol."""

    def __init__(self, master, paths, selection_id, run_id_getter):
        super().__init__(master)
        self.title("Predictions  sel=" + str(selection_id) + "  " + APP_VERSION)
        _setup_fullscreen(self)
        self.paths = paths
        self.selection_id = int(selection_id)
        self.run_id_getter = run_id_getter
        self._build()
        self._load()

    def _build(self):
        tb = ttk.Frame(self, padding=4)
        tb.pack(fill="x", side="top")
        ttk.Button(tb, text="Close", command=self.destroy).pack(side="right")
        ttk.Button(tb, text="Refresh", command=self._load).pack(side="right", padx=4)
        ttk.Label(tb, text="Prediction Signals", font=("TkDefaultFont", 5, "bold")).pack(side="left")

        # Signal legend
        legend = ttk.Frame(self, padding=4)
        legend.pack(fill="x")
        for sig, clr in [("strong_buy", "#006400"), ("buy", "#228B22"),
                         ("neutral", "#666"), ("sell", "#CC3300"), ("strong_sell", "#8B0000")]:
            ttk.Label(legend, text=sig, foreground=clr).pack(side="left", padx=6)

        cols = ("symbol", "signal", "confidence", "structural_avg", "rule")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=20)
        for c, w in zip(cols, (80, 100, 80, 100, 200)):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=w, anchor="center" if c != "rule" else "w")
        self.tree.pack(fill="both", expand=True, padx=6, pady=4)

        # Reasoning detail at bottom
        self._detail_var = tk.StringVar(value="Select a row to see reasoning detail.")
        ttk.Label(self, textvariable=self._detail_var, wraplength=600).pack(fill="x", padx=6, pady=4)
        self.tree.bind("<<TreeviewSelect>>", self._on_select)

    def _load(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        try:
            run_id = int(self.run_id_getter())
        except Exception:
            return
        con = db_connect(self.paths.db_path)
        preds = load_predictions(con, run_id, self.selection_id)
        con.close()
        # Color mapping for signal tags
        SIG_COLORS = {"strong_buy": "#006400", "buy": "#228B22",
                      "neutral": "#666666", "sell": "#CC3300", "strong_sell": "#8B0000"}
        for p in preds:
            sig = p["signal"]
            iid = self.tree.insert("", "end", values=(
                p["symbol"], sig,
                _fmt(p["confidence"], 3),
                _fmt(p.get("structural_avg"), 1),
                p.get("rule", ""),
            ))
            try:
                self.tree.tag_configure(sig, foreground=SIG_COLORS.get(sig, "#000"))
                self.tree.item(iid, tags=(sig,))
            except Exception:
                pass

    def _on_select(self, _event):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        sym = vals[0]
        try:
            run_id = int(self.run_id_getter())
            con = db_connect(self.paths.db_path)
            preds = load_predictions(con, run_id, self.selection_id)
            con.close()
            for p in preds:
                if p["symbol"] == sym:
                    import json
                    self._detail_var.set(
                        f"{sym}: {p['signal']} (conf={p['confidence']}) -- "
                        f"structural={p['reasoning'].get('structural_avg')} "
                        f"supplemental={p['reasoning'].get('supplemental_avg')} "
                        f"rule={p['reasoning'].get('rule')}")
                    break
        except Exception:
            pass


# ===========================================================================
# TRADE PLAN VIEWER (v7.1a)
# ===========================================================================

class TradePlanWindow(tk.Toplevel):
    """Display trade plans with entry/exit zones, stops, targets, and position sizing."""

    def __init__(self, master, paths, selection_id, run_id_getter):
        super().__init__(master)
        self.title("Trade Plans  sel=" + str(selection_id) + "  " + APP_VERSION)
        _setup_fullscreen(self)
        self.paths = paths
        self.selection_id = int(selection_id)
        self.run_id_getter = run_id_getter
        self._build()
        self._load()

    def _build(self):
        tb = ttk.Frame(self, padding=4)
        tb.pack(fill="x", side="top")
        ttk.Button(tb, text="Close", command=self.destroy).pack(side="right")
        ttk.Button(tb, text="Refresh", command=self._load).pack(side="right", padx=4)
        ttk.Label(tb, text="Trade Plans", font=("TkDefaultFont", 5, "bold")).pack(side="left")

        cols = ("symbol", "dir", "entry_low", "entry_high", "stop", "target1", "target2", "pos%", "risk")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=20)
        for c, w in zip(cols, (70, 50, 90, 90, 90, 90, 90, 50, 50)):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=w, anchor="center")
        self.tree.pack(fill="both", expand=True, padx=6, pady=4)

        # Summary at bottom
        self._summary_var = tk.StringVar(value="")
        ttk.Label(self, textvariable=self._summary_var, wraplength=600).pack(fill="x", padx=6, pady=4)

    def _load(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        try:
            run_id = int(self.run_id_getter())
        except Exception:
            return
        con = db_connect(self.paths.db_path)
        plans = load_trade_plans(con, run_id, self.selection_id)
        con.close()
        DIR_COLORS = {"long": "#006400", "short": "#CC3300", "hold": "#666666"}
        longs = shorts = holds = total_pos = 0
        for p in plans:
            d = p["direction"]
            iid = self.tree.insert("", "end", values=(
                p["symbol"], d,
                _fmt(p["entry_low"], 2), _fmt(p["entry_high"], 2),
                _fmt(p["stop_loss"], 2),
                _fmt(p["target_1"], 2), _fmt(p["target_2"], 2),
                _fmt(p["position_pct"], 1), _fmt(p["risk_score"], 2),
            ))
            try:
                self.tree.tag_configure(d, foreground=DIR_COLORS.get(d, "#000"))
                self.tree.item(iid, tags=(d,))
            except Exception:
                pass
            if d == "long": longs += 1
            elif d == "short": shorts += 1
            else: holds += 1
            total_pos += p["position_pct"] or 0
        self._summary_var.set(
            f"Plans: {len(plans)} total -- {longs} long, {shorts} short, {holds} hold. "
            f"Total position: {total_pos:.1f}%")


# ===========================================================================
# SUMMARY DASHBOARD (v7.1a)
# ===========================================================================

class SummaryDashboard(tk.Toplevel):
    """Post-analysis summary showing signal distribution, top picks, and warnings."""

    def __init__(self, master, paths, selection_id, run_id_getter):
        super().__init__(master)
        self.title("Analysis Summary  " + APP_VERSION)
        _setup_fullscreen(self)
        self.paths = paths
        self.selection_id = int(selection_id)
        self.run_id_getter = run_id_getter
        self._build()
        self._load()

    def _build(self):
        tb = ttk.Frame(self, padding=4)
        tb.pack(fill="x", side="top")
        ttk.Button(tb, text="Close", command=self.destroy).pack(side="right")
        ttk.Button(tb, text="Refresh", command=self._load).pack(side="right", padx=4)
        # Action buttons to open detail windows
        ttk.Button(tb, text="Predictions >", command=self._open_preds).pack(side="left", padx=4)
        ttk.Button(tb, text="Trade Plans >", command=self._open_plans).pack(side="left", padx=4)
        ttk.Label(tb, text="Summary Dashboard", font=("TkDefaultFont", 5, "bold")).pack(side="left", padx=8)

        self.txt = tk.Text(self, wrap="word", padx=12, pady=8)
        self.txt.pack(fill="both", expand=True)

    def _load(self):
        self.txt.configure(state="normal")
        self.txt.delete("1.0", "end")
        try:
            run_id = int(self.run_id_getter())
        except Exception:
            self.txt.insert("end", "No run_id available.")
            self.txt.configure(state="disabled")
            return

        con = db_connect(self.paths.db_path)
        try:
            s = run_summary(con, run_id, self.selection_id)
        except Exception as exc:
            self.txt.insert("end", f"Error: {exc}")
            con.close()
            self.txt.configure(state="disabled")
            return

        # Cross-scope correlations
        try:
            cors = cross_scope_correlation(con, run_id)
        except Exception:
            cors = []
        con.close()

        # Build summary text
        self.txt.insert("end", "=== ANALYSIS SUMMARY ===\n\n")
        self.txt.insert("end", f"Run ID: {s['run_id']}  |  Version: {s.get('app_version', '?')}\n")
        self.txt.insert("end", f"Scope: {s.get('scope', '?')}  |  Status: {s.get('status', '?')}\n")
        self.txt.insert("end", f"Candidates: {s.get('candidates', 0)}  |  "
                                f"Layers avg: {s.get('avg_layers_scored', 0)}/10\n\n")

        # Signal distribution
        self.txt.insert("end", "--- SIGNAL DISTRIBUTION ---\n")
        dist = s.get("signal_distribution", {})
        for sig in ["strong_buy", "buy", "neutral", "sell", "strong_sell"]:
            cnt = dist.get(sig, 0)
            bar = "#" * cnt
            self.txt.insert("end", f"  {sig:15s}  {cnt:2d}  {bar}\n")
        self.txt.insert("end", f"  Total predictions: {s.get('predictions_count', 0)}\n\n")

        # Top picks
        self.txt.insert("end", "--- TOP PICKS ---\n")
        for p in s.get("top_picks", []):
            self.txt.insert("end",
                f"  {p['symbol']:8s}  {p['signal']:12s}  conf={p['confidence']:.3f}  "
                f"struct={p.get('structural_avg', '?')}\n")
        if not s.get("top_picks"):
            self.txt.insert("end", "  (no buy/strong_buy signals)\n")
        self.txt.insert("end", "\n")

        # Trade plans summary
        self.txt.insert("end", "--- TRADE PLANS ---\n")
        self.txt.insert("end", f"  Total: {s.get('trade_plans_count', 0)}  |  "
                                f"Long: {s.get('long_count', 0)}  Short: {s.get('short_count', 0)}  "
                                f"Hold: {s.get('hold_count', 0)}\n")
        self.txt.insert("end", f"  Total position: {s.get('total_position_pct', 0):.1f}%\n\n")

        # Cross-scope correlations
        if cors and not (len(cors) == 1 and "note" in cors[0]):
            self.txt.insert("end", "--- CROSS-SCOPE CORRELATIONS ---\n")
            for c in cors:
                aligned = "ALIGNED" if c.get("currently_aligned") else "DIVERGENT"
                self.txt.insert("end",
                    f"  BTC ({c.get('btc_change', '?'):+.1f}%) vs "
                    f"{c['reference']} ({c.get('ref_change', '?'):+.1f}%)  "
                    f"expected: {c['expected_correlation']}  -> {aligned}\n")
            self.txt.insert("end", "\n")

        # Warnings
        if s.get("warnings"):
            self.txt.insert("end", "--- WARNINGS ---\n")
            for w in s["warnings"]:
                self.txt.insert("end", f"  [!] {w}\n")

        self.txt.configure(state="disabled")

    def _open_preds(self):
        PredictionsWindow(self.master, self.paths, self.selection_id, self.run_id_getter)

    def _open_plans(self):
        TradePlanWindow(self.master, self.paths, self.selection_id, self.run_id_getter)


# ===========================================================================
# ALERTS VIEWER (v7.2a)
# ===========================================================================

class AlertsWindow(tk.Toplevel):
    """Display trade plan alerts with acknowledge capability."""

    def __init__(self, master, paths, run_id_getter):
        super().__init__(master)
        self.title("Alerts  " + APP_VERSION)
        _setup_fullscreen(self)
        self.paths = paths
        self.run_id_getter = run_id_getter
        self._build()
        self._load()

    def _build(self):
        tb = ttk.Frame(self, padding=4)
        tb.pack(fill="x", side="top")
        ttk.Button(tb, text="Close", command=self.destroy).pack(side="right")
        ttk.Button(tb, text="Refresh + Check", command=self._check_and_load).pack(side="right", padx=4)
        ttk.Button(tb, text="Ack Selected", command=self._ack_selected).pack(side="right", padx=4)
        ttk.Label(tb, text="Trade Plan Alerts", font=("TkDefaultFont", 5, "bold")).pack(side="left")

        self._count_var = tk.StringVar(value="")
        ttk.Label(self, textvariable=self._count_var).pack(fill="x", padx=6)

        cols = ("id", "symbol", "type", "severity", "message", "price", "time")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=18)
        for c, w in zip(cols, (40, 70, 100, 60, 250, 80, 140)):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=w, anchor="center" if c != "message" else "w")
        self.tree.pack(fill="both", expand=True, padx=6, pady=4)

    def _load(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        try:
            con = db_connect(self.paths.db_path)
            alerts = load_alerts(con, unacknowledged_only=False, limit=100)
            con.close()
        except Exception:
            return
        SEV_COLORS = {"critical": "#8B0000", "success": "#006400", "warning": "#CC6600", "info": "#333"}
        unack = 0
        for a in alerts:
            if not a["acknowledged"]:
                unack += 1
            iid = self.tree.insert("", "end", values=(
                a["id"], a["symbol"], a["type"], a["severity"],
                a["message"], _fmt(a["price"], 2), a["created_utc"][:19],
            ))
            try:
                sev = a["severity"]
                self.tree.tag_configure(sev, foreground=SEV_COLORS.get(sev, "#000"))
                self.tree.item(iid, tags=(sev,))
            except Exception:
                pass
        self._count_var.set(f"Total: {len(alerts)}  Unacknowledged: {unack}")

    def _check_and_load(self):
        """Run alert check for current run, then reload."""
        try:
            run_id = int(self.run_id_getter())
            con = db_connect(self.paths.db_path)
            new = check_trade_plan_alerts(con, run_id)
            con.close()
            if new:
                messagebox.showinfo("New Alerts", f"{len(new)} new alerts triggered.", parent=self)
        except Exception:
            pass
        self._load()

    def _ack_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        try:
            con = db_connect(self.paths.db_path)
            for iid in sel:
                vals = self.tree.item(iid, "values")
                alert_id = int(vals[0])
                acknowledge_alert(con, alert_id)
            con.close()
        except Exception:
            pass
        self._load()


# ===========================================================================
# PREDICTION PERFORMANCE VIEWER (v7.2a)
# ===========================================================================

class PredictionPerformanceWindow(tk.Toplevel):
    """Show historical prediction hit rate and per-symbol outcomes."""

    def __init__(self, master, paths):
        super().__init__(master)
        self.title("Prediction Performance  " + APP_VERSION)
        _setup_fullscreen(self)
        self.paths = paths
        self._build()
        self._load()

    def _build(self):
        tb = ttk.Frame(self, padding=4)
        tb.pack(fill="x", side="top")
        ttk.Button(tb, text="Close", command=self.destroy).pack(side="right")
        ttk.Button(tb, text="Evaluate Now", command=self._evaluate).pack(side="right", padx=4)
        ttk.Label(tb, text="Prediction Performance", font=("TkDefaultFont", 5, "bold")).pack(side="left")

        self._summary_var = tk.StringVar(value="")
        ttk.Label(self, textvariable=self._summary_var,
                  font=("TkDefaultFont", 5, "bold")).pack(fill="x", padx=6, pady=4)

        cols = ("symbol", "signal", "conf", "price_pred", "price_now", "pnl%", "outcome")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=18)
        for c, w in zip(cols, (70, 90, 60, 80, 80, 60, 70)):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=w, anchor="center")
        self.tree.pack(fill="both", expand=True, padx=6, pady=4)

    def _load(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        try:
            con = db_connect(self.paths.db_path)
            perfs = load_prediction_performance(con, limit=100)
            con.close()
        except Exception:
            return
        correct = sum(1 for p in perfs if p["outcome"] == "correct")
        incorrect = sum(1 for p in perfs if p["outcome"] == "incorrect")
        total = correct + incorrect
        rate = round(correct / total * 100, 1) if total > 0 else 0
        self._summary_var.set(
            f"Hit rate: {rate}%  ({correct}/{total})  |  Total evaluated: {len(perfs)}")

        OUT_COLORS = {"correct": "#006400", "incorrect": "#CC3300", "unknown": "#666"}
        for p in perfs:
            iid = self.tree.insert("", "end", values=(
                p["symbol"], p["signal"], _fmt(p["confidence"], 2),
                _fmt(p["price_at_pred"], 2), _fmt(p["price_at_eval"], 2),
                _fmt_signed(p["pnl_pct"], 1), p["outcome"],
            ))
            try:
                out = p["outcome"]
                self.tree.tag_configure(out, foreground=OUT_COLORS.get(out, "#000"))
                self.tree.item(iid, tags=(out,))
            except Exception:
                pass

    def _evaluate(self):
        try:
            con = db_connect(self.paths.db_path)
            summary = evaluate_prediction_history(con)
            con.close()
            messagebox.showinfo("Evaluation",
                f"Evaluated: {summary['evaluated']}\n"
                f"Hit rate: {summary['hit_rate_pct']}%\n"
                f"Outcomes: {summary['outcomes']}",
                parent=self)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:500], parent=self)
        self._load()


# ===========================================================================
# PORTFOLIO WINDOW (v7.5a)
# ===========================================================================

class PortfolioWindow(tk.Toplevel):
    """Portfolio management: open/close positions, risk metrics, P&L tracking."""

    def __init__(self, master, paths, run_id_getter):
        super().__init__(master)
        self.title("Portfolio  " + APP_VERSION)
        _setup_fullscreen(self)
        self.paths = paths
        self.run_id_getter = run_id_getter
        self._build()
        self._load()

    def _build(self):
        tb = ttk.Frame(self, padding=4)
        tb.pack(fill="x", side="top")
        ttk.Button(tb, text="Close", command=self.destroy).pack(side="right")
        ttk.Button(tb, text="Refresh", command=self._load).pack(side="right", padx=4)
        ttk.Button(tb, text="Close Selected", command=self._close_position).pack(side="right", padx=4)
        ttk.Button(tb, text="Open from Plan", command=self._open_from_plan).pack(side="left", padx=4)
        ttk.Label(tb, text="Portfolio Manager", font=("TkDefaultFont", 5, "bold")).pack(side="left")

        # Risk summary bar
        self._risk_var = tk.StringVar(value="")
        risk_bar = ttk.Frame(self, padding=4)
        risk_bar.pack(fill="x")
        ttk.Label(risk_bar, textvariable=self._risk_var, wraplength=700).pack(fill="x")
        ttk.Separator(self).pack(fill="x")

        cols = ("id", "symbol", "dir", "entry", "size%", "stop", "target",
                "current", "pnl%", "status", "scope", "opened")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=16)
        for c, w in zip(cols, (40, 70, 45, 80, 45, 80, 80, 80, 55, 60, 80, 100)):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=w, anchor="center")
        self.tree.pack(fill="both", expand=True, padx=6, pady=4)

        # Closed positions section
        ttk.Label(self, text="Recent Closed:").pack(anchor="w", padx=6)
        self._closed_var = tk.StringVar(value="")
        ttk.Label(self, textvariable=self._closed_var, wraplength=700).pack(fill="x", padx=6, pady=4)

    def _load(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            dash = portfolio_dashboard(con)
            risk = dash.get("risk_metrics", {})
            con.close()
        except Exception as e:
            self._risk_var.set(f"Error: {e}")
            return

        # Risk bar
        r = risk
        warnings_str = " | ".join(r.get("warnings", [])) if r.get("warnings") else "No warnings"
        self._risk_var.set(
            f"Open: {r.get('positions_open', 0)}  "
            f"Exposure: {r.get('total_exposure_pct', 0):.1f}%  "
            f"Heat: {r.get('portfolio_heat', 0):.1f}%  "
            f"At risk: {r.get('positions_at_risk', 0)}  "
            f"Win rate: {r.get('closed_win_rate', 0):.0f}%  "
            f"| {warnings_str}")

        # Open positions
        PNL_COLORS = {"positive": "#006400", "negative": "#CC3300", "zero": "#666"}
        for p in dash.get("open_positions", []):
            pnl = p.get("unrealised_pnl")
            pnl_tag = "positive" if pnl and pnl > 0 else ("negative" if pnl and pnl < 0 else "zero")
            iid = self.tree.insert("", "end", values=(
                p["id"], p["symbol"], p["direction"],
                _fmt(p["entry_price"], 2), _fmt(p["size"], 1),
                _fmt(p["stop_loss"], 2), _fmt(p["target"], 2),
                _fmt(p.get("current_price"), 2), _fmt_signed(pnl, 1),
                p["status"], p.get("scope", ""), (p.get("opened") or "")[:10],
            ))
            try:
                self.tree.tag_configure(pnl_tag, foreground=PNL_COLORS.get(pnl_tag, "#000"))
                self.tree.item(iid, tags=(pnl_tag,))
            except Exception:
                pass

        # Closed summary
        closed = dash.get("recent_closed", [])
        total_pnl = dash.get("total_unrealised_pnl", 0)
        if closed:
            closed_str = " | ".join(
                f"{c['symbol']} {c.get('pnl_pct', 0):+.1f}%" for c in closed[:8])
            self._closed_var.set(f"Unrealised P&L: {total_pnl:+.1f}%  |  Last closed: {closed_str}")
        else:
            self._closed_var.set(f"Unrealised P&L: {total_pnl:+.1f}%  |  No closed positions yet")

    def _open_from_plan(self):
        """Open position from latest trade plan for a symbol."""
        import tkinter.simpledialog; sym = tkinter.simpledialog.askstring("Open Position", "Symbol:", parent=self)
        if not sym:
            return
        sym = sym.strip().upper()
        try:
            rid = int(self.run_id_getter())
            con = db_connect(self.paths.db_path)
            pos_id = open_position_from_trade_plan(con, rid, sym)
            con.close()
            if pos_id:
                messagebox.showinfo("Position Opened",
                    f"Opened position #{pos_id} for {sym} from trade plan.", parent=self)
            else:
                messagebox.showwarning("No Plan",
                    f"No trade plan found for {sym} (or plan says 'hold').", parent=self)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:500], parent=self)
        self._load()

    def _close_position(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Select a position to close.", parent=self)
            return
        vals = self.tree.item(sel[0], "values")
        pos_id = int(vals[0])
        current = vals[7]  # current price column
        try:
            exit_price = float(current)
        except (ValueError, TypeError):
            import tkinter.simpledialog; exit_price_str = tkinter.simpledialog.askstring("Exit Price", "Enter exit price:", parent=self)
            if not exit_price_str:
                return
            exit_price = float(exit_price_str)
        try:
            con = db_connect(self.paths.db_path)
            result = close_position(con, pos_id, exit_price)
            con.close()
            if result:
                messagebox.showinfo("Position Closed",
                    f"P&L: {result['pnl_pct']:+.2f}%", parent=self)
            else:
                messagebox.showwarning("Not Found", "Position not found or already closed.", parent=self)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:500], parent=self)
        self._load()


# BacktestWindow -- Historical signal validation
class BacktestWindow(tk.Toplevel):
    """
    Backtest: fetches 90-day historical daily data for top-N coins,
    computes SpotBasic score for each day, measures N-day forward returns,
    compares top-quartile vs bottom-quartile vs market average.
    Spec 9.3: replay test / signal validation.
    """

    DEFAULT_COINS    = 30   # coins to analyse
    DEFAULT_LOOKBACK = 90   # days of history to fetch
    DEFAULT_FORWARD  = [7, 14, 30]  # forward-return windows to measure

    def __init__(self, master, paths):
        super(BacktestWindow, self).__init__(master)
        self.title("Backtest -- Signal Validation")
        _setup_fullscreen(self)
        self.paths = paths
        self._running = False
        self._results = []        # list of row dicts for display
        self._build()

    # ------------------------------------------------------------------ UI --
    def _build(self):
        # TOP BAR
        tb = ttk.Frame(self, padding=6)
        tb.pack(fill="x", side="top")
        ttk.Button(tb, text="Close",    command=self.destroy).pack(side="right", padx=2)
        ttk.Button(tb, text="Export CSV", command=self._export_csv).pack(side="right", padx=2)
        ttk.Button(tb, text="Run Backtest", command=self._run).pack(side="left", padx=2)
        ttk.Separator(self).pack(fill="x", side="top")

        # PARAMS BAR
        pb = ttk.Frame(self, padding=6)
        pb.pack(fill="x", side="top")
        ttk.Label(pb, text="Coins:").pack(side="left")
        self._coins_var = tk.IntVar(value=self.DEFAULT_COINS)
        ttk.Spinbox(pb, from_=5, to=100, increment=5,
                    textvariable=self._coins_var, width=5).pack(side="left", padx=4)
        ttk.Label(pb, text="Lookback days:").pack(side="left", padx=(12, 0))
        self._days_var = tk.IntVar(value=self.DEFAULT_LOOKBACK)
        ttk.Spinbox(pb, from_=30, to=365, increment=30,
                    textvariable=self._days_var, width=5).pack(side="left", padx=4)
        ttk.Label(pb, text="vs_currency:").pack(side="left", padx=(12, 0))
        self._curr_var = tk.StringVar(value="usd")
        ttk.Entry(pb, textvariable=self._curr_var, width=5).pack(side="left", padx=4)
        self._status_var = tk.StringVar(value="Ready")
        ttk.Label(pb, textvariable=self._status_var, foreground="navy").pack(side="left", padx=12)
        ttk.Separator(self).pack(fill="x", side="top")

        # SUMMARY LABELS (packed from bottom)
        self._summary_var = tk.StringVar(value="")
        ttk.Label(self, textvariable=self._summary_var, foreground="darkgreen",
                  font=("TkDefaultFont", 5, "bold"),
                  wraplength=700, justify="left").pack(side="bottom", anchor="w", padx=8, pady=4)
        ttk.Separator(self).pack(fill="x", side="bottom")

        # LOG (packed from bottom)
        self.log_box = tk.Text(self, height=6, wrap="word")
        self.log_box.pack(fill="x", side="bottom", padx=6, pady=(0, 2))
        ttk.Label(self, text="Log:").pack(side="bottom", anchor="w", padx=6)
        ttk.Separator(self).pack(fill="x", side="bottom")

        # RESULTS TREE (fills middle)
        rf = ttk.Frame(self)
        rf.pack(fill="both", expand=True, padx=6, pady=4, side="top")

        cols = ("symbol", "avg_score", "fwd7", "fwd14", "fwd30",
                "quartile", "n_days", "hit_rate7", "hit_rate14")
        _apply_tree_font(self, "Bt.Treeview", 4)
        self.tree = ttk.Treeview(rf, columns=cols, show="headings",
                                 style="Bt.Treeview", selectmode="browse")
        headers = {
            "symbol":    ("Symbol",      80),
            "avg_score": ("AvgScore",    80),
            "fwd7":      ("Ret7d %",     80),
            "fwd14":     ("Ret14d %",    80),
            "fwd30":     ("Ret30d %",    80),
            "quartile":  ("Quartile",    70),
            "n_days":    ("Days",        55),
            "hit_rate7": ("Hit7d %",     75),
            "hit_rate14":("Hit14d %",    75),
        }
        for c, (hdr, w) in headers.items():
            self.tree.heading(c, text=hdr,
                              command=lambda _c=c: self._sort(_c, False))
            self.tree.column(c, width=w, anchor="e" if c != "symbol" else "w")

        # colour quartiles
        self.tree.tag_configure("q1", background="#c8f7c5")  # top 25% green
        self.tree.tag_configure("q2", background="#eafaea")
        self.tree.tag_configure("q3", background="#fff3cd")
        self.tree.tag_configure("q4", background="#f8d7da")  # bottom 25% red

        sb = ttk.Scrollbar(rf, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y", padx=(0, 3))
        self.tree.pack(fill="both", expand=True)

    # --------------------------------------------------------------- logging
    def _log(self, msg):
        try:
            self.log_box.insert("end", msg + "\n")
            self.log_box.see("end")
            self.update_idletasks()
        except Exception:
            pass

    def _set_status(self, msg):
        self._status_var.set(msg)
        self.update_idletasks()

    # --------------------------------------------------------------- sort
    def _sort(self, col, desc):
        rows = [(self.tree.set(k, col), k) for k in self.tree.get_children("")]
        try:
            rows.sort(key=lambda x: float(x[0].replace("%", "").replace(",", "")) if x[0] not in ("-", "") else -999.0,
                      reverse=desc)
        except Exception:
            rows.sort(key=lambda x: x[0], reverse=desc)
        for i, (_, k) in enumerate(rows):
            self.tree.move(k, "", i)
        self.tree.heading(col, command=lambda _c=col: self._sort(_c, not desc))

    # --------------------------------------------------------------- export
    def _export_csv(self):
        if not self._results:
            messagebox.showinfo("Backtest", "Run backtest first.", parent=self)
            return
        import csv, os
        path = os.path.join(self.paths.data_dir,
                            "backtest_" + utc_now_iso().replace(":", "").replace("-", "")[:15] + ".csv")
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=self._results[0].keys())
                w.writeheader()
                w.writerows(self._results)
            messagebox.showinfo("Exported", "Saved: " + path, parent=self)
        except Exception as e:
            messagebox.showerror("Error", str(e), parent=self)

    # --------------------------------------------------------------- run
    def _run(self):
        if self._running:
            return
        self._running = True
        self._results = []
        for k in self.tree.get_children(""):
            self.tree.delete(k)
        self._summary_var.set("")
        self.log_box.delete("1.0", "end")

        import threading
        t = threading.Thread(target=self._run_worker, daemon=True)
        t.start()

    def _run_worker(self):
        try:
            self._set_status("Fetching top coins list...")
            coins_n   = int(self._coins_var.get())
            days      = int(self._days_var.get())
            vs_curr   = self._curr_var.get().strip() or "usd"

            # Step 1: get current top-N coins list
            url = (
                "https://api.coingecko.com/api/v3/coins/markets"
                "?vs_currency=" + vs_curr +
                "&order=market_cap_desc&per_page=" + str(coins_n) +
                "&page=1&sparkline=false&price_change_percentage=24h"
            )
            self._log("Fetching top " + str(coins_n) + " coins list...")
            market_list = _http_get_json(url, timeout=15)
            if not market_list:
                self._log("ERROR: empty market list")
                self._set_status("Failed")
                return

            self._log("Got " + str(len(market_list)) + " coins. Fetching history...")

            # Step 2: for each coin fetch daily OHLCV + market_chart (price, mcap, vol)
            coin_histories = {}  # {symbol: {date_str: {price, mcap, vol, open, close}}}
            import time as _t
            for idx, coin in enumerate(market_list):
                cid  = coin.get("id", "")
                sym  = (coin.get("symbol") or "").upper()
                rank = coin.get("market_cap_rank") or (idx + 1)
                if not cid or not sym:
                    continue
                self._set_status(f"Fetching {sym} ({idx+1}/{len(market_list)})...")
                try:
                    # market_chart gives daily [timestamp_ms, value] arrays
                    chart_url = (
                        "https://api.coingecko.com/api/v3/coins/" + cid +
                        "/market_chart?vs_currency=" + vs_curr +
                        "&days=" + str(days) + "&interval=daily"
                    )
                    chart = _http_get_json(chart_url, timeout=12)
                    prices   = chart.get("prices",        [])
                    mcaps    = chart.get("market_caps",   [])
                    volumes  = chart.get("total_volumes", [])

                    # Index by date string
                    hist = {}
                    for i2, (ts, price) in enumerate(prices):
                        import datetime as _dt
                        date_str = _dt.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                        hist[date_str] = {
                            "price": float(price),
                            "mcap":  float(mcaps[i2][1])  if i2 < len(mcaps)   else None,
                            "vol24": float(volumes[i2][1]) if i2 < len(volumes) else None,
                            "rank":  rank,
                        }
                    coin_histories[sym] = {"coin_id": cid, "rank": rank, "hist": hist}
                    self._log(f"  {sym}: {len(hist)} days")
                except Exception as exc:
                    self._log(f"  {sym}: SKIP {str(exc)[:80]}")
                _t.sleep(1.8)  # CoinGecko free rate limit

            if not coin_histories:
                self._log("No data fetched.")
                self._set_status("Failed")
                return

            # Step 3: compute scores and forward returns per coin
            self._log("Computing scores and forward returns...")
            results = self._compute_backtest(coin_histories, days)

            # Step 4: display
            self.after(0, lambda: self._show_results(results))

        except Exception as e:
            self._log("FATAL: " + str(e)[:300])
            self._set_status("Error")
        finally:
            self._running = False

    def _compute_backtest(self, coin_histories, total_days):
        """
        For each coin:
        - Compute daily spot_basic score (simplified: rank + mcap + vol)
        - Compute average score over lookback period
        - Compute average forward return at +7, +14, +30 days
        - Hit rate: % of days where forward return was positive
        """
        import datetime as _dt
        import math as _math

        forward_windows = [7, 14, 30]

        coin_stats = []
        for sym, info in coin_histories.items():
            hist  = info["hist"]
            rank  = info["rank"]
            dates = sorted(hist.keys())
            if len(dates) < 35:
                continue

            scores = []
            fwd_returns = {w: [] for w in forward_windows}

            for i, date_str in enumerate(dates):
                d = hist[date_str]
                # Simplified spot_basic score (rank, mcap, vol24 - no chg24 for historical)
                # chg24 approximated from price change vs previous day
                price_today = d["price"]
                if i > 0:
                    price_prev = hist[dates[i-1]]["price"]
                    chg24 = ((price_today - price_prev) / price_prev * 100.0) if price_prev else 0.0
                else:
                    chg24 = 0.0

                sc = spot_basic_score(
                    rank=d.get("rank") or rank,
                    mcap=d.get("mcap"),
                    vol24=d.get("vol24"),
                    chg24=chg24,
                )
                scores.append(sc)

                # Forward returns
                for w in forward_windows:
                    future_idx = i + w
                    if future_idx < len(dates):
                        future_date = dates[future_idx]
                        price_future = hist[future_date]["price"]
                        if price_today and price_today > 0:
                            ret_pct = (price_future - price_today) / price_today * 100.0
                            fwd_returns[w].append(ret_pct)

            if not scores:
                continue

            avg_score = round(sum(scores) / len(scores), 1)
            fwd_avgs  = {}
            hit_rates = {}
            for w in forward_windows:
                rets = fwd_returns[w]
                if rets:
                    fwd_avgs[w]  = round(sum(rets) / len(rets), 2)
                    hit_rates[w] = round(sum(1 for r in rets if r > 0) / len(rets) * 100.0, 1)
                else:
                    fwd_avgs[w]  = None
                    hit_rates[w] = None

            coin_stats.append({
                "symbol":     sym,
                "rank":       rank,
                "avg_score":  avg_score,
                "n_days":     len(scores),
                "fwd7":       fwd_avgs.get(7),
                "fwd14":      fwd_avgs.get(14),
                "fwd30":      fwd_avgs.get(30),
                "hit_rate7":  hit_rates.get(7),
                "hit_rate14": hit_rates.get(14),
            })

        if not coin_stats:
            return []

        # Assign quartile by avg_score
        coin_stats.sort(key=lambda x: x["avg_score"], reverse=True)
        n = len(coin_stats)
        for i, c in enumerate(coin_stats):
            q = int(i / n * 4) + 1
            c["quartile"] = min(q, 4)

        return coin_stats

    def _show_results(self, results):
        self._results = results
        if not results:
            self._set_status("No results")
            return

        for k in self.tree.get_children(""):
            self.tree.delete(k)

        for c in results:
            q   = c.get("quartile", 4)
            tag = "q" + str(q)
            self.tree.insert("", "end", iid=c["symbol"], values=(
                c["symbol"],
                _fmt(c["avg_score"], 1),
                _fmt(c["fwd7"],  2) if c["fwd7"]  is not None else "-",
                _fmt(c["fwd14"], 2) if c["fwd14"] is not None else "-",
                _fmt(c["fwd30"], 2) if c["fwd30"] is not None else "-",
                "Q" + str(q),
                c["n_days"],
                _fmt(c["hit_rate7"],  1) + "%" if c["hit_rate7"]  is not None else "-",
                _fmt(c["hit_rate14"], 1) + "%" if c["hit_rate14"] is not None else "-",
            ), tags=(tag,))

        # Quartile summary
        from collections import defaultdict
        q_fwd7  = defaultdict(list)
        q_fwd30 = defaultdict(list)
        for c in results:
            q = c.get("quartile", 4)
            if c["fwd7"]  is not None: q_fwd7[q].append(c["fwd7"])
            if c["fwd30"] is not None: q_fwd30[q].append(c["fwd30"])

        summary_parts = []
        for q in [1, 2, 3, 4]:
            r7  = sum(q_fwd7[q])  / len(q_fwd7[q])  if q_fwd7[q]  else 0
            r30 = sum(q_fwd30[q]) / len(q_fwd30[q]) if q_fwd30[q] else 0
            label = ["TOP 25%", "Q2", "Q3", "BOT 25%"][q-1]
            summary_parts.append(
                f"{label}: ret7d={r7:+.1f}%  ret30d={r30:+.1f}%"
            )

        # Edge: Q1 vs Q4
        q1_7  = sum(q_fwd7[1])  / len(q_fwd7[1])  if q_fwd7[1]  else 0
        q4_7  = sum(q_fwd7[4])  / len(q_fwd7[4])  if q_fwd7[4]  else 0
        q1_30 = sum(q_fwd30[1]) / len(q_fwd30[1]) if q_fwd30[1] else 0
        q4_30 = sum(q_fwd30[4]) / len(q_fwd30[4]) if q_fwd30[4] else 0
        edge7  = q1_7  - q4_7
        edge30 = q1_30 - q4_30

        edge_verdict = "  |  EDGE 7d: {:+.1f}%  EDGE 30d: {:+.1f}%  {}".format(
            edge7, edge30,
            "*** SIGNAL DETECTED ***" if abs(edge7) > 3 or abs(edge30) > 5 else "(no clear edge yet)"
        )

        self._summary_var.set("  |  ".join(summary_parts) + edge_verdict)
        self._set_status("Done -- " + str(len(results)) + " coins analysed")
        self._log("DONE. " + str(len(results)) + " coins.")


class App(tk.Tk):
    def __init__(self):
        super(App, self).__init__()
        self._apply_global_font(4)
        self.title("NyoSig Analysator  " + APP_VERSION)
        self.minsize(760, 640)
        # Fullscreen + rotation-aware via after() polling
        _app_fs = [0, 0]
        def _app_poll():
            try:
                sw = self.winfo_screenwidth()
                sh = self.winfo_screenheight()
                if sw != _app_fs[0] or sh != _app_fs[1]:
                    _app_fs[0] = sw
                    _app_fs[1] = sh
                    self.geometry(str(sw) + "x" + str(sh) + "+0+0")
            except Exception:
                pass
            try:
                self.after(400, _app_poll)
            except Exception:
                pass
        self.after(0, _app_poll)

        self.paths = make_paths(PROJECT_ROOT)
        for d in [self.paths.cache_dir, self.paths.log_dir,
                  self.paths.data_dir, self.paths.db_dir]:
            ensure_dir(d)

        # Task 19: write default providers.yaml if missing, then load it
        write_default_providers_yaml(self.paths.providers_yaml)
        self._providers_cfg = parse_providers_config(self.paths.providers_yaml)
        apply_providers_to_rl_manager(self._providers_cfg)

        cfg = parse_simple_yaml(self.paths.defaults_yaml)
        mvp = cfg.get("mvp", {})

        self.scope        = tk.StringVar(value="crypto_spot")
        self.vs_currency  = tk.StringVar(value=str(mvp.get("vs_currency", "usd")))
        self.coins_limit  = tk.IntVar(value=int(mvp.get("coins_limit", 250)))
        self.order        = tk.StringVar(value=str(mvp.get("order", "market_cap_desc")))
        self.topnow_limit = tk.IntVar(value=int(mvp.get("topnow_limit", 100)))
        self.offline_mode = tk.BooleanVar(value=bool(mvp.get("offline_mode", False)))

        self.status_var    = tk.StringVar(value="Ready")
        self.run_id_var    = tk.StringVar(value="-")
        self.snapshot_var  = tk.StringVar(value="-")
        self.selection_var = tk.StringVar(value="-")

        self.last_log_path = None
        self._build()

    def _show_about(self):
        """Task 7: About dialog -- version, arch ref, sha256, doc hierarchy."""
        sha = compute_self_sha256()
        short_sha = sha[:16] + "..." if len(sha) > 16 else sha
        lines = [
            "NyoSig Analysator",
            "",
            "Version:    " + APP_VERSION,
            "Arch ref:   v3.0c",
            "Tasklist:   IMPLEMENTATION_SYNC_TASKLIST_v1.2a",
            "SHA256:     " + short_sha,
            "",
            "Root:       " + self.paths.project_root,
            "DB:         " + self.paths.db_path,
            "",
            "Concept, architecture and implementation by",
            "Marek Sima",
            "hutrat05@gmail.com  |  +44 7395 813008",
            "",
            "Principles:",
            "  - Every run is a RUN with unique run_id",
            "  - RAW data is immutable source of truth",
            "  - Append-only, no overwrite of historical data",
            "  - All outputs bound to run_id",
        ]
        messagebox.showinfo("About NyoSig Analysator",
                            "\n".join(lines), parent=self)

    def _apply_global_font(self, size):
        for name in ("TkDefaultFont", "TkTextFont", "TkFixedFont", "TkMenuFont",
                     "TkHeadingFont", "TkCaptionFont", "TkSmallCaptionFont"):
            try:
                tkfont.nametofont(name).configure(size=size)
            except Exception:
                pass
        try:
            f = tkfont.nametofont("TkDefaultFont")
            st = ttk.Style(self)
            st.configure(".", font=f)
            st.configure("Treeview", font=f, rowheight=max(18, size * 5))
            st.configure("Treeview.Heading", font=f)
        except Exception:
            pass


    def _build(self):
        root = ttk.Frame(self, padding=10)
        root.pack(fill="both", expand=True)

        hdr = ttk.Frame(root)
        hdr.pack(fill="x")
        ttk.Label(hdr, text="Project root").grid(row=0, column=0, sticky="w")
        ttk.Label(hdr, text=self.paths.project_root).grid(row=0, column=1, sticky="w")
        ttk.Label(hdr, text="DB").grid(row=1, column=0, sticky="w")
        ttk.Label(hdr, text=self.paths.db_path).grid(row=1, column=1, sticky="w")
        hdr.grid_columnconfigure(2, weight=1)
        # Task 21: permanent version widget -- always visible, right-aligned
        _ver_lbl = ttk.Label(hdr, text=APP_VERSION, foreground="#1a6e1a",
                             font=("TkDefaultFont", 5, "bold"))
        _ver_lbl.grid(row=0, column=3, sticky="e", padx=(8, 2))
        # Task 7: About button
        ttk.Button(hdr, text="About", command=self._show_about).grid(
            row=1, column=3, sticky="e", padx=(8, 2))


        ttk.Separator(root).pack(fill="x", pady=6)

        form = ttk.Frame(root)
        form.pack(fill="x")
        ttk.Label(form, text="Scope").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=3)
        # v7.0a: Multi-asset scope selector
        ttk.Combobox(form, textvariable=self.scope, state="readonly",
            values=["crypto_spot", "forex_spot", "stocks_spot", "macro_dashboard"],
            width=22).grid(row=0, column=1, sticky="w", pady=3)
        ttk.Label(form, text="vs_currency").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=3)
        e_vs = ttk.Entry(form, textvariable=self.vs_currency, width=12)
        e_vs.grid(row=1, column=1, sticky="w", pady=3)
        ttk.Label(form, text="coins_limit").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=3)
        e_cl = ttk.Entry(form, textvariable=self.coins_limit, width=12)
        e_cl.grid(row=2, column=1, sticky="w", pady=3)
        ttk.Label(form, text="order").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=3)
        e_ord = ttk.Entry(form, textvariable=self.order, width=26)
        e_ord.grid(row=3, column=1, sticky="w", pady=3)
        ttk.Label(form, text="TopNow candidates").grid(row=4, column=0, sticky="w", padx=(0, 8), pady=3)
        e_tn = ttk.Entry(form, textvariable=self.topnow_limit, width=12)
        e_tn.grid(row=4, column=1, sticky="w", pady=3)
        ttk.Checkbutton(form, text="offline_mode", variable=self.offline_mode).grid(
            row=5, column=1, sticky="w", pady=4)

        ttk.Separator(root).pack(fill="x", pady=6)

        r1 = ttk.Frame(root)
        r1.pack(fill="x", pady=2)
        ttk.Button(r1, text="Run Snapshot + TopNow",  command=self.run_pipeline).pack(side="left")
        ttk.Button(r1, text="Analyse >",             command=self.open_analyse_workflow).pack(side="left", padx=4)
        ttk.Button(r1, text="Prepare Analysis",       command=self.do_analysis).pack(side="left", padx=4)
        ttk.Button(r1, text="Open Results",           command=self.open_results).pack(side="left", padx=4)
        ttk.Button(r1, text="Analysis Detail",        command=self.open_analysis_detail).pack(side="left", padx=4)

        r2 = ttk.Frame(root)
        r2.pack(fill="x", pady=2)
        ttk.Button(r2, text="Export CSV",        command=self.export_csv).pack(side="left")
        ttk.Button(r2, text="Watchlist",         command=self.open_watchlist).pack(side="left", padx=4)
        ttk.Button(r2, text="Snapshot Diff",     command=self.open_diff).pack(side="left", padx=4)
        r3 = ttk.Frame(root)
        r3.pack(fill="x", pady=2)
        ttk.Button(r3, text="Promotion / Expanders", command=self.open_promotion).pack(side="left")
        ttk.Button(r3, text="Log Viewer",            command=self.open_log_viewer).pack(side="left", padx=4)
        ttk.Button(r3, text="[!] Replay",             command=self.open_replay_mode).pack(side="left", padx=4)
        ttk.Button(r3, text="Backtest",               command=self.open_backtest).pack(side="left", padx=4)
        # v7.1a: Predictions, Trade Plans, Summary
        r4 = ttk.Frame(root)
        r4.pack(fill="x", pady=2)
        ttk.Button(r4, text="Predictions",  command=self.open_predictions).pack(side="left")
        ttk.Button(r4, text="Trade Plans",  command=self.open_trade_plans).pack(side="left", padx=4)
        ttk.Button(r4, text="Summary",      command=self.open_summary).pack(side="left", padx=4)
        # v7.2a: Operational buttons
        r5 = ttk.Frame(root)
        r5.pack(fill="x", pady=2)
        ttk.Button(r5, text="Alerts",       command=self.open_alerts).pack(side="left")
        ttk.Button(r5, text="Pred. Perf",   command=self.open_pred_perf).pack(side="left", padx=4)
        ttk.Button(r5, text="Export CSV",    command=self.do_export_csv).pack(side="left", padx=4)
        ttk.Button(r5, text="Export Summary",command=self.do_export_summary).pack(side="left", padx=4)
        ttk.Button(r5, text="Quick Refresh Tracked", command=self.do_tracked_refresh).pack(side="left", padx=4)
        # v7.4a: Scheduler + Health
        r6 = ttk.Frame(root)
        r6.pack(fill="x", pady=2)
        self._sched_var = tk.StringVar(value="Scheduler: OFF")
        self._scheduler = None
        ttk.Button(r6, text="Start Scheduler", command=self._toggle_scheduler).pack(side="left")
        ttk.Label(r6, textvariable=self._sched_var, foreground="#666").pack(side="left", padx=8)
        ttk.Button(r6, text="Health Check",    command=self._show_health).pack(side="left", padx=4)
        ttk.Button(r6, text="Backtest v2",     command=self._open_backtest_v2).pack(side="left", padx=4)
        ttk.Button(r6, text="Portfolio",       command=self._open_portfolio).pack(side="left", padx=4)
        # v7.5a: Config profile buttons
        r7 = ttk.Frame(root)
        r7.pack(fill="x", pady=2)
        ttk.Button(r7, text="Save Profile",    command=self._save_profile).pack(side="left")
        ttk.Button(r7, text="Load Profile",    command=self._load_profile).pack(side="left", padx=4)
        ttk.Button(r3, text="Open last run log",     command=self.open_last_log).pack(side="right")

        ttk.Separator(root).pack(fill="x", pady=6)

        info = ttk.Frame(root)
        info.pack(fill="x")
        labels = [
            ("Status",           self.status_var),
            ("Last run_id",      self.run_id_var),
            ("Last snapshot_id", self.snapshot_var),
            ("Last selection_id",self.selection_var),
        ]
        for i, (lbl, var) in enumerate(labels):
            ttk.Label(info, text=lbl).grid(row=i, column=0, sticky="w", pady=1)
            ttk.Label(info, textvariable=var).grid(row=i, column=1, sticky="w", pady=1)

        ttk.Separator(root).pack(fill="x", pady=6)

        self.log = tk.Text(root, height=18)
        self.log.pack(fill="both", expand=True)
        self.log.insert("end", "Log\n")

    def log_cb(self, msg):
        self.log.insert("end", utc_now_iso() + " " + msg + "\n")
        self.log.see("end")
        self.update_idletasks()

    def _snap(self):
        return self.snapshot_var.get().strip()

    def _sel(self):
        return self.selection_var.get().strip()

    def run_pipeline(self):
        scope        = self.scope.get().strip() or "crypto_spot"
        vs_currency  = self.vs_currency.get().strip().lower() or "usd"
        coins_limit  = int(self.coins_limit.get())
        order        = self.order.get().strip() or "market_cap_desc"
        topnow_limit = int(self.topnow_limit.get())
        offline      = bool(self.offline_mode.get())

        if scope != "crypto_spot":
            # v7.0a: Non-crypto scopes now supported
            scope_def = get_scope(scope)
            if scope_def:
                self.log_cb(f"Scope: {scope_def.display_name} ({scope_def.asset_class})")
            else:
                messagebox.showwarning("Unknown scope",
                    f"Scope '{scope}' is not registered in SCOPE_REGISTRY.",
                    parent=self)
                return

        self.last_log_path = os.path.join(
            self.paths.log_dir, "run_" + utc_stamp_compact() + "_" + APP_VERSION + ".log")
        self.status_var.set("Run: 0%")
        self.log_cb("START pipeline (core)")

        # Pipeline has ~8 major checkpoints -- track progress by log messages
        _PIPELINE_STEPS = [
            ("Fetching", "Run: 10% fetching"),
            ("cache_save", "Run: 20% caching"),
            ("Snapshot OK", "Run: 30% snapshot"),
            ("SpotBasic scored", "Run: 40% scored"),
            ("OHLCV done", "Run: 55% ohlcv"),
            ("TOP selection built", "Run: 65% top"),
            ("macro ingestion done", "Run: 75% macro"),
            ("sentiment ingested", "Run: 82% sentiment"),
            ("onchain ingestion done", "Run: 89% onchain"),
            ("fundamental ingestion done", "Run: 96% fund."),
        ]
        def _prog_log_cb(msg):
            self.log_cb(msg)
            for trigger, label in _PIPELINE_STEPS:
                if trigger in msg:
                    self.status_var.set(label)
                    self.update_idletasks()
                    break
        try:
            res = run_snapshot_and_topnow(
                project_root=self.paths.project_root,
                app_version=APP_VERSION,
                scope_text=scope,
                vs_currency=vs_currency,
                coins_limit=coins_limit,
                order=order,
                offline_mode=offline,
                log_cb=_prog_log_cb,
                topnow_limit=topnow_limit,
            )
            self.run_id_var.set(str(res.run_id))
            self.snapshot_var.set(res.snapshot_id)
            self.selection_var.set(str(res.selection_id))
            try:
                con_r = db_connect(self.paths.db_path)
                # Task 17: read keep_runs from config, default to RETENTION_KEEP_RUNS
                try:
                    _cfg_r = parse_simple_yaml(self.paths.defaults_yaml)
                    _keep = int(_cfg_r.get("retention", {}).get("keep_runs",
                                _cfg_r.get("keep_runs", RETENTION_KEEP_RUNS)))
                except Exception:
                    _keep = RETENTION_KEEP_RUNS
                ret = apply_retention_policy(con_r, keep_runs=_keep, log_cb=self.log_cb)
                con_r.close()
                if ret.get("pruned"):
                    self.log_cb("RETENTION removed=" + str(ret["total_removed"])
                        + " protected=" + str(ret["tracked_protected"]))
            except Exception as _re:
                self.log_cb("RETENTION warn: " + str(_re)[:80])
            self.status_var.set("Done")
            self.log_cb("DONE run_id=" + str(res.run_id) +
                        " snapshot_id=" + res.snapshot_id +
                        " selection_id=" + str(res.selection_id) +
                        " candidates=" + str(res.candidates_n))
            with open(self.last_log_path, "w", encoding="utf-8") as fp:
                fp.write("run_id       " + str(res.run_id) + "\n")
                fp.write("snapshot_id  " + res.snapshot_id + "\n")
                fp.write("selection_id " + str(res.selection_id) + "\n")
                fp.write("candidates   " + str(res.candidates_n) + "\n")
        except Exception as e:
            self.status_var.set("Failed")
            self.log_cb("FAILED " + str(e))
            messagebox.showerror("Error", str(e)[:1200], parent=self)

    def do_analysis(self):
        sid  = self._sel()
        if not sid or sid == "-":
            messagebox.showinfo("Info", "Run pipeline first.", parent=self)
            return
        con = None
        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            # All layer scopes -- scope dropdown is for ingestion, not for analysis
            scopes = [lr["scope_key"] for lr in LAYER_REGISTRY]
            _ANALYSIS_LAYERS = 7
            self.status_var.set("Analyse: 0%")
            self.update_idletasks()
            run_id_val = int(self.run_id_var.get()) if self.run_id_var.get() not in ("-", "") else None
            def _analysis_progress(step, total, layer_name):
                pct = int(step / total * 100)
                self.status_var.set("Analyse: " + str(pct) + "% (" + layer_name + ")")
                self.update_idletasks()
            res = prepare_and_store_composite_preview(
                con, int(sid), scopes, run_id=run_id_val,
                progress_cb=_analysis_progress
            )
            self.status_var.set("Done")
            _s = res.get("sentiment_ok"); _m = res.get("macro_ok")
            self.log_cb("ANALYSIS done: sel=" + str(res["selection_id"]) +
                        " updated=" + str(res["updated_items"]) +
                        " layers=" + str(len(res["scopes"])) +
                        " sentiment=" + ("OK" if _s is True else "skipped" if _s is None else "FAILED") +
                        " macro=" + ("OK" if _m is True else "skipped" if _m is None else "FAILED"))
            # Auto-promote expanders to Watchlist stage 0
            auto_promoted = 0
            try:
                con2 = db_connect(self.paths.db_path)
                snap = self._snap()
                existing = set(r[0] for r in con2.execute(
                    "SELECT unified_symbol FROM watchlist WHERE exit_timestamp_utc IS NULL;"
                ).fetchall())
                expanders = con2.execute(
                    "SELECT unified_symbol, composite_preview FROM topnow_selection_items"
                    " WHERE selection_id=? AND composite_preview >= ?;",
                    (int(sid), EXPANDER_THRESHOLD)
                ).fetchall()
                for sym, score in expanders:
                    if sym not in existing:
                        con2.execute(
                            "INSERT OR IGNORE INTO watchlist (unified_symbol, tag, stage,"
                            " tracking_since_utc, entry_snapshot_id, entry_score)"
                            " VALUES (?,?,?,?,?,?);",
                            (sym, "expander", "0", utc_now_iso(), snap, float(score or 0))
                        )
                        auto_promoted += 1
                con2.commit()
                con2.close()
            except Exception:
                pass
            self.log_cb("AUTO-PROMOTED " + str(auto_promoted) + " expanders to Watchlist stage 0")
            # Build status line from available layer results
            _s_ok = res.get("sentiment_ok")
            _m_ok = res.get("macro_ok")
            sent_status  = "OK" if _s_ok is True else ("skipped" if _s_ok is None else "FAILED")
            macro_status = "OK" if _m_ok is True else ("skipped" if _m_ok is None else "FAILED")
            tech_n   = res.get("technical_symbols",  0)
            comm_n   = res.get("community_symbols",  0)
            oi_n     = res.get("oi_symbols",         0)
            fund_n   = res.get("fundamental_symbols", 0)
            fund_symbols = res.get("fundamental_symbols", 0)
            messagebox.showinfo("Analysis",
                "Composite preview updated.\n"
                "Items updated: " + str(res["updated_items"]) + "\n"
                "Layers run: " + str(len(res["scopes"])) + "\n"
                "\n"
                "Sentiment: " + sent_status + " | Macro: " + macro_status + "\n"
                "Technical: " + str(tech_n) + " syms | Community: " + str(comm_n) + " syms\n"
                "OpenInterest: " + str(oi_n) + " syms | Fundamental: " + str(fund_n) + " syms\n"
                "\n"
                "Auto-promoted to Watchlist: " + str(auto_promoted),
                parent=self)
        except Exception as e:
            self.status_var.set("Failed")
            self.log_cb("ANALYSIS FAILED " + str(e))
            messagebox.showerror("Error", str(e)[:1200], parent=self)
        finally:
            if con:
                con.close()

    def open_analyse_workflow(self):
        sid = self._sel()
        snap = self._snap()
        if not sid or sid == "-":
            messagebox.showinfo("Info", "Run Snapshot + TopNow first.", parent=self)
            return
        # Task 20: verify selection has items and last run completed
        try:
            con_g = db_connect(self.paths.db_path)
            n_items = con_g.execute(
                "SELECT COUNT(*) FROM topnow_selection_items WHERE selection_id=?;",
                (int(sid),),
            ).fetchone()[0]
            run_status = "-"
            rid = self._rid()
            if rid and rid != "-":
                row = con_g.execute(
                    "SELECT status FROM runs WHERE run_id=?;", (int(rid),)
                ).fetchone()
                if row:
                    run_status = row[0]
            con_g.close()
            if n_items == 0:
                messagebox.showinfo(
                    "Info",
                    "Selection is empty (sel=" + str(sid) + ").\n"
                    "Run pipeline again to build a valid TOP selection.",
                    parent=self,
                )
                return
            if run_status not in ("completed", "ok", "partial", "degraded"):
                messagebox.showinfo(
                    "Info",
                    "Last run status is '" + run_status + "'.\n"
                    "Run Snapshot + TopNow first to get a completed run.",
                    parent=self,
                )
                return
        except Exception:
            pass  # guard failures are non-fatal: proceed
        CandidateSelectionWindow(self, self.paths, int(sid), snap, self._rid)

    def _rid(self):
        return self.run_id_var.get().strip()

    def open_results(self):
        sid = self._sel()
        snap = self._snap()
        if not sid or sid == "-":
            messagebox.showinfo("Info", "Run pipeline first.", parent=self)
            return
        ResultWindow(self, self.paths, int(sid), snap)

    def open_analysis_detail(self):
        sid = self._sel()
        snap = self._snap()
        if not sid or sid == "-":
            messagebox.showinfo("Info", "Run pipeline first.", parent=self)
            return
        AnalyseWindow(self, self.paths, int(sid), snap)

    def open_watchlist(self):
        WatchlistWindow(self, self.paths, self._snap)

    def open_diff(self):
        DiffWindow(self, self.paths)

    def open_promotion(self):
        sid = self._sel()
        snap = self._snap()
        if not sid or sid == "-":
            messagebox.showinfo("Info", "Run pipeline and Prepare Analysis first.", parent=self)
            return
        PromotionWindow(self, self.paths, int(sid), snap)

    def open_replay_mode(self):
        ReplayWindow(self, self.paths)

    def open_backtest(self):
        BacktestWindow(self, self.paths)

    def open_predictions(self):
        sid = self._sel()
        if not sid or sid == "-":
            messagebox.showinfo("Info", "Run analysis first.", parent=self)
            return
        PredictionsWindow(self, self.paths, int(sid), lambda: self.run_id_var.get())

    def open_trade_plans(self):
        sid = self._sel()
        if not sid or sid == "-":
            messagebox.showinfo("Info", "Run analysis first.", parent=self)
            return
        TradePlanWindow(self, self.paths, int(sid), lambda: self.run_id_var.get())

    def open_summary(self):
        sid = self._sel()
        if not sid or sid == "-":
            messagebox.showinfo("Info", "Run analysis first.", parent=self)
            return
        SummaryDashboard(self, self.paths, int(sid), lambda: self.run_id_var.get())

    def open_alerts(self):
        AlertsWindow(self, self.paths, lambda: self.run_id_var.get())

    def open_pred_perf(self):
        PredictionPerformanceWindow(self, self.paths)

    def do_export_csv(self):
        sid = self._sel()
        rid = self.run_id_var.get()
        if not sid or sid == "-" or not rid or rid == "-":
            messagebox.showinfo("Info", "Run analysis first.", parent=self)
            return
        fname = "analysis_" + utc_stamp_compact() + "_" + APP_VERSION + ".csv"
        out_path = os.path.join(self.paths.data_dir, fname)
        try:
            ensure_dir(self.paths.data_dir)
            con = db_connect(self.paths.db_path)
            n = export_analysis_csv(con, int(rid), int(sid), out_path, log_cb=self.log_cb)
            con.close()
            messagebox.showinfo("Export", f"Exported {n} rows to:\n{out_path}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Error", str(e)[:500], parent=self)

    def do_export_summary(self):
        sid = self._sel()
        rid = self.run_id_var.get()
        if not sid or sid == "-" or not rid or rid == "-":
            messagebox.showinfo("Info", "Run analysis first.", parent=self)
            return
        fname = "summary_" + utc_stamp_compact() + "_" + APP_VERSION + ".txt"
        out_path = os.path.join(self.paths.data_dir, fname)
        try:
            ensure_dir(self.paths.data_dir)
            con = db_connect(self.paths.db_path)
            export_summary_text(con, int(rid), int(sid), out_path, log_cb=self.log_cb)
            con.close()
            messagebox.showinfo("Export", f"Summary exported to:\n{out_path}", parent=self)
        except Exception as e:
            messagebox.showerror("Export Error", str(e)[:500], parent=self)

    def do_tracked_refresh(self):
        """v7.3a: Quick price refresh for watchlist assets only."""
        self.status_var.set("Refreshing tracked assets...")
        self.log_cb("Quick Refresh: starting...")
        self.update_idletasks()
        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            result = tracked_only_refresh(con, APP_VERSION, log_cb=self.log_cb)
            con.close()
            self.status_var.set("Refresh done")
            alerts = result.get("alerts", [])
            msg = (f"Refreshed: {result['refreshed']}/{result['total_tracked']} tracked assets\n"
                   f"New alerts: {result['new_alerts']}")
            if alerts:
                msg += "\n\nAlerts:\n" + "\n".join(a["message"] for a in alerts[:5])
            messagebox.showinfo("Tracked Refresh", msg, parent=self)
        except Exception as e:
            self.status_var.set("Refresh failed")
            messagebox.showerror("Error", str(e)[:500], parent=self)

    def _toggle_scheduler(self):
        """v7.4a: Start/stop periodic tracked refresh scheduler."""
        if self._scheduler and self._scheduler.is_running():
            self._scheduler.stop()
            self._scheduler = None
            self._sched_var.set("Scheduler: OFF")
            self.log_cb("Scheduler stopped")
        else:
            self._scheduler = TrackedRefreshScheduler(
                db_path=self.paths.db_path,
                app_version=APP_VERSION,
                interval_minutes=30,
                log_cb=self.log_cb,
            )
            self._scheduler.start()
            self._sched_var.set("Scheduler: ON (30 min)")
            self.log_cb("Scheduler started: auto-refresh every 30 minutes")

    def _show_health(self):
        """v7.4a: Health check dashboard."""
        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            health = system_health_check(con, scheduler=self._scheduler)
            con.close()
        except Exception as e:
            messagebox.showerror("Error", str(e)[:500], parent=self)
            return
        win = tk.Toplevel(self)
        win.title("System Health  " + APP_VERSION)
        _setup_fullscreen(win)
        tb = ttk.Frame(win, padding=4)
        tb.pack(fill="x", side="top")
        ttk.Button(tb, text="Close", command=win.destroy).pack(side="right")
        status_colors = {"ok": "#006400", "warning": "#CC6600", "critical": "#8B0000"}
        ttk.Label(tb, text=f"Status: {health['status'].upper()}",
                  foreground=status_colors.get(health["status"], "#000"),
                  font=("TkDefaultFont", 6, "bold")).pack(side="left", padx=8)
        txt = tk.Text(win, wrap="word", padx=12, pady=8)
        txt.pack(fill="both", expand=True)
        txt.insert("end", "=== SYSTEM HEALTH CHECK ===\n\n")
        for key, val in health.get("checks", {}).items():
            if isinstance(val, dict):
                txt.insert("end", f"{key}:\n")
                for k2, v2 in val.items():
                    txt.insert("end", f"  {k2}: {v2}\n")
            else:
                txt.insert("end", f"{key}: {val}\n")
        if health.get("warnings"):
            txt.insert("end", "\n--- WARNINGS ---\n")
            for w in health["warnings"]:
                txt.insert("end", f"  [!] {w}\n")
        else:
            txt.insert("end", "\nNo warnings.\n")
        txt.insert("end", f"\nChecked: {utc_now_iso()}")
        txt.configure(state="disabled")

    def _open_backtest_v2(self):
        """v7.4a: Backtest using trade_plans data."""
        sid = self._sel()
        rid = self.run_id_var.get()
        if not sid or sid == "-" or not rid or rid == "-":
            messagebox.showinfo("Info", "Run analysis first.", parent=self)
            return
        try:
            con = db_connect(self.paths.db_path)
            results = backtest_from_trade_plans(con, int(rid), int(sid))
            con.close()
        except Exception as e:
            messagebox.showerror("Error", str(e)[:500], parent=self)
            return
        win = tk.Toplevel(self)
        win.title("Backtest v2 (Trade Plans)  " + APP_VERSION)
        _setup_fullscreen(win)
        tb = ttk.Frame(win, padding=4)
        tb.pack(fill="x", side="top")
        ttk.Button(tb, text="Close", command=win.destroy).pack(side="right")
        cols = ("symbol", "dir", "entry_mid", "current", "pnl%", "status", "stop_hit", "t1_hit", "t2_hit")
        tree = ttk.Treeview(win, columns=cols, show="headings", height=20)
        for c, w in zip(cols, (70, 50, 80, 80, 60, 90, 50, 50, 50)):
            tree.heading(c, text=c)
            tree.column(c, width=w, anchor="center")
        tree.pack(fill="both", expand=True, padx=6, pady=4)
        STATUS_COLORS = {"target_2_hit": "#006400", "target_1_hit": "#228B22",
                         "open": "#333", "stopped_out": "#8B0000", "no_trade": "#999"}
        wins = losses = opens = 0
        total_pnl = 0
        for r in results:
            status = r.get("status", "")
            pnl = r.get("pnl_pct", 0) or 0
            if status in ("target_1_hit", "target_2_hit"): wins += 1
            elif status == "stopped_out": losses += 1
            elif status == "open": opens += 1
            total_pnl += pnl
            iid = tree.insert("", "end", values=(
                r["symbol"], r.get("direction", ""),
                _fmt(r.get("entry_mid"), 2), _fmt(r.get("current_price"), 2),
                _fmt_signed(pnl, 1), status,
                "Y" if r.get("hit_stop") else "", "Y" if r.get("hit_t1") else "",
                "Y" if r.get("hit_t2") else "",
            ))
            try:
                tree.tag_configure(status, foreground=STATUS_COLORS.get(status, "#000"))
                tree.item(iid, tags=(status,))
            except Exception:
                pass
        # Summary
        summary = ttk.Label(win,
            text=f"Plans: {len(results)}  |  Wins: {wins}  Losses: {losses}  "
                 f"Open: {opens}  |  Total P&L: {total_pnl:+.1f}%",
            font=("TkDefaultFont", 5, "bold"))
        summary.pack(fill="x", padx=6, pady=4)

    def _open_portfolio(self):
        """v7.5a: Open portfolio management window."""
        PortfolioWindow(self, self.paths, lambda: self.run_id_var.get())

    def _save_profile(self):
        """v7.5a: Save current settings as a named profile."""
        import tkinter.simpledialog
        name = tkinter.simpledialog.askstring("Save Profile", "Profile name:", parent=self)
        if not name:
            return
        config = {
            "scope": self.scope.get(),
            "vs_currency": self.vs_currency.get(),
            "coins_limit": self.coins_limit.get(),
            "order": self.order.get(),
            "topnow_limit": self.topnow_limit.get(),
            "offline_mode": self.offline_mode.get(),
        }
        # Include layer configs
        try:
            layer_cfgs = {}
            for sk, adapter in LAYER_ADAPTERS.items():
                c = adapter.get_config()
                if c:
                    layer_cfgs[sk] = c
            config["layer_configs"] = layer_cfgs
        except Exception:
            pass
        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            save_config_profile(con, name, self.scope.get(), config)
            con.close()
            messagebox.showinfo("Profile Saved", f"Profile '{name}' saved.", parent=self)
            self.log_cb(f"Config profile saved: {name}")
        except Exception as e:
            messagebox.showerror("Error", str(e)[:500], parent=self)

    def _load_profile(self):
        """v7.5a: Load a saved profile."""
        try:
            con = db_connect(self.paths.db_path)
            ensure_schema(con)
            profiles = list_config_profiles(con)
            con.close()
        except Exception as e:
            messagebox.showerror("Error", str(e)[:500], parent=self)
            return
        if not profiles:
            messagebox.showinfo("No Profiles", "No saved profiles found.", parent=self)
            return
        # Pick dialog
        win = tk.Toplevel(self)
        win.title("Load Profile")
        win.geometry("400x300")
        lb = tk.Listbox(win, font=("TkDefaultFont", 5))
        lb.pack(fill="both", expand=True, padx=8, pady=8)
        for p in profiles:
            lb.insert("end", f"{p[0]}  ({p[1]})  created={p[2][:10]}")
        def _pick():
            sel = lb.curselection()
            if not sel:
                return
            profile_name = profiles[sel[0]][0]
            try:
                con = db_connect(self.paths.db_path)
                result = load_config_profile(con, profile_name)
                con.close()
                if result:
                    scope, cfg = result
                    self.scope.set(cfg.get("scope", scope))
                    self.vs_currency.set(cfg.get("vs_currency", "usd"))
                    self.coins_limit.set(cfg.get("coins_limit", 250))
                    self.order.set(cfg.get("order", "market_cap_desc"))
                    self.topnow_limit.set(cfg.get("topnow_limit", 100))
                    self.offline_mode.set(cfg.get("offline_mode", False))
                    # Restore layer configs
                    for sk, lc in cfg.get("layer_configs", {}).items():
                        adapter = get_layer_adapter(sk)
                        if adapter:
                            adapter.set_config(lc)
                    self.log_cb(f"Loaded profile: {profile_name}")
                    messagebox.showinfo("Profile Loaded",
                        f"Profile '{profile_name}' loaded.", parent=self)
            except Exception as e:
                messagebox.showerror("Error", str(e)[:500], parent=self)
            win.destroy()
        ttk.Button(win, text="Load", command=_pick).pack(pady=4)
        ttk.Button(win, text="Cancel", command=win.destroy).pack()

    def open_log_viewer(self):
        LogViewerWindow(self, self.paths)

    def open_last_log(self):
        if not self.last_log_path or not os.path.isfile(self.last_log_path):
            messagebox.showinfo("Info", "No log file yet.", parent=self)
            return
        try:
            if sys.platform.startswith("linux"):
                os.system("xdg-open \"" + self.last_log_path + "\"")
            else:
                os.system("open \"" + self.last_log_path + "\"")
        except Exception:
            messagebox.showinfo("Log path", self.last_log_path, parent=self)

    def export_csv(self):
        sid = self._sel()
        snap = self._snap()
        if not sid or sid == "-":
            messagebox.showinfo("Info", "Run pipeline first.", parent=self)
            return
        con = None
        try:
            con = db_connect(self.paths.db_path)
            out = os.path.join(
                self.paths.exports_dir,
                "topnow_sel" + sid + "_" + utc_stamp_compact() + "_" + APP_VERSION + ".csv")
            export_selection_csv(con, int(sid), snap, "spot", out)
            messagebox.showinfo("Export", "Exported:\n" + out, parent=self)
        except Exception as e:
            messagebox.showerror("Error", str(e)[:1200], parent=self)
        finally:
            if con:
                con.close()


if __name__ == "__main__":
    App().mainloop()
