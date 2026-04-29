#!/usr/bin/env python3
"""
NyoSig Analysator — Web Dashboard (Streamlit)
Modern web UI for multi-layer market intelligence.

Install: pip install streamlit requests plotly pandas
Run:     streamlit run nyosig_dashboard.py
Requires: nyosig_api.py running on port 8000
"""
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import time
import json

API_URL = "http://localhost:8000"

st.set_page_config(
    page_title="NyoSig Analysator",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Dark theme CSS ---
st.markdown("""
<style>
    .stMetricLabel { font-size: 0.85rem; }
    .signal-buy { color: #00c853; font-weight: bold; }
    .signal-sell { color: #ff1744; font-weight: bold; }
    .signal-neutral { color: #9e9e9e; }
    div[data-testid="stMetricValue"] { font-size: 1.5rem; }
    .layer-ok { color: #00c853; }
    .layer-degraded { color: #ff9100; }
    .layer-error { color: #ff1744; }
</style>
""", unsafe_allow_html=True)


def api_get(path, params=None):
    try:
        r = requests.get(f"{API_URL}{path}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None

def api_post(path, json_data=None):
    try:
        r = requests.post(f"{API_URL}{path}", json=json_data, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


# =====================================================================
# SIDEBAR
# =====================================================================

with st.sidebar:
    st.image("https://img.icons8.com/fluency/48/analytics.png", width=40)
    st.title("NyoSig Analysator")
    st.caption("Multi-layer Market Intelligence")

    page = st.radio("Navigation", [
        "🏠 Dashboard",
        "🚀 Run Pipeline",
        "📊 Analysis & Predictions",
        "💼 Trade Plans",
        "📈 Portfolio",
        "👁 Watchlist",
        "🔔 Alerts",
        "🎯 Performance",
        "🤖 Automat",
        "📝 Paper Trading",
        "🤖 AI Report",
        "📊 Analytics",
        "⚙️ System",
    ], label_visibility="collapsed")

    st.divider()

    # Quick status
    health = api_get("/health")
    if health:
        status = health.get("status", "unknown")
        color = {"ok": "🟢", "warning": "🟡", "critical": "🔴"}.get(status, "⚪")
        st.caption(f"System: {color} {status.upper()}")
        checks = health.get("checks", {})
        st.caption(f"Runs: {checks.get('total_runs', 0)} | "
                   f"Alerts: {checks.get('pending_alerts', 0)}")


# =====================================================================
# PAGES
# =====================================================================

# --- DASHBOARD ---
if "Dashboard" in page:
    st.header("Market Intelligence Dashboard")

    # Top metrics
    runs = api_get("/runs", {"limit": 1})
    latest = runs[0] if runs else {}

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Latest Run", f"#{latest.get('run_id', '-')}")
    col2.metric("Status", latest.get("status", "-"))
    col3.metric("Scope", latest.get("scope", "-"))
    col4.metric("Version", latest.get("version", "-"))

    st.divider()

    # Summary if we have a run
    if latest.get("run_id"):
        summary = api_get(f"/runs/{latest['run_id']}/summary")
        if summary:
            col_l, col_r = st.columns([3, 2])

            with col_l:
                st.subheader("Signal Distribution")
                dist = summary.get("signal_distribution", {})
                if dist:
                    fig = go.Figure(data=[go.Bar(
                        x=list(dist.keys()),
                        y=list(dist.values()),
                        marker_color=["#00c853", "#4caf50", "#9e9e9e", "#ff5722", "#ff1744"],
                    )])
                    fig.update_layout(height=300, margin=dict(l=20, r=20, t=30, b=20),
                                      template="plotly_dark", title="Signal Distribution")
                    st.plotly_chart(fig, use_container_width=True)

                # Top picks
                picks = summary.get("top_picks", [])
                if picks:
                    st.subheader("Top Picks")
                    for p in picks[:5]:
                        signal = p.get("signal", "")
                        conf = p.get("confidence", 0)
                        color = "#00c853" if "buy" in signal else "#ff1744" if "sell" in signal else "#9e9e9e"
                        st.markdown(
                            f"**{p['symbol']}** — "
                            f"<span style='color:{color}'>{signal}</span> "
                            f"(conf: {conf:.1%})",
                            unsafe_allow_html=True)

            with col_r:
                st.subheader("Run Summary")
                st.metric("Candidates", summary.get("candidates", 0))
                st.metric("Predictions", summary.get("predictions_count", 0))
                st.metric("Trade Plans", summary.get("trade_plans_count", 0))
                st.metric("Avg Layers Scored", f"{summary.get('avg_layers_scored', 0):.1f}/10")

                # Warnings
                warnings = summary.get("warnings", [])
                if warnings:
                    st.warning("⚠️ " + " | ".join(warnings))

                # Position summary
                st.metric("Longs", summary.get("long_count", 0))
                st.metric("Shorts", summary.get("short_count", 0))
                st.metric("Total Position", f"{summary.get('total_position_pct', 0):.1f}%")

    # Cross-scope correlations
    if latest.get("run_id"):
        cors = api_get(f"/runs/{latest['run_id']}/correlations")
        if cors and not (len(cors) == 1 and "note" in cors[0]):
            st.subheader("Cross-Scope Correlations")
            for c in cors:
                aligned = "✅ ALIGNED" if c.get("currently_aligned") else "⚠️ DIVERGENT"
                st.markdown(
                    f"BTC ({c.get('btc_change', 0):+.1f}%) vs "
                    f"**{c['reference']}** ({c.get('ref_change', 0):+.1f}%) "
                    f"— expected: {c['expected_correlation']} — {aligned}")


# --- RUN PIPELINE ---
elif "Run Pipeline" in page:
    st.header("🚀 Run Data Pipeline")

    col1, col2 = st.columns([1, 2])
    with col1:
        scope = st.selectbox("Scope", ["crypto_spot", "forex_spot", "stocks_spot", "macro_dashboard"])
        vs_currency = st.text_input("vs_currency", "usd")
        coins_limit = st.number_input("coins_limit", 10, 500, 250)
        topnow_limit = st.number_input("TopNow candidates", 5, 100, 15)
        offline = st.checkbox("Offline mode")

        if st.button("▶ Run Snapshot + TopNow", type="primary", use_container_width=True):
            result = api_post("/pipeline/run", {
                "scope": scope, "vs_currency": vs_currency,
                "coins_limit": coins_limit, "topnow_limit": topnow_limit,
                "offline_mode": offline,
            })
            if result:
                st.success("Pipeline started! Watch the log below.")

    with col2:
        st.subheader("Pipeline Status")
        status_placeholder = st.empty()
        log_placeholder = st.empty()

        # Auto-refresh status
        if st.button("🔄 Refresh Status"):
            pass  # triggers rerun

        status = api_get("/pipeline/status")
        if status:
            state = status.get("status", "idle")
            color = {"idle": "gray", "running": "blue", "done": "green", "failed": "red",
                     "analysing": "blue"}.get(state, "gray")
            status_placeholder.markdown(f"### Status: :{color}[{state.upper()}]")

            # Show result
            result = status.get("result")
            if result and "error" not in result:
                st.success(f"✅ Run #{result.get('run_id')} — "
                          f"{result.get('candidates_n', 0)} candidates")

                # Offer analysis
                if st.button("📊 Run Analysis Now", type="primary"):
                    api_post("/analyse", {
                        "selection_id": result["selection_id"],
                        "run_id": result["run_id"],
                    })
                    st.success("Analysis started!")

            # Log
            logs = status.get("log", [])
            if logs:
                log_placeholder.code("\n".join(logs[-30:]), language="text")


# --- ANALYSIS & PREDICTIONS ---
elif "Predictions" in page:
    st.header("📊 Analysis & Predictions")

    runs = api_get("/runs", {"limit": 10})
    if runs:
        run_options = {f"Run #{r['run_id']} ({r['created'][:16]}) — {r['scope']}": r
                       for r in runs}
        selected = st.selectbox("Select run", list(run_options.keys()))
        run = run_options[selected]
        run_id = run["run_id"]

        tab1, tab2, tab3 = st.tabs(["Predictions", "Feature Vectors", "Candidates"])

        with tab1:
            preds = api_get(f"/runs/{run_id}/predictions")
            if preds:
                df = pd.DataFrame(preds)
                if not df.empty:
                    # Signal color mapping
                    def signal_color(sig):
                        if "strong_buy" in str(sig): return "background-color: #1b5e20; color: white"
                        if "buy" in str(sig): return "background-color: #2e7d32; color: white"
                        if "strong_sell" in str(sig): return "background-color: #b71c1c; color: white"
                        if "sell" in str(sig): return "background-color: #c62828; color: white"
                        return ""

                    display_cols = ["symbol", "signal", "confidence", "structural_avg", "rule"]
                    available = [c for c in display_cols if c in df.columns]
                    display_df = df[available] if available else df

                    if "signal" in display_df.columns:
                        st.dataframe(
                            display_df.style.applymap(signal_color, subset=["signal"]),
                            use_container_width=True, height=500)
                    else:
                        st.warning("Predictions were returned, but the signal column is missing. Showing raw prediction data.")
                        st.dataframe(display_df, use_container_width=True, height=500)

                    # Signal summary chart
                    if "signal" in df.columns:
                        fig = px.pie(df, names="signal", title="Signal Distribution",
                                     color_discrete_map={
                                         "strong_buy": "#00c853", "buy": "#4caf50",
                                         "neutral": "#9e9e9e",
                                         "sell": "#ff5722", "strong_sell": "#ff1744"})
                        fig.update_layout(template="plotly_dark", height=350)
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No predictions yet. Run pipeline + analysis first.")

        with tab2:
            features = api_get(f"/runs/{run_id}/features")
            if features:
                df_f = pd.DataFrame(features)
                if not df_f.empty and "features" in df_f.columns:
                    # Expand features dict into columns
                    feat_expanded = pd.json_normalize(df_f["features"])
                    feat_expanded.insert(0, "symbol", df_f["symbol"])
                    feat_expanded.insert(1, "norm_score", df_f["norm_score"])
                    st.dataframe(feat_expanded, use_container_width=True, height=500)

                    # Heatmap
                    if len(feat_expanded.columns) > 3:
                        numeric_cols = feat_expanded.select_dtypes(include="number").columns.tolist()
                        if "norm_score" in numeric_cols:
                            numeric_cols.remove("norm_score")
                        if numeric_cols:
                            heatmap_data = feat_expanded.set_index("symbol")[numeric_cols].fillna(0)
                            fig = px.imshow(heatmap_data, text_auto=".0f",
                                            title="Layer Score Heatmap",
                                            color_continuous_scale="RdYlGn",
                                            aspect="auto")
                            fig.update_layout(template="plotly_dark", height=400)
                            st.plotly_chart(fig, use_container_width=True)

        with tab3:
            # Get selection
            summary = api_get(f"/runs/{run_id}/summary")
            if summary and summary.get("candidates", 0) > 0:
                # Find selection_id
                with st.spinner("Loading candidates..."):
                    # Use latest selection for this run
                    sel_data = None
                    try:
                        r = requests.get(f"{API_URL}/runs", params={"limit": 20}, timeout=5)
                        all_runs = r.json()
                        # We need selection_id - get from pipeline state or runs
                    except Exception:
                        pass
                st.json(summary)


# --- TRADE PLANS ---
elif "Trade Plans" in page:
    st.header("💼 Trade Plans")

    runs = api_get("/runs", {"limit": 10})
    if runs:
        run_id = st.selectbox("Run", [r["run_id"] for r in runs],
                               format_func=lambda x: f"Run #{x}")
        plans = api_get(f"/runs/{run_id}/trade_plans")
        if plans:
            df = pd.DataFrame(plans)
            if not df.empty:
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Plans", len(df))
                col2.metric("Longs", len(df[df["direction"] == "long"]))
                col3.metric("Shorts", len(df[df["direction"] == "short"]))
                col4.metric("Holds", len(df[df["direction"] == "hold"]))

                # Table
                display = ["symbol", "direction", "entry_low", "entry_high",
                           "stop_loss", "target_1", "target_2", "position_pct", "risk_score"]
                available = [c for c in display if c in df.columns]
                st.dataframe(df[available], use_container_width=True, height=400)

                # Chart: position allocation
                active = df[df["direction"] != "hold"]
                if not active.empty and "position_pct" in active.columns:
                    fig = px.bar(active, x="symbol", y="position_pct",
                                 color="direction",
                                 color_discrete_map={"long": "#00c853", "short": "#ff1744"},
                                 title="Position Allocation")
                    fig.update_layout(template="plotly_dark", height=350)
                    st.plotly_chart(fig, use_container_width=True)

                # Backtest
                st.subheader("Trade Plan Backtest")
                bt = api_get(f"/runs/{run_id}/backtest")
                if bt:
                    df_bt = pd.DataFrame(bt)
                    if not df_bt.empty:
                        display_bt = ["symbol", "direction", "entry_mid",
                                      "current_price", "pnl_pct", "status"]
                        available_bt = [c for c in display_bt if c in df_bt.columns]
                        st.dataframe(df_bt[available_bt], use_container_width=True)
        else:
            st.info("No trade plans for this run. Run analysis first.")


# --- PORTFOLIO ---
elif "Portfolio" in page:
    st.header("📈 Portfolio Management")

    portfolio = api_get("/portfolio")
    if portfolio:
        risk = portfolio.get("risk_metrics", {})

        # Risk metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Open Positions", risk.get("positions_open", 0))
        col2.metric("Exposure", f"{risk.get('total_exposure_pct', 0):.1f}%")
        col3.metric("Portfolio Heat", f"{risk.get('portfolio_heat', 0):.1f}%")
        col4.metric("Win Rate", f"{risk.get('closed_win_rate', 0):.0f}%")
        col5.metric("At Risk", risk.get("positions_at_risk", 0))

        # Warnings
        warnings = risk.get("warnings", [])
        if warnings:
            for w in warnings:
                st.warning(f"⚠️ {w}")

        # Open positions
        positions = portfolio.get("open_positions", [])
        if positions:
            st.subheader("Open Positions")
            df_pos = pd.DataFrame(positions)
            display = ["symbol", "direction", "entry_price", "size",
                        "current_price", "unrealised_pnl", "stop_loss", "target"]
            available = [c for c in display if c in df_pos.columns]
            st.dataframe(df_pos[available], use_container_width=True)

        # Total P&L
        st.metric("Total Unrealised P&L",
                   f"{portfolio.get('total_unrealised_pnl', 0):+.2f}%")

        # Closed
        closed = portfolio.get("recent_closed", [])
        if closed:
            st.subheader("Recent Closed")
            df_cl = pd.DataFrame(closed)
            st.dataframe(df_cl[["symbol", "direction", "pnl_pct", "closed"]].head(10),
                          use_container_width=True)
    else:
        st.info("No portfolio data. Open positions first.")


# --- WATCHLIST ---
elif "Watchlist" in page:
    st.header("👁 Watchlist")

    watchlist = api_get("/watchlist")
    if watchlist:
        df = pd.DataFrame(watchlist)
        if not df.empty:
            st.dataframe(df[["symbol", "stage", "prediction", "plan",
                              "current_price", "pnl_pct", "active_alerts"]].head(30),
                          use_container_width=True)

            # Signal history for selected symbol
            symbol = st.selectbox("Signal History for:", df["symbol"].tolist())
            if symbol:
                history = api_get(f"/watchlist/{symbol}/history")
                if history:
                    df_h = pd.DataFrame(history)
                    if not df_h.empty:
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=df_h["created_utc"], y=df_h["confidence"],
                            mode="lines+markers",
                            marker=dict(
                                color=df_h["signal"].map({
                                    "strong_buy": "#00c853", "buy": "#4caf50",
                                    "neutral": "#9e9e9e",
                                    "sell": "#ff5722", "strong_sell": "#ff1744"
                                }),
                                size=10
                            ),
                            text=df_h["signal"],
                            name="Confidence"
                        ))
                        fig.update_layout(title=f"Signal Evolution: {symbol}",
                                          template="plotly_dark", height=350)
                        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Watchlist empty. Run pipeline and promote candidates first.")


# --- ALERTS ---
elif "Alerts" in page:
    st.header("🔔 Alerts")

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Check Alerts Now"):
            runs = api_get("/runs", {"limit": 1})
            if runs:
                result = api_post(f"/alerts/{runs[0]['run_id']}/check")
                if result:
                    st.success(f"Found {result.get('new_alerts', 0)} new alerts")

    alerts = api_get("/alerts")
    if alerts:
        df = pd.DataFrame(alerts)
        if not df.empty:
            # Color by severity
            severity_colors = {"critical": "🔴", "success": "🟢",
                               "warning": "🟡", "info": "⚪"}
            df["sev"] = df["severity"].map(severity_colors)
            display = ["sev", "symbol", "type", "message", "price", "created_utc"]
            available = [c for c in display if c in df.columns]
            st.dataframe(df[available], use_container_width=True, height=400)
    else:
        st.info("No alerts. Run pipeline and check trade plans first.")


# --- PERFORMANCE ---
elif "Performance" in page:
    st.header("🎯 Prediction Performance")

    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("📊 Evaluate Now", type="primary"):
            result = api_post("/performance/evaluate")
            if result:
                st.success(f"Evaluated: {result.get('evaluated', 0)} predictions\n"
                          f"Hit rate: {result.get('hit_rate_pct', 0):.1f}%")

    perf = api_get("/performance")
    if perf:
        df = pd.DataFrame(perf)
        if not df.empty:
            # Summary metrics
            correct = len(df[df["outcome"] == "correct"])
            incorrect = len(df[df["outcome"] == "incorrect"])
            total = correct + incorrect
            hit_rate = correct / total * 100 if total > 0 else 0

            col1, col2, col3 = st.columns(3)
            col1.metric("Hit Rate", f"{hit_rate:.1f}%")
            col2.metric("Correct", correct)
            col3.metric("Total Evaluated", total)

            # Performance table
            display = ["symbol", "signal", "confidence", "price_at_pred",
                        "price_at_eval", "pnl_pct", "outcome"]
            available = [c for c in display if c in df.columns]
            st.dataframe(df[available], use_container_width=True, height=400)

            # Outcome pie
            if "outcome" in df.columns:
                fig = px.pie(df, names="outcome",
                             color_discrete_map={
                                 "correct": "#00c853", "incorrect": "#ff1744",
                                 "unknown": "#9e9e9e"},
                             title="Prediction Outcomes")
                fig.update_layout(template="plotly_dark", height=350)
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No performance data. Run evaluate first.")






# --- AUTOMAT ---
elif "Automat" in page:
    st.header("🤖 Automation Engine")
    st.caption("Configure and run automated pipeline cycles with scheduling")

    # Load current config
    cfg = api_get("/automat/config") or {}
    status = api_get("/automat/status") or {"running": False}

    # STATUS BAR
    if status.get("running"):
        st.success(f"🟢 AUTOMAT RUNNING — Cycle #{status.get('run_count', 0)} | "
                   f"Interval: {status.get('interval', '?')} | Scope: {status.get('scope', '?')}")
        if st.button("⏹️ STOP AUTOMAT", type="primary", use_container_width=True):
            api_post("/automat/stop")
            st.rerun()
    else:
        st.info("⚪ Automat is not running")

    tab_setup, tab_select, tab_schedule, tab_history = st.tabs([
        "⚙️ Pipeline Setup", "📋 Candidate Selection", "⏰ Schedule & Run", "📊 History"])

    # ==================== TAB 1: PIPELINE SETUP ====================
    with tab_setup:
        st.subheader("Pipeline Parameters")
        col1, col2 = st.columns(2)
        with col1:
            a_scope = st.selectbox("Scope", ["crypto_spot", "forex_spot", "stocks_spot", "macro_dashboard"],
                                     index=["crypto_spot", "forex_spot", "stocks_spot", "macro_dashboard"].index(
                                         cfg.get("scope", "crypto_spot")),
                                     key="a_scope")
            a_vs = st.text_input("vs_currency", cfg.get("vs_currency", "usd"), key="a_vs")
            a_coins = st.number_input("coins_limit", 10, 500, cfg.get("coins_limit", 250), key="a_coins")
        with col2:
            a_topn = st.number_input("topnow_limit", 5, 100, cfg.get("topnow_limit", 15), key="a_topn")
            a_order = st.text_input("order", cfg.get("order", "market_cap_desc"), key="a_order")
            a_offline = st.checkbox("offline_mode", cfg.get("offline_mode", False), key="a_offline")

        st.divider()
        st.subheader("Operations to run each cycle")
        col1, col2, col3, col4 = st.columns(4)
        a_analysis = col1.checkbox("Run Analysis", cfg.get("run_analysis", True), key="a_analysis")
        a_paper = col2.checkbox("Paper Trading", cfg.get("run_paper_trading", True), key="a_paper")
        a_ai = col3.checkbox("AI Report", cfg.get("run_ai_report", False), key="a_ai")
        a_alerts = col4.checkbox("Alert Check", cfg.get("run_alerts_check", True), key="a_alerts")

        # Layer settings
        st.divider()
        st.subheader("Layer Settings")
        st.caption("Configure individual analysis layers (shown when Analysis is enabled)")
        layers = api_get("/layers") or []
        layer_configs = cfg.get("layer_configs", {})
        layers_enabled = cfg.get("layers_enabled", {})

        if layers and a_analysis:
            layer_cols = st.columns(2)
            for i, layer in enumerate(layers):
                sk = layer["scope_key"]
                with layer_cols[i % 2]:
                    enabled = st.checkbox(f"{layer['name']}", 
                                          layers_enabled.get(sk, True),
                                          key=f"layer_{sk}")
                    layers_enabled[sk] = enabled
                    lc = layer.get("configurable", {})
                    if lc and enabled:
                        with st.expander(f"Settings: {layer['name']}"):
                            for param, default in lc.items():
                                val = layer_configs.get(sk, {}).get(param, default)
                                layer_configs.setdefault(sk, {})[param] = st.text_input(
                                    param, str(val), key=f"lc_{sk}_{param}")

    # ==================== TAB 2: CANDIDATE SELECTION ====================
    with tab_select:
        st.subheader("Candidate Selection Mode")

        sel_mode = st.radio("Selection method", [
            "🔝 Top N (automatic — select best N by score)",
            "🔝 Top N by column (sort by any column, take top N)",
            "☑️ Manual (select individual symbols)",
        ], index={"top_n": 0, "top_n_column": 1, "manual": 2}.get(
            cfg.get("selection_mode", "top_n"), 0),
            key="sel_mode")

        if "Top N (automatic" in sel_mode:
            sel_mode_key = "top_n"
            a_sel_n = st.number_input("How many top candidates?", 3, 100,
                                        cfg.get("top_n", 15), key="sel_n")
            st.caption("Selects top N from the pipeline results sorted by composite score (descending)")

        elif "Top N by column" in sel_mode:
            sel_mode_key = "top_n"
            st.markdown("**Step 1:** Choose column to sort by")
            sort_options = ["composite", "price", "mcap", "vol24", "chg24", "base_score", "mkt_rank"]
            a_sort_col = st.selectbox("Sort by column", sort_options,
                                       index=sort_options.index(cfg.get("sort_column", "composite"))
                                       if cfg.get("sort_column", "composite") in sort_options else 0,
                                       key="sort_col")
            a_sort_desc = st.checkbox("Descending (highest first)", cfg.get("sort_descending", True),
                                       key="sort_desc")
            st.markdown("**Step 2:** How many to select")
            a_sel_n = st.number_input("Select top N after sorting", 3, 100,
                                        cfg.get("top_n", 15), key="sel_n2")
        else:
            sel_mode_key = "manual"
            a_sel_n = cfg.get("top_n", 15)

        # Show current candidates if available
        st.divider()
        st.subheader("Current Candidates Preview")
        candidates = api_get("/automat/candidates")
        if candidates:
            df_cand = pd.DataFrame(candidates)
            if not df_cand.empty:
                # Apply sorting if top_n_column mode
                if "by column" in sel_mode and 'a_sort_col' in dir():
                    col = a_sort_col if a_sort_col in df_cand.columns else "composite"
                    asc = not a_sort_desc if 'a_sort_desc' in dir() else False
                    df_cand = df_cand.sort_values(col, ascending=asc).reset_index(drop=True)

                if sel_mode_key == "manual":
                    st.caption("Check the symbols you want to include:")
                    manual_syms = []
                    # Create checkbox grid
                    cols_grid = st.columns(5)
                    for i, row in df_cand.iterrows():
                        with cols_grid[i % 5]:
                            checked = st.checkbox(
                                f"{row['symbol']} ({row.get('composite', 0):.0f})",
                                value=row['symbol'] in cfg.get("manual_symbols", []),
                                key=f"man_{row['symbol']}")
                            if checked:
                                manual_syms.append(row['symbol'])
                    st.caption(f"Selected: {len(manual_syms)} symbols")
                else:
                    manual_syms = cfg.get("manual_symbols", [])

                # Display table with sort headers
                st.dataframe(
                    df_cand.head(a_sel_n if sel_mode_key == "top_n" else len(df_cand)),
                    use_container_width=True, height=400)

                if sel_mode_key == "top_n":
                    selected_syms = df_cand.head(a_sel_n)["symbol"].tolist()
                    st.success(f"Will select: {', '.join(selected_syms)}")
        else:
            st.info("No candidates yet. Run pipeline first to see candidates here.")
            manual_syms = cfg.get("manual_symbols", [])

    # ==================== TAB 3: SCHEDULE & RUN ====================
    with tab_schedule:
        st.subheader("Schedule Configuration")

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Interval**")
            interval_options = {
                "Every hour": "1h",
                "Every 12 hours": "12h",
                "Once daily": "daily",
                "Once weekly": "weekly",
                "Custom interval": "custom",
            }
            interval_labels = list(interval_options.keys())
            current_interval = cfg.get("interval_mode", "daily")
            current_idx = list(interval_options.values()).index(current_interval) \
                if current_interval in interval_options.values() else 2
            a_interval = st.radio("Run frequency", interval_labels,
                                    index=current_idx, key="a_interval")
            a_interval_key = interval_options[a_interval]

            if a_interval_key == "custom":
                a_custom_h = st.number_input("Custom interval (hours)",
                                               0.5, 168.0,
                                               cfg.get("custom_interval_hours", 24.0),
                                               step=0.5, key="a_custom_h")
            else:
                a_custom_h = cfg.get("custom_interval_hours", 24.0)

        with col2:
            st.markdown("**Start time (UTC)**")
            a_start = st.text_input("Start at (HH:MM UTC, or 'now')",
                                      cfg.get("start_time", "08:00"), key="a_start")
            st.caption("If set to a future time, automation waits until that time to start. "
                       "Set to 'now' to start immediately.")

            st.divider()
            st.markdown("**Server mode**")
            a_server = st.checkbox("Enable server mode (headless, for VPS deployment)",
                                     cfg.get("server_mode", False), key="a_server")
            if a_server:
                st.code("python nyosig_automator.py", language="bash")
                st.caption("Run this command on your server. It will start the automation "
                           "engine headlessly with the saved configuration.")

        # ---- RUN AUTOMAT button ----
        st.divider()
        st.subheader("🚀 Launch")

        # Build config from all inputs
        new_config = {
            "scope": a_scope if 'a_scope' in dir() else cfg.get("scope", "crypto_spot"),
            "vs_currency": a_vs if 'a_vs' in dir() else cfg.get("vs_currency", "usd"),
            "coins_limit": a_coins if 'a_coins' in dir() else cfg.get("coins_limit", 250),
            "topnow_limit": a_topn if 'a_topn' in dir() else cfg.get("topnow_limit", 15),
            "order": a_order if 'a_order' in dir() else cfg.get("order", "market_cap_desc"),
            "offline_mode": a_offline if 'a_offline' in dir() else cfg.get("offline_mode", False),
            "selection_mode": sel_mode_key if 'sel_mode_key' in dir() else cfg.get("selection_mode", "top_n"),
            "top_n": a_sel_n if 'a_sel_n' in dir() else cfg.get("top_n", 15),
            "sort_column": a_sort_col if 'a_sort_col' in dir() else cfg.get("sort_column", "composite"),
            "sort_descending": a_sort_desc if 'a_sort_desc' in dir() else cfg.get("sort_descending", True),
            "manual_symbols": manual_syms if 'manual_syms' in dir() else cfg.get("manual_symbols", []),
            "layers_enabled": layers_enabled if 'layers_enabled' in dir() else cfg.get("layers_enabled", {}),
            "layer_configs": layer_configs if 'layer_configs' in dir() else cfg.get("layer_configs", {}),
            "interval_mode": a_interval_key if 'a_interval_key' in dir() else cfg.get("interval_mode", "daily"),
            "custom_interval_hours": a_custom_h if 'a_custom_h' in dir() else cfg.get("custom_interval_hours", 24),
            "start_time": a_start if 'a_start' in dir() else cfg.get("start_time", "08:00"),
            "run_analysis": a_analysis if 'a_analysis' in dir() else cfg.get("run_analysis", True),
            "run_paper_trading": a_paper if 'a_paper' in dir() else cfg.get("run_paper_trading", True),
            "run_ai_report": a_ai if 'a_ai' in dir() else cfg.get("run_ai_report", False),
            "run_alerts_check": a_alerts if 'a_alerts' in dir() else cfg.get("run_alerts_check", True),
            "server_mode": a_server if 'a_server' in dir() else cfg.get("server_mode", False),
        }

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("💾 Save Configuration", use_container_width=True):
                result = api_post("/automat/config", new_config)
                if result and result.get("status") == "saved":
                    st.success("✅ Configuration saved!")
        with col2:
            if not status.get("running"):
                if st.button("🚀 RUN AUTOMAT", type="primary", use_container_width=True):
                    # Save config first, then start
                    api_post("/automat/config", new_config)
                    result = api_post("/automat/start")
                    if result and result.get("status") == "started":
                        st.success("🚀 Automation started!")
                        st.rerun()
            else:
                if st.button("⏹️ Stop Automation", type="secondary", use_container_width=True):
                    api_post("/automat/stop")
                    st.rerun()
        with col3:
            if st.button("▶️ Run Single Cycle Now", use_container_width=True):
                api_post("/automat/config", new_config)
                # Just start and let it run one cycle, then check
                st.info("Starting single cycle... check History tab for results.")
                api_post("/automat/start")

        # Summary of what will happen
        st.divider()
        interval_text = {"1h": "every hour", "12h": "every 12 hours",
                         "daily": "once daily", "weekly": "once weekly",
                         "custom": f"every {a_custom_h if 'a_custom_h' in dir() else 24}h"
                         }.get(new_config.get("interval_mode", "daily"), "daily")
        st.markdown(f"""
**Configuration summary:**
- **Scope:** {new_config.get('scope')} | **Candidates:** {new_config.get('topnow_limit')}
- **Selection:** {new_config.get('selection_mode')} (top {new_config.get('top_n')})
- **Schedule:** {interval_text} starting at {new_config.get('start_time')} UTC
- **Operations:** {'Analysis' if new_config.get('run_analysis') else '~~Analysis~~'} → 
  {'Paper Trade' if new_config.get('run_paper_trading') else '~~Paper~~'} → 
  {'AI Report' if new_config.get('run_ai_report') else '~~AI~~'} → 
  {'Alerts' if new_config.get('run_alerts_check') else '~~Alerts~~'}
""")

    # ==================== TAB 4: HISTORY ====================
    with tab_history:
        st.subheader("Automation Run History")

        history = api_get("/automat/history")
        if history:
            for h in history:
                cycle = h.get("cycle", "?")
                ts = h.get("timestamp", "")[:19]
                status_h = h.get("status", "?")
                icon = "✅" if status_h == "completed" else "❌" if status_h == "failed" else "⏳"

                with st.expander(f"{icon} Cycle #{cycle} — {ts} — {status_h}"):
                    steps = h.get("steps", {})
                    for step_name, step_data in steps.items():
                        if isinstance(step_data, dict):
                            if step_data.get("skipped"):
                                st.caption(f"⏭️ {step_name}: skipped")
                            elif step_data.get("error"):
                                st.error(f"❌ {step_name}: {step_data['error']}")
                            else:
                                st.success(f"✅ {step_name}: {json.dumps(step_data)}")
                    if h.get("error"):
                        st.error(h["error"])
        else:
            st.info("No automation history yet. Start the automation to see results here.")

        # Status refresh
        if st.button("🔄 Refresh"):
            st.rerun()


# --- PAPER TRADING ---
elif "Paper Trading" in page:
    st.header("📝 Paper Trading Track Record")
    st.caption("Auditable prediction log with SHA256 hashes — proves predictions were made before outcomes")

    tab1, tab2, tab3 = st.tabs(["Track Record", "Daily Reports", "All Predictions"])

    with tab1:
        stats = api_get("/paper/stats")
        if stats and stats.get("total_1d", 0) > 0:
            # Headline metrics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("1-Day Hit Rate", f"{stats.get('hit_rate_1d', 0):.0f}%",
                         help="Percentage of predictions correct after 24 hours")
            col2.metric("7-Day Hit Rate", f"{stats.get('hit_rate_7d', 0):.0f}%")
            col3.metric("Avg P&L (1d)", f"{stats.get('avg_pnl_1d', 0):+.1f}%")
            col4.metric("Total Evaluated", stats.get("total_1d", 0))

            st.divider()

            col1, col2 = st.columns(2)
            col1.metric("Open Predictions", stats.get("open_predictions", 0))
            col2.metric("Total Recorded", stats.get("total_predictions", 0))

            # Hit rate chart across periods
            periods = ["1d", "7d", "30d"]
            rates = [stats.get(f"hit_rate_{p}", 0) for p in periods]
            totals = [stats.get(f"total_{p}", 0) for p in periods]
            if any(r > 0 for r in rates):
                import plotly.graph_objects as go
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=periods, y=rates,
                    text=[f"{r:.0f}% ({t})" for r, t in zip(rates, totals)],
                    textposition="auto",
                    marker_color=["#4caf50" if r >= 55 else "#ff9800" if r >= 45 else "#f44336"
                                  for r in rates]
                ))
                fig.update_layout(title="Hit Rate by Evaluation Period",
                                  yaxis_title="Hit Rate %", yaxis_range=[0, 100],
                                  template="plotly_dark", height=350)
                fig.add_hline(y=55, line_dash="dash", line_color="white",
                              annotation_text="55% (profitable threshold)")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No evaluations yet. Run pipeline + paper trade + wait 24h + evaluate.")

        # Actions
        st.divider()
        col1, col2, col3 = st.columns(3)
        with col1:
            runs = api_get("/runs", {"limit": 5})
            if runs:
                run_id = st.selectbox("Run for paper trading", [r["run_id"] for r in runs],
                                       format_func=lambda x: f"Run #{x}")
                if st.button("📝 Record Predictions", type="primary"):
                    result = api_post(f"/paper/run/{run_id}")
                    if result:
                        st.success(f"Recorded: {result.get('predictions_recorded', 0)} predictions\n"
                                   f"Evaluated: {result.get('outcomes_evaluated', 0)} outcomes")
        with col2:
            if st.button("📊 Evaluate Outcomes"):
                result = api_post("/paper/evaluate")
                if result:
                    st.success(f"Evaluated: {result.get('evaluated', 0)} outcomes")
                    st.rerun()
        with col3:
            st.caption("Predictions are SHA256-hashed at creation time.\n"
                       "Hash proves the prediction existed before the outcome was known.")

    with tab2:
        reports = api_get("/paper/reports")
        if reports:
            for report in reports[:10]:
                with st.expander(f"📅 {report['date']} — {report['predictions']} predictions"):
                    st.markdown(report["report"])
                    st.divider()
                    st.code(report.get("tweet", ""), language="text")
                    st.caption("Copy the tweet text above to post on X/Twitter")
        else:
            st.info("No daily reports yet. Record predictions first.")

    with tab3:
        preds = api_get("/paper/predictions", {"limit": 50})
        if preds:
            df = pd.DataFrame(preds)
            if not df.empty:
                st.dataframe(df, use_container_width=True, height=500)

                # Filter by status
                open_count = len(df[df["status"] == "open"])
                closed_count = len(df[df["status"] == "closed"])
                st.caption(f"Open: {open_count} | Closed: {closed_count}")
        else:
            st.info("No paper predictions recorded yet.")


# --- AI REPORT ---
elif "AI Report" in page:
    st.header("🤖 AI Market Intelligence Report")
    st.caption("Powered by Claude / GPT / Gemini — interprets layer conflicts, identifies risks")

    runs = api_get("/runs", {"limit": 10})
    if runs:
        run_id = st.selectbox("Select run", [r["run_id"] for r in runs],
                               format_func=lambda x: f"Run #{x}")

        col1, col2 = st.columns([1, 1])
        with col1:
            multi = st.checkbox("Multi-AI (use all available providers)", value=False)
        with col2:
            st.caption("Set API keys as environment variables:\n"
                       "ANTHROPIC_API_KEY, OPENAI_API_KEY, GEMINI_API_KEY")

        if st.button("📝 Generate Report", type="primary", use_container_width=True):
            with st.spinner("AI is analyzing 13 layers of market data..."):
                result = api_get(f"/ai/report/{run_id}",
                                 {"multi": str(multi).lower()})

            if result:
                if "error" in result and result["error"]:
                    st.warning(f"⚠️ {result['error']}")

                # Single report
                if "report" in result:
                    st.markdown(result["report"])
                    st.divider()
                    col1, col2, col3 = st.columns(3)
                    col1.caption(f"Model: {result.get('model', '?')}")
                    col2.caption(f"Cost: ${result.get('cost_estimate_usd', 0):.4f}")
                    col3.caption(f"Time: {result.get('timestamp', '')[:19]}")

                # Multi-AI report
                elif "ensemble_report" in result:
                    providers = result.get("providers_used", [])
                    st.success(f"Reports from: {', '.join(providers)}")

                    # Show individual reports in tabs
                    individual = result.get("individual", {})
                    if individual:
                        tabs = st.tabs([p.upper() for p in individual.keys()])
                        for tab, (provider, data) in zip(tabs, individual.items()):
                            with tab:
                                if "report" in data:
                                    st.markdown(data["report"])
                                if "error" in data:
                                    st.error(data["error"])
                                st.caption(f"Model: {data.get('model', '?')} | "
                                           f"Cost: ${data.get('cost_estimate_usd', 0):.4f}")

                    st.divider()
                    st.metric("Total Cost", f"${result.get('total_cost_usd', 0):.4f}")

                # Save report
                if st.button("💾 Save Report"):
                    report_text = result.get("report") or result.get("ensemble_report", "")
                    st.download_button("Download as .md", report_text,
                                       f"nyosig_report_{run_id}.md", "text/markdown")
    else:
        st.info("No runs yet. Run pipeline first.")



# --- ANALYTICS ---
elif "Analytics" in page:
    st.header("📊 Analytics & Performance Log")
    st.caption("Separate database tracking every operation with timing — for system tuning")

    tab_overview, tab_runs, tab_layers, tab_api, tab_daily = st.tabs([
        "Overview", "Run Profiles", "Layer Timing", "API Calls", "Daily Trends"])

    # ---- OVERVIEW ----
    with tab_overview:
        overview = api_get("/analytics/overview")
        if overview:
            runs = overview.get("runs", {})
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Runs", runs.get("total_completed", 0))
            col2.metric("Avg Duration", f"{runs.get('avg_duration_s', 0):.0f}s")
            col3.metric("Fastest", f"{runs.get('min_duration_s', 0):.0f}s")
            col4.metric("Slowest", f"{runs.get('max_duration_s', 0):.0f}s")

            st.metric("Error Rate", f"{overview.get('error_rate', 0):.1f}%")
            st.metric("Total Operations Logged", overview.get("total_operations", 0))

            # Layer performance bar chart
            layers = overview.get("layers", [])
            if layers:
                st.subheader("Layer Performance (avg ms)")
                df_l = pd.DataFrame(layers)
                fig = px.bar(df_l, x="layer", y="avg_ms",
                             color="avg_ms",
                             color_continuous_scale=["#00c853", "#ffeb3b", "#ff1744"],
                             title="Average Layer Duration (ms)")
                fig.update_layout(template="plotly_dark", height=400)
                st.plotly_chart(fig, use_container_width=True)

            # Provider stats
            providers = overview.get("providers", [])
            if providers:
                st.subheader("API Provider Stats")
                df_p = pd.DataFrame(providers)
                st.dataframe(df_p, use_container_width=True)

            db_info = api_get("/analytics/db_size")
            if db_info:
                st.caption(f"Analytics DB size: {db_info.get('size_mb', 0):.2f} MB")
        else:
            st.info("No analytics data yet. Run pipeline to start collecting metrics.")

    # ---- RUN PROFILES ----
    with tab_runs:
        profiles = api_get("/analytics/runs", {"limit": 30})
        if profiles:
            df = pd.DataFrame(profiles)
            if not df.empty:
                st.dataframe(df[["run_id", "scope", "started", "duration_s",
                                 "status", "api_calls", "layers", "errors",
                                 "candidates"]],
                             use_container_width=True, height=400)

                # Duration trend
                if len(df) > 2 and "duration_s" in df.columns:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df["started"], y=df["duration_s"],
                        mode="lines+markers",
                        marker=dict(
                            color=df["duration_s"],
                            colorscale="RdYlGn_r", size=8),
                        name="Duration"))
                    fig.update_layout(title="Run Duration Trend",
                                      yaxis_title="seconds",
                                      template="plotly_dark", height=350)
                    st.plotly_chart(fig, use_container_width=True)

                # Drill into a run
                run_id = st.selectbox("Drill into run", df["run_id"].tolist(),
                                       key="analytics_run_drill")
                if run_id:
                    ops = api_get(f"/analytics/runs/{run_id}/operations")
                    if ops:
                        st.subheader(f"Operations for Run #{run_id}")
                        df_ops = pd.DataFrame(ops)
                        if not df_ops.empty:
                            # Color by status
                            st.dataframe(df_ops[["operation", "category", "duration_ms",
                                                  "status", "api_calls", "items", "error"]],
                                         use_container_width=True, height=350)

                            # Waterfall / timeline
                            if "duration_ms" in df_ops.columns:
                                fig = px.bar(df_ops, x="operation", y="duration_ms",
                                             color="status",
                                             color_discrete_map={
                                                 "ok": "#00c853", "error": "#ff1744",
                                                 "running": "#2196f3"},
                                             title="Operation Timeline (ms)")
                                fig.update_layout(template="plotly_dark", height=350)
                                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No run profiles yet.")

    # ---- LAYER TIMING ----
    with tab_layers:
        profiles = api_get("/analytics/runs", {"limit": 10})
        if profiles:
            run_id = st.selectbox("Select run", [p["run_id"] for p in profiles],
                                   format_func=lambda x: f"Run #{x}",
                                   key="layer_timing_run")
            layers = api_get(f"/analytics/runs/{run_id}/layers")
            if layers:
                df_lt = pd.DataFrame(layers)
                if not df_lt.empty:
                    st.dataframe(df_lt[["layer", "duration_ms", "status",
                                         "symbols_in", "symbols_scored",
                                         "api_calls", "cache_hits",
                                         "rate_waits", "wait_ms", "error"]],
                                 use_container_width=True)

                    # Stacked bar: computation vs waiting
                    if "duration_ms" in df_lt.columns and "wait_ms" in df_lt.columns:
                        df_lt["compute_ms"] = df_lt["duration_ms"] - df_lt["wait_ms"].fillna(0)
                        fig = go.Figure()
                        fig.add_trace(go.Bar(
                            x=df_lt["layer"], y=df_lt["compute_ms"],
                            name="Compute", marker_color="#2196f3"))
                        fig.add_trace(go.Bar(
                            x=df_lt["layer"], y=df_lt["wait_ms"],
                            name="Rate-limit wait", marker_color="#ff9800"))
                        fig.update_layout(barmode="stack",
                                          title="Layer Time: Compute vs Wait",
                                          yaxis_title="ms",
                                          template="plotly_dark", height=400)
                        st.plotly_chart(fig, use_container_width=True)

                    # Cache efficiency
                    total_calls = df_lt["api_calls"].sum()
                    total_cache = df_lt["cache_hits"].sum()
                    if total_calls > 0:
                        hit_rate = total_cache / total_calls * 100
                        st.metric("Cache Hit Rate", f"{hit_rate:.0f}%",
                                  help=f"{total_cache} cached / {total_calls} total API calls")
            else:
                st.info("No layer timing data for this run.")
        else:
            st.info("No runs available.")

    # ---- API CALLS ----
    with tab_api:
        profiles = api_get("/analytics/runs", {"limit": 10})
        if profiles:
            run_id = st.selectbox("Select run", [p["run_id"] for p in profiles],
                                   format_func=lambda x: f"Run #{x}",
                                   key="api_calls_run")
            calls = api_get(f"/analytics/runs/{run_id}/api_calls")
            if calls:
                df_api = pd.DataFrame(calls)
                if not df_api.empty:
                    # Summary by provider
                    st.subheader("By Provider")
                    provider_stats = df_api.groupby("provider").agg({
                        "duration_ms": ["mean", "max", "count"],
                        "cached": "sum",
                        "rate_limited": "sum",
                    }).round(0)
                    provider_stats.columns = ["avg_ms", "max_ms", "calls",
                                              "cached", "rate_limited"]
                    st.dataframe(provider_stats, use_container_width=True)

                    # Full call log
                    st.subheader("All API Calls")
                    st.dataframe(df_api[["provider", "endpoint", "duration_ms",
                                          "http_status", "bytes", "cached",
                                          "rate_limited", "wait_ms", "error"]],
                                 use_container_width=True, height=400)

                    # Duration distribution
                    fig = px.histogram(df_api, x="duration_ms", color="provider",
                                       nbins=30, title="API Call Duration Distribution")
                    fig.update_layout(template="plotly_dark", height=350)
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No API calls logged for this run.")

    # ---- DAILY TRENDS ----
    with tab_daily:
        daily = api_get("/analytics/daily")
        if daily:
            df_d = pd.DataFrame(daily)
            if not df_d.empty:
                st.dataframe(df_d, use_container_width=True)

                if len(df_d) > 1:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df_d["date"], y=df_d["avg_duration_s"],
                        mode="lines+markers", name="Avg Duration (s)"))
                    fig.add_trace(go.Bar(
                        x=df_d["date"], y=df_d["api_calls"],
                        name="API Calls", opacity=0.3))
                    fig.update_layout(title="Daily Performance Trends",
                                      template="plotly_dark", height=400)
                    st.plotly_chart(fig, use_container_width=True)

                    # Error + rate limit trend
                    fig2 = go.Figure()
                    fig2.add_trace(go.Bar(
                        x=df_d["date"], y=df_d["errors"],
                        name="Errors", marker_color="#ff1744"))
                    fig2.add_trace(go.Bar(
                        x=df_d["date"], y=df_d["rate_limits"],
                        name="Rate Limits", marker_color="#ff9800"))
                    fig2.update_layout(title="Errors & Rate Limits",
                                       barmode="stack",
                                       template="plotly_dark", height=300)
                    st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No daily summaries yet. Run pipeline to start collecting data.")


# --- SYSTEM ---
elif "System" in page:
    st.header("⚙️ System Administration")

    tab1, tab2, tab3, tab4 = st.tabs(["🔑 API Keys", "Health", "Layers", "Run History"])

    with tab1:
        st.subheader("AI & Data API Keys")
        st.caption("Keys are stored locally in `config/api_keys.json`. "
                   "They never leave your machine except when calling the respective API.")

        keys_data = api_get("/keys")
        if keys_data:
            for ki in keys_data:
                provider = ki["provider"]
                label = ki["label"]
                is_set = ki["is_set"]
                preview = ki.get("preview", "")
                prefix = ki.get("expected_prefix", "")

                st.divider()
                col_name, col_status, col_actions = st.columns([2, 2, 3])

                with col_name:
                    if is_set:
                        st.markdown(f"### 🟢 {label}")
                    else:
                        st.markdown(f"### ⚪ {label}")

                with col_status:
                    if is_set:
                        st.code(preview, language="text")
                        st.caption(f"env: `{ki['env_var']}`")
                    else:
                        st.caption("Not configured")
                        st.caption(f"Expected prefix: `{prefix}`")

                with col_actions:
                    # Set / Edit key
                    with st.popover(f"{'✏️ Edit' if is_set else '➕ Set'} Key", use_container_width=True):
                        new_key = st.text_input(
                            f"Paste your {label} key:",
                            type="password",
                            key=f"key_input_{provider}",
                            placeholder=f"{prefix}...")
                        if st.button(f"💾 Save", key=f"save_{provider}", use_container_width=True):
                            if new_key.strip():
                                result = api_post(f"/keys/{provider}", {"key": new_key.strip()})
                                if result and result.get("status") == "saved":
                                    st.success(f"✅ {label} key saved!")
                                    st.rerun()
                                else:
                                    st.error("Failed to save key")
                            else:
                                st.warning("Key cannot be empty")

                    # Delete key
                    if is_set:
                        if st.button(f"🗑️ Delete", key=f"del_{provider}", use_container_width=True):
                            r = requests.delete(f"{API_URL}/keys/{provider}", timeout=5)
                            if r.status_code == 200:
                                st.success(f"🗑️ {label} key deleted")
                                st.rerun()

                    # Test key
                    if is_set:
                        if st.button(f"🧪 Test", key=f"test_{provider}", use_container_width=True):
                            result = api_post(f"/keys/{provider}/test")
                            if result:
                                if result.get("status") == "ok":
                                    st.success(f"✅ {result.get('message', 'Working')}")
                                else:
                                    st.error(f"❌ {result.get('message', 'Failed')}")

            st.divider()
            st.caption("**How to get keys:**")
            st.markdown("""
| Provider | Where to get key | Free tier |
|----------|-----------------|-----------|
| **Claude** | [console.anthropic.com](https://console.anthropic.com) | $5 free credit |
| **GPT** | [platform.openai.com](https://platform.openai.com) | Pay-as-you-go |
| **Gemini** | [aistudio.google.com](https://aistudio.google.com) | Free tier available |
| **Grok** | [console.x.ai](https://console.x.ai) | Limited free |
| **GitHub** | Settings → Developer → Personal access tokens | 5000 req/hr (free) |
""")

    with tab2:
        health = api_get("/health")
        if health:
            status = health.get("status", "unknown")
            st.markdown(f"### System Status: {'🟢' if status == 'ok' else '🟡' if status == 'warning' else '🔴'} {status.upper()}")

            checks = health.get("checks", {})
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Runs", checks.get("total_runs", 0))
            col2.metric("Pending Alerts", checks.get("pending_alerts", 0))
            col3.metric("Active Watchlist", checks.get("active_watchlist", 0))

            if "db_size_mb" in checks:
                st.metric("Database Size", f"{checks['db_size_mb']:.1f} MB")

            latest = checks.get("latest_run", {})
            if latest:
                st.json(latest)

            warnings = health.get("warnings", [])
            for w in warnings:
                st.warning(f"⚠️ {w}")

    with tab3:
        layers = api_get("/layers")
        if layers:
            for l in layers:
                with st.expander(f"**{l['name']}** — {l['source']}"):
                    st.write(l.get("description", ""))
                    if l.get("fallbacks"):
                        st.caption(f"Fallbacks: {', '.join(l['fallbacks'])}")
                    if l.get("configurable"):
                        st.json(l["configurable"])

    with tab4:
        runs = api_get("/runs", {"limit": 50})
        if runs:
            df = pd.DataFrame(runs)
            st.dataframe(df, use_container_width=True, height=500)

    # Quick actions
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Quick Refresh Tracked"):
            result = api_post("/tracked/refresh")
            if result:
                st.success(f"Refreshed: {result.get('refreshed', 0)} assets")
    with col2:
        scopes = api_get("/scopes")
        if scopes:
            st.write("Available scopes:")
            for s in scopes:
                st.caption(f"**{s['name']}** ({s['key']}) — {s['asset_class']} — limit: {s['default_limit']}")
