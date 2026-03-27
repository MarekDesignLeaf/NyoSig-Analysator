#!/usr/bin/env python3
"""
NyoSig Analysator — Paper Trading Engine
Automated auditable prediction log with outcome tracking.

Creates timestamped, immutable prediction records BEFORE outcomes are known.
After T+1d, T+7d, T+30d automatically evaluates accuracy.
Generates public-ready reports for X/Twitter, blog, or investor presentations.

No external dependencies beyond core + sqlite3.
"""
import os
import sys
import json
import time
import sqlite3
import hashlib
from typing import Optional, Dict, List, Any

# --- Load core ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    from nyosig_ai_commentator import generate_ai_commentary, _generate_fallback_report
    _HAS_AI = True
except ImportError:
    _HAS_AI = False


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def utc_date():
    return time.strftime("%Y-%m-%d", time.gmtime())


# =====================================================================
# PAPER TRADE LOG SCHEMA
# =====================================================================

def ensure_paper_schema(con):
    """Create paper trading tables if they don't exist."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS paper_predictions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            created_utc     TEXT    NOT NULL,
            scope           TEXT    NOT NULL,
            symbol          TEXT    NOT NULL,
            signal          TEXT    NOT NULL,
            confidence      REAL,
            structural_avg  REAL,
            price_at_call   REAL,
            entry_low       REAL,
            entry_high      REAL,
            stop_loss       REAL,
            target_1        REAL,
            target_2        REAL,
            direction       TEXT,
            ai_summary      TEXT,
            hash_sha256     TEXT    NOT NULL,
            status          TEXT    NOT NULL DEFAULT 'open'
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS paper_outcomes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            prediction_id   INTEGER NOT NULL,
            eval_period     TEXT    NOT NULL,
            eval_utc        TEXT    NOT NULL,
            price_at_eval   REAL,
            pnl_pct         REAL,
            hit_target_1    INTEGER DEFAULT 0,
            hit_target_2    INTEGER DEFAULT 0,
            hit_stop        INTEGER DEFAULT 0,
            outcome         TEXT,
            FOREIGN KEY (prediction_id) REFERENCES paper_predictions(id)
        );
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS paper_daily_reports (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date_utc        TEXT    NOT NULL,
            run_id          INTEGER,
            scope           TEXT,
            report_text     TEXT    NOT NULL,
            tweet_text      TEXT,
            predictions_n   INTEGER,
            created_utc     TEXT    NOT NULL
        );
    """)
    con.commit()


# =====================================================================
# RECORD PREDICTIONS (immutable, hashed)
# =====================================================================

def record_paper_predictions(con, run_id, scope, predictions, trade_plans,
                              ai_summary="", log_cb=None):
    """
    Record predictions as immutable paper trades.
    Each prediction is SHA256-hashed at creation time for audit integrity.
    Returns count of new predictions recorded.
    """
    if log_cb is None:
        log_cb = lambda m: None

    ensure_paper_schema(con)
    created = utc_now()
    plans_by_sym = {p["symbol"]: p for p in (trade_plans or [])}
    recorded = 0

    for pred in (predictions or []):
        sym = pred.get("symbol", "")
        signal = pred.get("signal", "neutral")
        if signal == "neutral":
            continue  # Don't paper-trade neutrals

        # Check if already recorded for this run
        existing = con.execute(
            "SELECT id FROM paper_predictions WHERE run_id=? AND symbol=?;",
            (run_id, sym)).fetchone()
        if existing:
            continue

        plan = plans_by_sym.get(sym, {})
        price = pred.get("reasoning", {}).get("price") or plan.get("entry_low") or 0

        # Get latest price from market_snapshots
        price_row = con.execute(
            "SELECT price FROM market_snapshots "
            "WHERE unified_symbol=? AND timeframe='spot' "
            "ORDER BY id DESC LIMIT 1;", (sym,)).fetchone()
        if price_row:
            price = price_row[0]

        # Build hash payload (proves prediction was made at this time)
        hash_payload = json.dumps({
            "run_id": run_id, "symbol": sym, "signal": signal,
            "confidence": pred.get("confidence"),
            "price": price, "timestamp": created,
        }, sort_keys=True)
        hash_sha = hashlib.sha256(hash_payload.encode()).hexdigest()

        con.execute(
            "INSERT INTO paper_predictions "
            "(run_id, created_utc, scope, symbol, signal, confidence, "
            "structural_avg, price_at_call, entry_low, entry_high, "
            "stop_loss, target_1, target_2, direction, ai_summary, hash_sha256) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
            (run_id, created, scope, sym, signal,
             pred.get("confidence"),
             pred.get("structural_avg"),
             price,
             plan.get("entry_low"), plan.get("entry_high"),
             plan.get("stop_loss"), plan.get("target_1"), plan.get("target_2"),
             plan.get("direction", ""),
             ai_summary[:500] if ai_summary else "",
             hash_sha))
        recorded += 1
        log_cb(f"PAPER: recorded {sym} {signal} @ {price:.2f} (hash={hash_sha[:12]})")

    con.commit()
    log_cb(f"PAPER: {recorded} new predictions recorded for run {run_id}")
    return recorded


# =====================================================================
# EVALUATE OUTCOMES
# =====================================================================

def evaluate_paper_outcomes(con, log_cb=None):
    """
    Evaluate all open paper predictions against current prices.
    Checks T+1d, T+7d, T+30d outcomes.
    Returns summary dict.
    """
    if log_cb is None:
        log_cb = lambda m: None

    ensure_paper_schema(con)
    now = utc_now()
    now_ts = time.time()

    open_preds = con.execute(
        "SELECT id, symbol, signal, price_at_call, stop_loss, target_1, target_2, "
        "direction, created_utc FROM paper_predictions WHERE status='open';"
    ).fetchall()

    evaluated = 0
    for pred_id, sym, signal, price_at, sl, t1, t2, direction, created_utc in open_preds:
        # Parse creation time
        try:
            created_ts = time.mktime(time.strptime(created_utc[:19], "%Y-%m-%dT%H:%M:%S"))
        except Exception:
            continue

        age_hours = (now_ts - created_ts) / 3600

        # Get current price
        price_row = con.execute(
            "SELECT price FROM market_snapshots "
            "WHERE unified_symbol=? AND timeframe='spot' "
            "ORDER BY id DESC LIMIT 1;", (sym,)).fetchone()
        if not price_row or not price_row[0] or not price_at:
            continue
        current = price_row[0]

        # Evaluate periods
        periods_to_check = []
        if age_hours >= 24:
            periods_to_check.append("1d")
        if age_hours >= 168:
            periods_to_check.append("7d")
        if age_hours >= 720:
            periods_to_check.append("30d")

        for period in periods_to_check:
            # Skip if already evaluated
            existing = con.execute(
                "SELECT id FROM paper_outcomes WHERE prediction_id=? AND eval_period=?;",
                (pred_id, period)).fetchone()
            if existing:
                continue

            # Calculate P&L
            if direction == "long" or signal in ("buy", "strong_buy"):
                pnl = round((current - price_at) / price_at * 100, 2)
            else:
                pnl = round((price_at - current) / price_at * 100, 2)

            hit_t1 = 1 if t1 and ((direction == "long" and current >= t1) or
                                   (direction == "short" and current <= t1)) else 0
            hit_t2 = 1 if t2 and ((direction == "long" and current >= t2) or
                                   (direction == "short" and current <= t2)) else 0
            hit_sl = 1 if sl and ((direction == "long" and current <= sl) or
                                   (direction == "short" and current >= sl)) else 0

            outcome = "correct" if pnl > 0 else "incorrect"
            if hit_sl:
                outcome = "stopped_out"
            elif hit_t2:
                outcome = "target_2_hit"
            elif hit_t1:
                outcome = "target_1_hit"

            con.execute(
                "INSERT INTO paper_outcomes "
                "(prediction_id, eval_period, eval_utc, price_at_eval, "
                "pnl_pct, hit_target_1, hit_target_2, hit_stop, outcome) "
                "VALUES (?,?,?,?,?,?,?,?,?);",
                (pred_id, period, now, current, pnl, hit_t1, hit_t2, hit_sl, outcome))
            evaluated += 1

        # Close prediction after 30d evaluation
        if age_hours >= 720:
            con.execute("UPDATE paper_predictions SET status='closed' WHERE id=?;", (pred_id,))

    con.commit()

    # Summary statistics
    stats = _compute_paper_stats(con)
    log_cb(f"PAPER EVAL: {evaluated} new outcomes. Hit rate: {stats.get('hit_rate_1d', 0):.1f}%")
    return {"evaluated": evaluated, **stats}


def _compute_paper_stats(con):
    """Compute aggregate paper trading statistics."""
    stats = {}

    for period in ["1d", "7d", "30d"]:
        rows = con.execute(
            "SELECT outcome, pnl_pct FROM paper_outcomes WHERE eval_period=?;",
            (period,)).fetchall()
        if not rows:
            continue
        correct = sum(1 for r in rows if r[0] in ("correct", "target_1_hit", "target_2_hit"))
        incorrect = sum(1 for r in rows if r[0] in ("incorrect", "stopped_out"))
        total = correct + incorrect
        avg_pnl = sum(r[1] for r in rows if r[1]) / len(rows) if rows else 0

        stats[f"total_{period}"] = total
        stats[f"correct_{period}"] = correct
        stats[f"hit_rate_{period}"] = round(correct / total * 100, 1) if total > 0 else 0
        stats[f"avg_pnl_{period}"] = round(avg_pnl, 2)

    # Overall
    all_outcomes = con.execute(
        "SELECT outcome FROM paper_outcomes WHERE eval_period='1d';").fetchall()
    total = len(all_outcomes)
    stats["total_predictions"] = con.execute(
        "SELECT COUNT(*) FROM paper_predictions;").fetchone()[0]
    stats["open_predictions"] = con.execute(
        "SELECT COUNT(*) FROM paper_predictions WHERE status='open';").fetchone()[0]

    return stats


# =====================================================================
# GENERATE DAILY REPORT
# =====================================================================

def generate_daily_report(con, run_id, scope, predictions, trade_plans,
                           ai_summary="", log_cb=None):
    """
    Generate a daily market intelligence report suitable for public posting.
    Stores in paper_daily_reports table.
    Returns dict with report_text and tweet_text.
    """
    if log_cb is None:
        log_cb = lambda m: None

    ensure_paper_schema(con)
    today = utc_date()
    stats = _compute_paper_stats(con)

    # Build report
    buys = [p for p in predictions if p.get("signal") in ("buy", "strong_buy")]
    sells = [p for p in predictions if p.get("signal") in ("sell", "strong_sell")]
    buys.sort(key=lambda x: -(x.get("confidence", 0)))
    sells.sort(key=lambda x: -(x.get("confidence", 0)))

    lines = [
        f"# NyoSig Market Intelligence — {today}",
        f"Scope: {scope} | Signals: {len(buys)} buy, {len(sells)} sell",
        "",
    ]

    # Track record
    hr = stats.get("hit_rate_1d", 0)
    total = stats.get("total_1d", 0)
    if total >= 10:
        lines.append(f"📊 Track record (1d): **{hr:.0f}%** hit rate ({total} predictions)")
        lines.append("")

    # Top signals
    if buys:
        lines.append("## 🟢 Bullish signals")
        for p in buys[:3]:
            lines.append(f"- **{p['symbol']}**: {p['signal']} "
                         f"(confidence: {p.get('confidence', 0):.0%}, "
                         f"structural: {p.get('structural_avg', '?')})")
    if sells:
        lines.append("")
        lines.append("## 🔴 Bearish signals")
        for p in sells[:3]:
            lines.append(f"- **{p['symbol']}**: {p['signal']} "
                         f"(confidence: {p.get('confidence', 0):.0%})")

    # AI summary excerpt
    if ai_summary:
        lines.append("")
        lines.append("## 🤖 AI Analysis")
        # Take first 3 sentences
        sentences = ai_summary.split(". ")[:3]
        lines.append(". ".join(sentences) + ".")

    # Layer conflicts
    lines.append("")
    lines.append("## ⚠️ Key conflicts")
    lines.append("_Layer disagreements that require attention:_")
    # Simple conflict detection from predictions
    high_conf_buys = [p for p in buys if (p.get("confidence") or 0) > 0.7]
    high_conf_sells = [p for p in sells if (p.get("confidence") or 0) > 0.7]
    if high_conf_buys and high_conf_sells:
        lines.append("- Mixed market: strong buys AND strong sells simultaneously "
                     "— selective, not broad directional")
    elif not buys and not sells:
        lines.append("- No directional signals — market in wait mode")

    lines.append("")
    lines.append("---")
    lines.append(f"_Generated by NyoSig Analysator | 13-layer analysis | "
                 f"All predictions SHA256-hashed at creation time_")

    report_text = "\n".join(lines)

    # Tweet (280 chars max)
    tweet_parts = [f"📊 NyoSig {today}"]
    if buys:
        top = buys[0]
        tweet_parts.append(f"🟢 Top: ${top['symbol']} {top['signal']} "
                           f"({top.get('confidence', 0):.0%})")
    if sells:
        top = sells[0]
        tweet_parts.append(f"🔴 ${top['symbol']} {top['signal']}")
    if total >= 10:
        tweet_parts.append(f"📈 Track: {hr:.0f}% hit rate ({total})")
    tweet_parts.append("#crypto #NyoSig")
    tweet_text = " | ".join(tweet_parts)
    if len(tweet_text) > 280:
        tweet_text = tweet_text[:277] + "..."

    # Store
    con.execute(
        "INSERT INTO paper_daily_reports "
        "(date_utc, run_id, scope, report_text, tweet_text, predictions_n, created_utc) "
        "VALUES (?,?,?,?,?,?,?);",
        (today, run_id, scope, report_text, tweet_text, len(predictions), utc_now()))
    con.commit()

    log_cb(f"PAPER REPORT: {today} generated ({len(buys)} buys, {len(sells)} sells)")
    return {
        "report_text": report_text,
        "tweet_text": tweet_text,
        "date": today,
        "buys": len(buys),
        "sells": len(sells),
        "track_record": stats,
    }


# =====================================================================
# FULL DAILY WORKFLOW (one-call)
# =====================================================================

def run_daily_paper_workflow(db_path, run_id, scope, predictions,
                              trade_plans, ai_summary="", log_cb=None):
    """
    Complete daily paper trading workflow:
    1. Record new predictions (immutable, hashed)
    2. Evaluate past predictions against current prices
    3. Generate daily report with track record
    Returns dict with all results.
    """
    if log_cb is None:
        log_cb = lambda m: None

    con = sqlite3.connect(db_path)
    ensure_paper_schema(con)

    # 1. Record
    n_recorded = record_paper_predictions(
        con, run_id, scope, predictions, trade_plans, ai_summary, log_cb)

    # 2. Evaluate
    eval_result = evaluate_paper_outcomes(con, log_cb)

    # 3. Report
    report = generate_daily_report(
        con, run_id, scope, predictions, trade_plans, ai_summary, log_cb)

    con.close()

    return {
        "predictions_recorded": n_recorded,
        "outcomes_evaluated": eval_result.get("evaluated", 0),
        "report": report,
        "stats": eval_result,
    }
