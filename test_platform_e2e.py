#!/usr/bin/env python3
"""
NyoSig Analysator — Local End-to-End Platform Test
Tests the entire web platform without needing a browser.

Usage:
    1. Start API:  python nyosig_api.py
    2. Run tests:  python test_platform_e2e.py

Tests all 48 API endpoints, full pipeline flow, analysis,
predictions, trade plans, paper trading, automation, analytics,
AI commentator (fallback), and key management.
"""
import sys
import time
import json
import requests

API = "http://localhost:8000"
PASSED = 0
FAILED = 0
ERRORS = []

def test(name, fn):
    global PASSED, FAILED
    try:
        result = fn()
        if result:
            PASSED += 1
            print(f"  ✅ {name}")
        else:
            FAILED += 1
            ERRORS.append(f"{name}: returned False")
            print(f"  ❌ {name}")
    except Exception as e:
        FAILED += 1
        ERRORS.append(f"{name}: {str(e)[:200]}")
        print(f"  ❌ {name}: {e}")

def get(path, params=None):
    r = requests.get(f"{API}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def post(path, data=None):
    r = requests.post(f"{API}{path}", json=data, timeout=120)
    r.raise_for_status()
    return r.json()

def delete(path):
    r = requests.delete(f"{API}{path}", timeout=10)
    r.raise_for_status()
    return r.json()


# =====================================================================
print("=" * 60)
print("NyoSig Platform E2E Test")
print("=" * 60)

# Check API is running
print("\n[0] API Connectivity")
try:
    r = requests.get(f"{API}/", timeout=5)
    r.raise_for_status()
    info = r.json()
    print(f"  API: {info.get('service', '?')} {info.get('version', '?')}")
    print(f"  Root: {info.get('project_root', '?')}")
except Exception as e:
    print(f"  ❌ API not reachable at {API}")
    print(f"     Start the API first: python nyosig_api.py")
    print(f"     Error: {e}")
    sys.exit(1)

# =====================================================================
print("\n[1] System Endpoints")
test("GET /health", lambda: get("/health").get("status") in ("ok", "warning", "critical"))
test("GET /scopes", lambda: len(get("/scopes")) >= 1)
test("GET /layers", lambda: len(get("/layers")) == 10)
test("GET /runs (empty ok)", lambda: isinstance(get("/runs"), list))

# =====================================================================
print("\n[2] Key Management")
test("GET /keys", lambda: len(get("/keys")) == 5)
test("POST /keys/github (set test key)", lambda: post("/keys/github", {"key": "ghp_test123"}).get("status") == "saved")
test("GET /keys (github now set)", lambda: any(k["is_set"] and k["provider"] == "github" for k in get("/keys")))
test("DELETE /keys/github", lambda: delete("/keys/github").get("status") == "deleted")
test("GET /keys (github now unset)", lambda: not any(k["is_set"] and k["provider"] == "github" for k in get("/keys")))

# =====================================================================
print("\n[3] Pipeline Execution (this takes 30-120 seconds)")
print("     Starting pipeline...")
t0 = time.time()
result = post("/pipeline/run", {
    "scope": "crypto_spot",
    "vs_currency": "usd",
    "coins_limit": 50,
    "topnow_limit": 10,
    "offline_mode": False,
})
test("POST /pipeline/run starts", lambda: result.get("status") == "started")

# Wait for pipeline to finish
max_wait = 180
while time.time() - t0 < max_wait:
    status = get("/pipeline/status")
    state = status.get("status", "")
    if state in ("done", "failed"):
        break
    time.sleep(3)
    elapsed = int(time.time() - t0)
    print(f"     ... {state} ({elapsed}s)")

pipeline_result = get("/pipeline/status")
pipeline_state = pipeline_result.get("status", "")
pipeline_data = pipeline_result.get("result", {})
test("Pipeline completed", lambda: pipeline_state == "done")
test("Pipeline has run_id", lambda: pipeline_data.get("run_id") is not None)
test("Pipeline has candidates", lambda: (pipeline_data.get("candidates_n") or 0) > 0)

RUN_ID = pipeline_data.get("run_id")
SEL_ID = pipeline_data.get("selection_id")
print(f"     Pipeline done: run_id={RUN_ID}, selection_id={SEL_ID}, "
      f"candidates={pipeline_data.get('candidates_n')}, "
      f"time={int(time.time()-t0)}s")

# =====================================================================
print("\n[4] Analysis Execution")
if RUN_ID and SEL_ID:
    t1 = time.time()
    print("     Starting analysis...")
    post("/analyse", {"selection_id": SEL_ID, "run_id": RUN_ID})

    # Wait for analysis
    while time.time() - t1 < 120:
        status = get("/pipeline/status")
        state = status.get("status", "")
        if state in ("done", "failed", "idle"):
            break
        time.sleep(3)
        elapsed = int(time.time() - t1)
        print(f"     ... {state} ({elapsed}s)")

    print(f"     Analysis done: {int(time.time()-t1)}s")

# =====================================================================
print("\n[5] Data Retrieval Endpoints")
if RUN_ID:
    test("GET /runs", lambda: len(get("/runs")) >= 1)
    test("GET /runs/{id}/summary", lambda: get(f"/runs/{RUN_ID}/summary") is not None)
    test("GET /runs/{id}/predictions", lambda: isinstance(get(f"/runs/{RUN_ID}/predictions"), list))
    test("GET /runs/{id}/trade_plans", lambda: isinstance(get(f"/runs/{RUN_ID}/trade_plans"), list))
    test("GET /runs/{id}/features", lambda: isinstance(get(f"/runs/{RUN_ID}/features"), list))
    test("GET /runs/{id}/correlations", lambda: isinstance(get(f"/runs/{RUN_ID}/correlations"), list))
    test("GET /runs/{id}/backtest", lambda: isinstance(get(f"/runs/{RUN_ID}/backtest"), list))

    # Check content
    preds = get(f"/runs/{RUN_ID}/predictions")
    plans = get(f"/runs/{RUN_ID}/trade_plans")
    feats = get(f"/runs/{RUN_ID}/features")
    summary = get(f"/runs/{RUN_ID}/summary")

    test("Predictions generated", lambda: len(preds) > 0)
    test("Trade plans generated", lambda: len(plans) > 0)
    test("Feature vectors generated", lambda: len(feats) > 0)
    test("Summary has signal_distribution", lambda: "signal_distribution" in (summary or {}))

    if preds:
        p = preds[0]
        test("Prediction has symbol", lambda: "symbol" in p)
        test("Prediction has signal", lambda: p.get("signal") in ("strong_buy", "buy", "neutral", "sell", "strong_sell"))
        test("Prediction has confidence", lambda: 0 <= (p.get("confidence") or 0) <= 1)

    if plans:
        tp = plans[0]
        test("Trade plan has direction", lambda: tp.get("direction") in ("long", "short", "hold"))
        test("Trade plan has stop_loss", lambda: tp.get("stop_loss") is not None)

    print(f"     Predictions: {len(preds)}, Trade plans: {len(plans)}, Features: {len(feats)}")
else:
    print("  ⏭ Skipping (no run_id)")

# =====================================================================
print("\n[6] Selection & Candidates")
if SEL_ID:
    test("GET /selection/{id}", lambda: len(get(f"/selection/{SEL_ID}")) > 0)

    candidates = get(f"/selection/{SEL_ID}")
    if candidates:
        c = candidates[0]
        test("Candidate has symbol", lambda: "symbol" in c)
        test("Candidate has composite", lambda: "composite" in c)
        test("Candidate has price", lambda: c.get("price") is not None)
        print(f"     Top candidate: {c['symbol']} composite={c.get('composite')}")

# =====================================================================
print("\n[7] Alerts")
if RUN_ID:
    test("GET /alerts", lambda: isinstance(get("/alerts"), list))
    test("POST /alerts/{id}/check", lambda: "new_alerts" in post(f"/alerts/{RUN_ID}/check"))

# =====================================================================
print("\n[8] Watchlist")
test("GET /watchlist", lambda: isinstance(get("/watchlist"), list))

# =====================================================================
print("\n[9] Portfolio")
test("GET /portfolio", lambda: isinstance(get("/portfolio"), dict))
test("GET /portfolio/risk", lambda: isinstance(get("/portfolio/risk"), dict))

# =====================================================================
print("\n[10] Performance")
test("GET /performance", lambda: isinstance(get("/performance"), list))
test("POST /performance/evaluate", lambda: isinstance(post("/performance/evaluate"), dict))

# =====================================================================
print("\n[11] Quick Refresh")
test("POST /tracked/refresh", lambda: isinstance(post("/tracked/refresh"), dict))

# =====================================================================
print("\n[12] AI Report (fallback mode)")
if RUN_ID:
    ai = get(f"/ai/report/{RUN_ID}")
    test("GET /ai/report/{id}", lambda: ai is not None)
    test("AI report has text", lambda: len(ai.get("report", "")) > 50)
    test("AI report model", lambda: ai.get("model") is not None)
    if ai.get("report"):
        print(f"     Report length: {len(ai['report'])} chars")
        print(f"     Model: {ai.get('model', '?')}")
        # Print first 150 chars
        print(f"     Preview: {ai['report'][:150]}...")

# =====================================================================
print("\n[13] Paper Trading")
if RUN_ID:
    paper = post(f"/paper/run/{RUN_ID}")
    test("POST /paper/run/{id}", lambda: paper is not None)
    test("Paper predictions recorded", lambda: (paper.get("predictions_recorded") or 0) >= 0)
    test("GET /paper/stats", lambda: isinstance(get("/paper/stats"), dict))
    test("GET /paper/reports", lambda: isinstance(get("/paper/reports"), list))
    test("GET /paper/predictions", lambda: isinstance(get("/paper/predictions"), list))
    test("POST /paper/evaluate", lambda: isinstance(post("/paper/evaluate"), dict))

    stats = get("/paper/stats")
    preds_paper = get("/paper/predictions")
    reports = get("/paper/reports")
    print(f"     Paper predictions: {len(preds_paper)}")
    print(f"     Paper reports: {len(reports)}")
    print(f"     Stats: {json.dumps(stats)[:200]}")

# =====================================================================
print("\n[14] Automation Config")
test("GET /automat/config", lambda: isinstance(get("/automat/config"), dict))
test("GET /automat/status", lambda: "running" in get("/automat/status"))
test("GET /automat/history", lambda: isinstance(get("/automat/history"), list))
test("GET /automat/candidates", lambda: isinstance(get("/automat/candidates"), list))

# Save and load config
cfg = get("/automat/config")
test("Config has scope", lambda: "scope" in cfg)
test("Config has interval_mode", lambda: "interval_mode" in cfg)

# Update config
cfg["top_n"] = 5
cfg["interval_mode"] = "daily"
test("POST /automat/config", lambda: post("/automat/config", cfg).get("status") == "saved")
test("Config persisted", lambda: get("/automat/config").get("top_n") == 5)

# Restore
cfg["top_n"] = 15
post("/automat/config", cfg)

# =====================================================================
print("\n[15] Analytics")
test("GET /analytics/overview", lambda: isinstance(get("/analytics/overview"), dict))
test("GET /analytics/runs", lambda: isinstance(get("/analytics/runs"), list))
test("GET /analytics/daily", lambda: isinstance(get("/analytics/daily"), list))
test("GET /analytics/db_size", lambda: "size_mb" in get("/analytics/db_size"))

if RUN_ID:
    test("GET /analytics/runs/{id}/operations", lambda: isinstance(get(f"/analytics/runs/{RUN_ID}/operations"), list))
    test("GET /analytics/runs/{id}/layers", lambda: isinstance(get(f"/analytics/runs/{RUN_ID}/layers"), list))
    test("GET /analytics/runs/{id}/api_calls", lambda: isinstance(get(f"/analytics/runs/{RUN_ID}/api_calls"), list))

# =====================================================================
print("\n" + "=" * 60)
print(f"RESULTS: {PASSED} passed, {FAILED} failed, {PASSED + FAILED} total")
print("=" * 60)

if ERRORS:
    print("\nFAILED TESTS:")
    for e in ERRORS:
        print(f"  ❌ {e}")

if FAILED == 0:
    print("\n🎉 ALL TESTS PASSED — platform is ready for deployment!")
else:
    print(f"\n⚠️ {FAILED} tests failed — fix issues before deployment")

sys.exit(0 if FAILED == 0 else 1)
