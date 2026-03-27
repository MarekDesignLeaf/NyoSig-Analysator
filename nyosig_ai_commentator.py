#!/usr/bin/env python3
"""
NyoSig Analysator — AI Commentator Module
Generates professional market intelligence reports from 13-layer analysis data.

Uses Claude API (Anthropic) to interpret conflicts between layers,
identify risks, and produce hedge-fund-grade market context reports.

Install: pip install anthropic
Usage:   Called by API endpoint or directly from Python.

IMPORTANT: This is NOT a prediction engine. It's an interpretation engine.
The AI reads structured data and explains what's happening — conflicts,
convergences, risks, and context. It does NOT predict prices.
"""
import os
import json
import time
from typing import Optional, Dict, Any, List

# --- Config ---
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DEFAULT_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 2000


def _build_market_context_prompt(
    summary: Dict,
    predictions: List[Dict],
    trade_plans: List[Dict],
    features: List[Dict],
    correlations: List[Dict],
    portfolio_risk: Optional[Dict] = None,
    scope: str = "crypto_spot",
) -> str:
    """
    Build the prompt that turns structured data into professional analysis.
    The prompt instructs the AI to act as an analyst, NOT an oracle.
    """

    # Compact the data to fit in context
    pred_summary = []
    for p in (predictions or [])[:15]:
        pred_summary.append({
            "symbol": p.get("symbol"),
            "signal": p.get("signal"),
            "confidence": round(p.get("confidence", 0), 3),
            "structural_avg": p.get("structural_avg"),
        })

    plan_summary = []
    for tp in (trade_plans or [])[:15]:
        plan_summary.append({
            "symbol": tp.get("symbol"),
            "direction": tp.get("direction"),
            "entry": f"{tp.get('entry_low', 0):.2f}-{tp.get('entry_high', 0):.2f}",
            "stop": tp.get("stop_loss"),
            "target1": tp.get("target_1"),
            "position_pct": tp.get("position_pct"),
        })

    feat_summary = []
    for f in (features or [])[:15]:
        layers = f.get("features", {})
        feat_summary.append({
            "symbol": f.get("symbol"),
            "norm_score": f.get("norm_score"),
            "layers": {k: round(v, 1) if v else None
                       for k, v in layers.items()},
        })

    cor_summary = []
    for c in (correlations or []):
        if "note" in c:
            continue
        cor_summary.append({
            "btc_change": c.get("btc_change"),
            "reference": c.get("reference"),
            "ref_change": c.get("ref_change"),
            "expected": c.get("expected_correlation"),
            "aligned": c.get("currently_aligned"),
        })

    signal_dist = summary.get("signal_distribution", {})
    warnings = summary.get("warnings", [])

    prompt = f"""You are a senior hedge fund market analyst. You have access to a proprietary 
multi-layer market intelligence system (NyoSig) that aggregates data from 10+ independent 
analytical layers: spot pricing, derivatives (funding rates), on-chain metrics, institutional 
flows (ETF/CME), macro indicators (DXY/VIX/yields), sentiment (Fear & Greed), technical 
analysis (RSI/MACD/EMA), community engagement, open interest, and fundamental development activity.

Your job is to INTERPRET the data — find conflicts between layers, identify risks, 
and explain what the combined picture means. You are NOT predicting prices. You are 
providing SITUATIONAL AWARENESS.

SCOPE: {scope}

=== SIGNAL DISTRIBUTION ===
{json.dumps(signal_dist, indent=2)}

=== PREDICTIONS (per symbol) ===
{json.dumps(pred_summary, indent=2)}

=== FEATURE VECTORS (layer scores per symbol, 0-100) ===
{json.dumps(feat_summary, indent=2)}

=== TRADE PLANS ===
{json.dumps(plan_summary, indent=2)}

=== CROSS-SCOPE CORRELATIONS ===
{json.dumps(cor_summary, indent=2)}

=== PORTFOLIO RISK ===
{json.dumps(portfolio_risk or {}, indent=2)}

=== SYSTEM WARNINGS ===
{json.dumps(warnings, indent=2)}

Write a professional market intelligence report with these sections:

1. MARKET REGIME (2-3 sentences)
What is the overall market character right now? Risk-on, risk-off, transitional, 
range-bound? Base this on the combination of macro, sentiment, and derivatives data.

2. LAYER CONFLICTS (most important section)
Identify where layers DISAGREE. For example:
- On-chain bullish but macro bearish
- Sentiment extreme fear but derivatives show accumulation
- Technical degraded while fundamentals strong
Explain what each conflict means and which side has more weight.

3. TOP OPPORTUNITIES (2-3 symbols max)
Which symbols show the strongest CONVERGENCE across structural layers?
Only mention symbols where on-chain + derivatives + technical agree.
If no strong convergence exists, say so explicitly.

4. KEY RISKS (3-5 bullet points)
What could go wrong? What are the biggest threats to current positions?
Consider: macro divergence, overextended sentiment, liquidity risks.

5. RECOMMENDED POSTURE (1 paragraph)
Given ALL the data, what should the overall portfolio stance be?
Not specific trades — but general risk appetite: aggressive, cautious, 
defensive, or wait-for-clarity.

RULES:
- Never say "the price will go to X"
- Always frame as probabilities and scenarios, not certainties
- If data is incomplete or degraded, say so explicitly
- Structural layers (on-chain, derivatives, technical) always outweigh 
  sentiment and community in your assessment
- Be direct and concise — this is for professionals, not retail
"""
    return prompt


def generate_ai_commentary(
    summary: Dict,
    predictions: List[Dict],
    trade_plans: List[Dict],
    features: List[Dict],
    correlations: List[Dict],
    portfolio_risk: Optional[Dict] = None,
    scope: str = "crypto_spot",
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
) -> Dict[str, Any]:
    """
    Generate AI market commentary from layer data.
    Returns dict with: report (str), model, tokens_used, timestamp, cost_estimate.
    
    Requires: pip install anthropic
    Set ANTHROPIC_API_KEY environment variable or pass api_key parameter.
    """
    key = api_key or ANTHROPIC_API_KEY
    if not key:
        return {
            "error": "No API key. Set ANTHROPIC_API_KEY or pass api_key parameter.",
            "report": _generate_fallback_report(summary, predictions, correlations),
            "model": "fallback_rule_based",
            "cost_estimate": 0,
        }

    prompt = _build_market_context_prompt(
        summary, predictions, trade_plans, features,
        correlations, portfolio_risk, scope)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=key)
        response = client.messages.create(
            model=model,
            max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
        report_text = response.content[0].text
        input_tokens = response.usage.input_tokens
        output_tokens = response.usage.output_tokens

        # Cost estimate (Sonnet 4 pricing approx)
        cost = (input_tokens * 0.003 + output_tokens * 0.015) / 1000

        return {
            "report": report_text,
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost_estimate_usd": round(cost, 4),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "scope": scope,
        }
    except ImportError:
        return {
            "error": "anthropic package not installed. Run: pip install anthropic",
            "report": _generate_fallback_report(summary, predictions, correlations),
            "model": "fallback_rule_based",
            "cost_estimate_usd": 0,
        }
    except Exception as e:
        return {
            "error": str(e)[:300],
            "report": _generate_fallback_report(summary, predictions, correlations),
            "model": "fallback_rule_based",
            "cost_estimate_usd": 0,
        }


def _generate_fallback_report(summary, predictions, correlations):
    """
    Rule-based fallback when API key is not available.
    Not as good as AI, but still useful structured output.
    """
    lines = ["# NyoSig Market Intelligence Report (rule-based fallback)", ""]

    # Signal distribution
    dist = summary.get("signal_distribution", {})
    buys = dist.get("strong_buy", 0) + dist.get("buy", 0)
    sells = dist.get("strong_sell", 0) + dist.get("sell", 0)
    neutrals = dist.get("neutral", 0)
    total = buys + sells + neutrals

    lines.append("## MARKET REGIME")
    if buys > sells * 2:
        lines.append("Market shows broad bullish structural signals across multiple layers.")
    elif sells > buys * 2:
        lines.append("Market shows broad bearish structural signals. Risk-off posture recommended.")
    elif neutrals > total * 0.6:
        lines.append("Market is range-bound with no clear directional bias. Wait for clarity.")
    else:
        lines.append("Mixed signals across layers. Transitional regime — reduced position sizes recommended.")

    # Correlations
    if correlations:
        lines.append("")
        lines.append("## CROSS-ASSET SIGNALS")
        for c in correlations:
            if "note" in c:
                continue
            status = "aligned with historical pattern" if c.get("currently_aligned") else "DIVERGENT from historical pattern"
            lines.append(f"- BTC vs {c.get('reference', '?')}: {status}")

    # Top picks
    if predictions:
        lines.append("")
        lines.append("## NOTABLE SIGNALS")
        strong = [p for p in predictions if p.get("signal") in ("strong_buy", "strong_sell")]
        for p in strong[:3]:
            lines.append(f"- {p['symbol']}: {p['signal']} (confidence: {p.get('confidence', 0):.1%}, "
                         f"structural: {p.get('structural_avg', '?')})")

    # Warnings
    warnings = summary.get("warnings", [])
    if warnings:
        lines.append("")
        lines.append("## WARNINGS")
        for w in warnings:
            lines.append(f"- {w}")

    lines.append("")
    lines.append("---")
    lines.append("*This is a rule-based summary. Set ANTHROPIC_API_KEY for AI-powered analysis.*")

    return "\n".join(lines)


def generate_multi_ai_commentary(
    summary: Dict,
    predictions: List[Dict],
    trade_plans: List[Dict],
    features: List[Dict],
    correlations: List[Dict],
    portfolio_risk: Optional[Dict] = None,
    scope: str = "crypto_spot",
) -> Dict[str, Any]:
    """
    Generate commentary from multiple AI providers and combine.
    Uses whichever API keys are available.
    Returns dict with individual reports + ensemble summary.
    """
    results = {}

    # Claude
    claude_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if claude_key:
        results["claude"] = generate_ai_commentary(
            summary, predictions, trade_plans, features,
            correlations, portfolio_risk, scope,
            api_key=claude_key, model="claude-sonnet-4-20250514")

    # OpenAI (GPT-4o)
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if openai_key:
        try:
            results["gpt"] = _generate_openai_commentary(
                summary, predictions, trade_plans, features,
                correlations, portfolio_risk, scope, openai_key)
        except Exception as e:
            results["gpt"] = {"error": str(e)[:200]}

    # Google Gemini
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    if gemini_key:
        try:
            results["gemini"] = _generate_gemini_commentary(
                summary, predictions, trade_plans, features,
                correlations, portfolio_risk, scope, gemini_key)
        except Exception as e:
            results["gemini"] = {"error": str(e)[:200]}

    # Fallback if no API keys
    if not results:
        results["fallback"] = {
            "report": _generate_fallback_report(summary, predictions, correlations),
            "model": "rule_based",
        }

    # Ensemble: simple concatenation with headers
    ensemble_parts = []
    for provider, data in results.items():
        if "report" in data:
            ensemble_parts.append(f"--- {provider.upper()} ANALYSIS ---\n{data['report']}")
    ensemble = "\n\n".join(ensemble_parts)

    total_cost = sum(r.get("cost_estimate_usd", 0) for r in results.values())

    return {
        "individual": results,
        "ensemble_report": ensemble,
        "providers_used": list(results.keys()),
        "total_cost_usd": round(total_cost, 4),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def _generate_openai_commentary(summary, predictions, trade_plans, features,
                                 correlations, portfolio_risk, scope, api_key):
    """OpenAI GPT-4o commentary. Requires: pip install openai"""
    import openai
    client = openai.OpenAI(api_key=api_key)

    prompt = _build_market_context_prompt(
        summary, predictions, trade_plans, features,
        correlations, portfolio_risk, scope)

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=MAX_TOKENS,
    )
    text = response.choices[0].message.content
    usage = response.usage
    cost = (usage.prompt_tokens * 0.0025 + usage.completion_tokens * 0.01) / 1000

    return {
        "report": text,
        "model": "gpt-4o",
        "input_tokens": usage.prompt_tokens,
        "output_tokens": usage.completion_tokens,
        "cost_estimate_usd": round(cost, 4),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def _generate_gemini_commentary(summary, predictions, trade_plans, features,
                                 correlations, portfolio_risk, scope, api_key):
    """Google Gemini commentary. Requires: pip install google-genai"""
    import google.generativeai as genai
    genai.configure(api_key=api_key)

    prompt = _build_market_context_prompt(
        summary, predictions, trade_plans, features,
        correlations, portfolio_risk, scope)

    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)

    return {
        "report": response.text,
        "model": "gemini-2.0-flash",
        "cost_estimate_usd": 0.001,  # Gemini Flash is very cheap
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
