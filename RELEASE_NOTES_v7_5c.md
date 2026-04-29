# NyoSig Analysator v7.5c -- Release Notes

## Summary
v7.5c is a conservative corrective version. It keeps the full v7.5a analytical engine and integrates the useful part of v7.5b without replacing the full core.

## Changed
1. Added `nyosig_analysator_core_v7.5c.py`.
2. Added `nyosig_analysator_gui_v7.5c.py`.
3. GUI now prefers core v7.5c, with fallback to v7.5a and older compatible cores.
4. API and deploy package now prefer core v7.5c.
5. Added `run_pipeline()` compatibility wrapper backed by the full core.
6. Added JSON run mirrors:
   `data/run_history.json`
   `data/run_profiles.json`
7. `run_snapshot_and_topnow()` now records success and failure profiles.
8. CoinGecko requests now always send a NyoSig User-Agent.
9. Local default project root is now `/storage/emulated/0/Programy/analyza_trhu`.

## Important
v7.5b was not used as a replacement core. Its simplified run-history idea was integrated into the full v7.5a codebase.

## Validation performed
Syntax validation passed with Python AST parse for:
`nyosig_analysator_core_v7.5c.py`
`nyosig_analysator_gui_v7.5c.py`
Updated deploy package Python files

Live CoinGecko or Railway execution was not verified in this environment.
