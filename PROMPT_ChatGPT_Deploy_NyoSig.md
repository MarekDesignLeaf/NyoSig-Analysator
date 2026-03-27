# Prompt pro ChatGPT — Deploy NyoSig Analysator na GitHub + cloud server

Potřebuji tvou pomoc s nasazením Python webové aplikace na GitHub a následným deployem na cloud server (Railway nebo Render free tier).

## Co mám

Mám 8 Python souborů které tvoří webovou analytickou platformu pro kryptoměny. Všechny soubory jsou v jedné složce na mém Windows PC:

```
C:\Users\hutra\AppData\Local\NyoSig\NyoSig_Analysator\app\versions\
```

### Seznam souborů:

| Soubor | Velikost | Účel |
|--------|----------|------|
| `nyosig_analysator_core_v7.5a.py` | 286 KB | Core engine — 13 analytických vrstev, 28 DB tabulek, SQLite |
| `nyosig_api.py` | 30 KB | FastAPI REST backend — 48+ endpointů |
| `nyosig_dashboard.py` | 63 KB | Streamlit web dashboard — 13 stránek, Plotly grafy |
| `nyosig_ai_commentator.py` | 14 KB | AI report generátor (Claude/GPT/Gemini + rule-based fallback) |
| `nyosig_paper_trading.py` | 16 KB | Paper trading s SHA256 auditem |
| `nyosig_automator.py` | 18 KB | Scheduling engine + headless server mode |
| `nyosig_analytics_log.py` | 23 KB | Analytická databáze pro profiling operací |
| `test_platform_e2e.py` | 12 KB | E2E test suite — 65 testů |

### Jak to funguje:

1. **FastAPI backend** (`nyosig_api.py`) běží na portu 8000, importuje core a všechny moduly
2. **Streamlit dashboard** (`nyosig_dashboard.py`) běží na portu 8501, volá API přes HTTP
3. **Core** (`nyosig_analysator_core_v7.5a.py`) obsahuje veškerou business logiku, SQLite databázi
4. Aplikace stahuje data z free API (CoinGecko, Yahoo Finance, Binance, GitHub, FRED, Blockchair)
5. Lokálně otestováno na Windows — 64/65 testů prošlo

### Závislosti (Python 3.10+):
```
fastapi
uvicorn
streamlit
requests
plotly
pandas
```

## Co potřebuji udělat:

### Krok 1: Vytvořit GitHub repozitář
- Název: `NyoSig-Analysator`
- Private repo
- Nahrát všech 8 souborů
- Přidat `requirements.txt` s výše uvedenými závislostmi
- Přidat `Procfile` nebo `railway.json` pro deployment
- Přidat `.gitignore` (Python standard + db/ + config/ + cache/)

### Krok 2: Deploy na Railway.app (preferováno) nebo Render.com
- Potřebuji DVA procesy běžící současně:
  - **API server**: `uvicorn nyosig_api:app --host 0.0.0.0 --port 8000`
  - **Dashboard**: `streamlit run nyosig_dashboard.py --server.port 8501 --server.address 0.0.0.0`
- Dashboard musí vědět kde je API — environment variable `NYOSIG_API_URL=http://localhost:8000` nebo interní URL na Railway
- SQLite databáze se vytvoří automaticky při prvním spuštění
- Složky `db/`, `config/`, `cache/`, `logs/` se vytvoří automaticky

### Krok 3: Startup script
Vytvoř `start.sh` který spustí obě služby:
```bash
#!/bin/bash
# Start API in background
uvicorn nyosig_api:app --host 0.0.0.0 --port 8000 &
# Start Streamlit dashboard
streamlit run nyosig_dashboard.py --server.port 8501 --server.address 0.0.0.0 --server.headless true
```

### Environment variables které budu potřebovat nastavit na serveru:
```
NYOSIG_PROJECT_ROOT=/app
NYOSIG_API_URL=http://localhost:8000
```

### Volitelně (API klíče pro rozšířenou funkčnost):
```
NYOSIG_GITHUB_TOKEN=ghp_xxx (GitHub API pro Fundamental layer)
ANTHROPIC_API_KEY=sk-ant-xxx (Claude pro AI reporty)
OPENAI_API_KEY=sk-xxx (GPT pro AI reporty)
```

## Důležité detaily:

1. Core soubor detekuje project root přes env `NYOSIG_PROJECT_ROOT`, defaultuje na `/storage/emulated/0/NyoSig/NyoSig_Analysator` (Android). Na serveru musí být nastaveno na `/app` nebo odpovídající cestu.

2. API soubor (`nyosig_api.py`) detekuje Windows vs Linux automaticky a nastavuje cesty.

3. Dashboard (`nyosig_dashboard.py`) volá API na `http://localhost:8000` — na Railway/Render bude potřeba interní networking nebo shared process.

4. SQLite databáze je single-file, vytvoří se automaticky v `{PROJECT_ROOT}/db/nyosig_analysator.db`.

5. Aplikace je self-contained — žádné další systémové závislosti kromě Python 3.10+.

## Výstup který očekávám:

1. GitHub repo vytvořený s všemi soubory + config soubory pro deploy
2. Deploy na Railway/Render s funkčním URL pro dashboard
3. Návod jak přidat API klíče přes environment variables
4. URL kde uvidím běžící dashboard

Můžeš začít? Mám soubory připravené k nahrání.
