# Koju Dhan Algo

FastAPI dashboard and DhanHQ trading engine for 1-minute and 5-minute opening-range momentum setups.

The production app is split into three services:

- `kujialgo-api`: dashboard and control API
- `kujialgo-engine`: market feed, order socket, strategy evaluation, order placement
- `kujialgo-reconcile`: premarket cache, broker reconciliation, missing candle recovery

## Run

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Terminal 1:

```powershell
python -m app.engine_service
```

Terminal 2:

```powershell
python -m app.reconcile_service
```

Terminal 3:

```powershell
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Open `http://127.0.0.1:8000`.

For VM systemd deployment, see `deploy/README.md`.

## Notes

- The app starts in `dry_run` mode. Disable dry-run in the dashboard only after credentials, scrip resolution, historical cache, and order updates are verified.
- Dhan order placement requires DhanHQ API access and static IP whitelisting for order endpoints.
- Market data APIs require an active DhanHQ data subscription.
- The dashboard can paste watchlists from Chartink, NSE tables, CSV, or plain text and extracts valid NSE-style symbols.
