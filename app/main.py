from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .control import queue_command, read_status_snapshot, save_runtime_config
from .storage import read_json
from .config import PREMARKET_REPORT_FILE
from .symbols import extract_symbols

app = FastAPI(title="Koju Dhan Algo API")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/status")
def status():
    return read_status_snapshot()


@app.post("/api/config")
def configure(payload: dict = Body(...)):
    try:
        save_runtime_config(payload)
        queue_command("engine", "config", payload)
        queue_command("reconcile", "config", payload)
        return read_status_snapshot()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/start")
def start_algo(payload: dict = Body(default={})):
    try:
        if payload:
            save_runtime_config(payload)
            queue_command("reconcile", "config", payload)
        queue_command("engine", "start", payload)
        snapshot = read_status_snapshot()
        snapshot["running"] = True
        snapshot["market_connecting"] = not bool(snapshot.get("market_connected"))
        snapshot["order_connecting"] = not bool(snapshot.get("order_connected"))
        snapshot["last_error"] = ""
        snapshot.setdefault("events", []).insert(0, {"kind": "INFO", "message": "Start command queued for engine service.", "time": ""})
        return snapshot
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/stop")
def stop_algo():
    queue_command("engine", "stop", {})
    snapshot = read_status_snapshot()
    snapshot["running"] = False
    snapshot["market_connected"] = False
    snapshot["market_connecting"] = False
    snapshot["order_connected"] = False
    snapshot["order_connecting"] = False
    return snapshot


@app.post("/api/premarket-cache")
def premarket_cache(payload: dict = Body(default={})):
    try:
        if payload:
            save_runtime_config(payload)
            queue_command("engine", "config", payload)
        queue_command("reconcile", "premarket-cache", payload)
        snapshot = read_status_snapshot()
        snapshot.setdefault("premarket", {})["message"] = "Premarket cache command queued for reconcile service."
        return snapshot
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/premarket-report")
def premarket_report():
    return read_json(PREMARKET_REPORT_FILE, {"message": "Report not started", "summary": {}})


@app.post("/api/extract-symbols")
def parse_symbols(payload: dict = Body(...)):
    return {"symbols": extract_symbols(payload.get("text") or "")}


@app.post("/api/reconcile/{symbol}")
def reconcile(symbol: str):
    queue_command("reconcile", "reconcile-symbol", {"symbol": symbol})
    return {"queued": True, "symbol": symbol}


@app.post("/api/broker-reconcile")
def broker_reconcile():
    queue_command("reconcile", "broker-reconcile", {})
    snapshot = read_status_snapshot()
    snapshot.setdefault("broker_reconcile", {})["message"] = "Broker reconcile command queued for reconcile service."
    return snapshot
