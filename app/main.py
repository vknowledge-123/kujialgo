import asyncio

from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import BROKER_RECONCILE_TIMEOUT_SECONDS, START_API_TIMEOUT_SECONDS
from .engine import DhanAlgoEngine
from .symbols import extract_symbols

app = FastAPI(title="Koju Dhan Algo")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
engine = DhanAlgoEngine()


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/status")
def status():
    return engine.snapshot()


@app.post("/api/config")
def configure(payload: dict = Body(...)):
    try:
        return engine.configure(payload)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/start")
async def start_algo(payload: dict = Body(default={})):
    if payload:
        engine.configure(payload)
    try:
        return await asyncio.wait_for(engine.start(), timeout=START_API_TIMEOUT_SECONDS)
    except asyncio.TimeoutError as exc:
        message = f"Start request timed out after {START_API_TIMEOUT_SECONDS:g}s."
        engine.last_error = message
        engine.event("ERROR", message)
        raise HTTPException(status_code=504, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/stop")
async def stop_algo():
    return await engine.stop()


@app.post("/api/premarket-cache")
async def premarket_cache(payload: dict = Body(default={})):
    if payload:
        engine.configure(payload)
    await engine.run_premarket_cache(force=bool(payload.get("force", False)))
    return engine.snapshot()


@app.get("/api/premarket-report")
def premarket_report():
    return engine.premarket_report()


@app.post("/api/extract-symbols")
def parse_symbols(payload: dict = Body(...)):
    universe = set(payload.get("universe") or [])
    return {"symbols": extract_symbols(payload.get("text") or "", universe if universe else None)}


@app.post("/api/reconcile/{symbol}")
async def reconcile(symbol: str):
    try:
        return await engine.reconcile_missing_candles(symbol)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/broker-reconcile")
async def broker_reconcile():
    try:
        await asyncio.wait_for(engine.reconcile_broker_state(), timeout=BROKER_RECONCILE_TIMEOUT_SECONDS)
        return engine.snapshot()
    except asyncio.TimeoutError as exc:
        message = f"Broker reconcile timed out after {BROKER_RECONCILE_TIMEOUT_SECONDS:g}s."
        engine.entries_blocked_until_reconcile = True
        engine.broker_reconcile_status = {
            **engine.broker_reconcile_status,
            "running": False,
            "message": message,
            "entries_blocked_until_reconcile": True,
        }
        engine.event("WARN", message)
        raise HTTPException(status_code=504, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
