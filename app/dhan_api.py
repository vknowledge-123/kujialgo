import asyncio
import json
import time
from collections import deque
from datetime import datetime
from typing import Any

import requests

from .config import (
    DHAN_API_BASE_URL,
    FETCH_REQUESTS_PER_SECOND,
    FETCH_RETRY_ATTEMPTS,
    FETCH_RETRY_BASE_SECONDS,
    FETCH_RETRY_MAX_SECONDS,
    ORDER_REQUESTS_PER_SECOND,
)


class DhanRateLimitError(RuntimeError):
    pass


class DhanAuthenticationError(RuntimeError):
    pass


class AsyncRateLimiter:
    def __init__(self, calls_per_second: int):
        self.calls_per_second = max(1, int(calls_per_second))
        self.calls: deque[float] = deque()
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = time.monotonic()
            while self.calls and now - self.calls[0] >= 1:
                self.calls.popleft()
            if len(self.calls) >= self.calls_per_second:
                await asyncio.sleep(max(0.0, 1 - (now - self.calls[0])))
            self.calls.append(time.monotonic())


class DhanClient:
    def __init__(self, client_id: str, access_token: str, session: requests.Session | None = None):
        self.client_id = str(client_id or "").strip()
        self.access_token = str(access_token or "").strip()
        self.http = session or requests.Session()
        self.http.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "access-token": self.access_token,
                "client-id": self.client_id,
            }
        )
        self.fetch_limiter = AsyncRateLimiter(FETCH_REQUESTS_PER_SECOND)
        self.order_limiter = AsyncRateLimiter(ORDER_REQUESTS_PER_SECOND)

    async def get(self, path: str) -> dict[str, Any]:
        await self.fetch_limiter.wait()
        return await asyncio.to_thread(self._request, "GET", path, None)

    async def post(self, path: str, payload: dict[str, Any], order: bool = False) -> dict[str, Any]:
        limiter = self.order_limiter if order else self.fetch_limiter
        attempts = 3 if order else max(1, FETCH_RETRY_ATTEMPTS)
        for attempt in range(attempts):
            await limiter.wait()
            try:
                return await asyncio.to_thread(self._request, "POST", path, payload)
            except DhanRateLimitError:
                if attempt >= attempts - 1:
                    raise
                backoff = min(FETCH_RETRY_MAX_SECONDS, FETCH_RETRY_BASE_SECONDS * (2 ** attempt))
                await asyncio.sleep(backoff)
        return {}

    async def delete(self, path: str, order: bool = True) -> dict[str, Any]:
        await (self.order_limiter if order else self.fetch_limiter).wait()
        return await asyncio.to_thread(self._request, "DELETE", path, None)

    def _request(self, method: str, path: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        response = self.http.request(
            method,
            f"{DHAN_API_BASE_URL}{path}",
            json=payload,
            timeout=(5, 30),
        )
        if response.status_code == 429:
            raise DhanRateLimitError(f"Dhan API rate limited on {path}: {response.text[:300]}")
        if "DH-901" in response.text or "Invalid_Authentication" in response.text:
            raise DhanAuthenticationError(f"Dhan authentication failed on {path}: {response.text[:300]}")
        data: Any = {}
        if response.text:
            try:
                data = response.json()
            except ValueError:
                data = {}
            if isinstance(data, dict) and (data.get("errorCode") == "DH-904" or data.get("errorType") == "Rate_Limit"):
                raise DhanRateLimitError(f"Dhan API rate limited on {path}: {response.text[:300]}")
            if isinstance(data, dict) and (data.get("errorCode") == "DH-901" or data.get("errorType") == "Invalid_Authentication"):
                raise DhanAuthenticationError(f"Dhan authentication failed on {path}: {response.text[:300]}")
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(f"Dhan API {path} failed ({response.status_code}): {response.text[:500]}") from exc
        if not response.text:
            return {}
        if data == {}:
            data = response.json()
        if isinstance(data, dict) and data.get("status") not in (None, "success"):
            raise RuntimeError(data.get("remarks") or data.get("message") or json.dumps(data)[:500])
        return data

    async def intraday(
        self,
        security_id: str,
        interval: int,
        from_dt: datetime,
        to_dt: datetime,
        exchange_segment: str = "NSE_EQ",
        instrument: str = "EQUITY",
    ) -> list[dict[str, Any]]:
        payload = {
            "securityId": str(security_id),
            "exchangeSegment": exchange_segment,
            "instrument": instrument,
            "interval": str(interval),
            "oi": False,
            "fromDate": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "toDate": to_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }
        data = await self.post("/charts/intraday", payload)
        return candles_from_response(data)

    async def daily(
        self,
        security_id: str,
        from_date: datetime,
        to_date: datetime,
        exchange_segment: str = "NSE_EQ",
        instrument: str = "EQUITY",
    ) -> list[dict[str, Any]]:
        payload = {
            "securityId": str(security_id),
            "exchangeSegment": exchange_segment,
            "instrument": instrument,
            "expiryCode": 0,
            "oi": False,
            "fromDate": from_date.date().isoformat(),
            "toDate": to_date.date().isoformat(),
        }
        data = await self.post("/charts/historical", payload)
        return candles_from_response(data)

    async def market_quote(self, security_ids: list[str]) -> dict[str, Any]:
        securities = {"NSE_EQ": [int(sid) for sid in security_ids if str(sid).isdigit()]}
        return await self.post("/marketfeed/quote", securities)

    async def place_market_order(self, security_id: str, transaction_type: str, quantity: int, correlation_id: str) -> dict[str, Any]:
        payload = {
            "dhanClientId": self.client_id,
            "correlationId": correlation_id[:30],
            "transactionType": transaction_type.upper(),
            "exchangeSegment": "NSE_EQ",
            "productType": "INTRADAY",
            "orderType": "MARKET",
            "validity": "DAY",
            "securityId": str(security_id),
            "quantity": int(quantity),
            "disclosedQuantity": 0,
            "price": 0,
            "triggerPrice": 0,
            "afterMarketOrder": False,
            "amoTime": "",
            "boProfitValue": "",
            "boStopLossValue": "",
        }
        return await self.post("/orders", payload, order=True)

    async def cancel_order(self, order_id: str) -> dict[str, Any]:
        return await self.delete(f"/orders/{order_id}", order=True)

    async def order_by_id(self, order_id: str) -> dict[str, Any]:
        return await self.get(f"/orders/{order_id}")


def candles_from_response(data: dict[str, Any]) -> list[dict[str, Any]]:
    payload = data.get("data", data) if isinstance(data, dict) else data
    if isinstance(payload, dict) and isinstance(payload.get("data"), (dict, list)):
        payload = payload["data"]
    if isinstance(payload, list):
        return [normalize_candle_row(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        return []
    timestamps = payload.get("timestamp") or payload.get("time") or []
    opens = payload.get("open") or []
    highs = payload.get("high") or []
    lows = payload.get("low") or []
    closes = payload.get("close") or []
    volumes = payload.get("volume") or []
    rows = []
    for index, ts in enumerate(timestamps):
        try:
            rows.append(
                {
                    "timestamp": ts,
                    "open": float(opens[index]),
                    "high": float(highs[index]),
                    "low": float(lows[index]),
                    "close": float(closes[index]),
                    "volume": int(float(volumes[index] if index < len(volumes) else 0)),
                }
            )
        except (IndexError, TypeError, ValueError):
            continue
    return rows


def normalize_candle_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": row.get("timestamp") or row.get("time") or row.get("start"),
        "open": float(row.get("open") or row.get("o") or 0),
        "high": float(row.get("high") or row.get("h") or 0),
        "low": float(row.get("low") or row.get("l") or 0),
        "close": float(row.get("close") or row.get("c") or 0),
        "volume": int(float(row.get("volume") or row.get("v") or 0)),
    }
