import csv
import io
import re
import time
from pathlib import Path

import requests

from .config import DHAN_SCRIP_MASTER_URL, SCRIP_MASTER_FILE
from .models import Instrument

KNOWN_INDICES = {
    "NIFTY", "NIFTY50", "NIFTY 50", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX",
}

COMMON_NSE_SYMBOLS = {
    "360ONE", "ABB", "APLAPOLLO", "AUBANK", "ADANIENT", "ADANIPORTS", "ADANIENSOL",
    "ANGELONE", "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT", "AUROPHARMA", "AXISBANK",
    "BAJAJ-AUTO", "BAJFINANCE", "BAJAJFINSV", "BANDHANBNK", "BANKBARODA", "BEL",
    "BHARATFORG", "BHEL", "BHARTIARTL", "BIOCON", "BOSCHLTD", "BPCL", "BRITANNIA",
    "BSE", "CANBK", "CDSL", "CHOLAFIN", "CIPLA", "COALINDIA", "COFORGE", "COLPAL",
    "CONCOR", "CUMMINSIND", "DABUR", "DIVISLAB", "DIXON", "DLF", "DRREDDY",
    "EICHERMOT", "ETERNAL", "FEDERALBNK", "GAIL", "GLENMARK", "GODREJCP", "GRASIM",
    "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO", "HINDALCO",
    "HINDPETRO", "HINDUNILVR", "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDEA", "IDFCFIRSTB",
    "IEX", "INDHOTEL", "INDIGO", "INDUSINDBK", "INFY", "IOC", "IRCTC", "ITC",
    "JINDALSTEL", "JIOFIN", "JSWSTEEL", "JUBLFOOD", "KALYANKJIL", "KOTAKBANK", "KPITTECH",
    "LAURUSLABS", "LICHSGFIN", "LT", "LTIM", "LUPIN", "M&M", "MANAPPURAM", "MARICO",
    "MARUTI", "MAXHEALTH", "MCX", "MOTHERSON", "MPHASIS", "MUTHOOTFIN", "NATIONALUM",
    "NAUKRI", "NESTLEIND", "NMDC", "NTPC", "OBEROIRLTY", "OFSS", "ONGC", "PAYTM",
    "PERSISTENT", "PETRONET", "PFC", "PIDILITIND", "PNB", "POLICYBZR", "POLYCAB",
    "POWERGRID", "RECLTD", "RELIANCE", "SAIL", "SBICARD", "SBILIFE", "SBIN",
    "SHRIRAMFIN", "SIEMENS", "SRF", "SUNPHARMA", "SYNGENE", "TATACONSUM", "TATAMOTORS",
    "TATAPOWER", "TATASTEEL", "TCS", "TECHM", "TITAN", "TORNTPHARM", "TRENT",
    "TVSMOTOR", "ULTRACEMCO", "UNITDSPR", "UPL", "VBL", "VEDL", "VOLTAS", "WIPRO",
    "YESBANK", "ZYDUSLIFE",
}

FALLBACK_SECTOR_MEMBERS = {
    "NIFTY AUTO": [
        "ASHOKLEY", "BAJAJ-AUTO", "BALKRISIND", "BHARATFORG", "BOSCHLTD",
        "EICHERMOT", "EXIDEIND", "HEROMOTOCO", "M&M", "MARUTI",
        "MOTHERSON", "MRF", "TATAMOTORS", "TMPV", "TIINDIA", "TVSMOTOR",
    ],
    "NIFTY BANK": [
        "AUBANK", "AXISBANK", "BANDHANBNK", "BANKBARODA", "FEDERALBNK",
        "HDFCBANK", "ICICIBANK", "IDFCFIRSTB", "INDUSINDBK", "KOTAKBANK",
        "PNB", "SBIN",
    ],
    "NIFTY CAPITAL MKT": [
        "ANGELONE", "BSE", "CDSL", "CAMS", "IEX", "MCX", "MOTILALOFS",
        "NUVAMA", "360ONE", "KFINTECH", "HDFCAMC", "NAM-INDIA",
    ],
    "NIFTY CONSR DURBL": [
        "AMBER", "BATAINDIA", "BLUESTARCO", "CROMPTON", "DIXON",
        "HAVELLS", "KAJARIACER", "KALYANKJIL", "PGEL", "TITAN",
        "VOLTAS", "WHIRLPOOL",
    ],
    "NIFTY CPSE": [
        "BEL", "BHEL", "COALINDIA", "CONCOR", "GAIL", "HAL", "IRCTC",
        "NHPC", "NMDC", "NTPC", "OIL", "ONGC", "POWERGRID", "SAIL",
    ],
    "NIFTY ENERGY": [
        "ADANIENSOL", "ADANIGREEN", "ADANIPOWER", "BPCL", "CGPOWER",
        "COALINDIA", "GAIL", "IOC", "JSWENERGY", "NTPC", "ONGC",
        "POWERGRID", "RELIANCE", "TATAPOWER", "TORNTPOWER",
    ],
    "NIFTY FINSEREXBNK": [
        "BAJAJFINSV", "BAJFINANCE", "CHOLAFIN", "HDFCAMC", "HDFCLIFE",
        "ICICIGI", "ICICIPRULI", "JIOFIN", "LICHSGFIN", "MUTHOOTFIN",
        "PFC", "RECLTD", "SBICARD", "SBILIFE", "SHRIRAMFIN",
    ],
    "NIFTY FMCG": [
        "BALRAMCHIN", "BRITANNIA", "COLPAL", "DABUR", "GODREJCP",
        "HINDUNILVR", "ITC", "MARICO", "NESTLEIND", "PATANJALI",
        "PGHH", "RADICO", "TATACONSUM", "UBL", "UNITDSPR", "VBL",
    ],
    "NIFTY HEALTHCARE": [
        "ABBOTINDIA", "ALKEM", "APOLLOHOSP", "AUROPHARMA", "BIOCON",
        "CIPLA", "DIVISLAB", "DRREDDY", "FORTIS", "GLENMARK",
        "IPCALAB", "LAURUSLABS", "LUPIN", "MANKIND", "MAXHEALTH",
        "SUNPHARMA", "SYNGENE", "TORNTPHARM", "ZYDUSLIFE",
    ],
    "NIFTY IND DEFENCE": [
        "ASTRAMICRO", "BEML", "BEL", "BDL", "COCHINSHIP", "DATAPATTNS",
        "GRSE", "HAL", "MAZDOCK", "MTARTECH", "PARAS", "SOLARINDS",
    ],
    "NIFTY IND DIGITAL": [
        "AFFLE", "BHARTIARTL", "BSOFT", "CYIENT", "DELHIVERY", "EASEMYTRIP",
        "ETERNAL", "HAPPSTMNDS", "INDIAMART", "INFOBEAN", "INTELLECT",
        "JUSTDIAL", "KPITTECH", "NAUKRI", "NYKAA", "PAYTM", "POLICYBZR",
        "TANLA", "TATAELXSI",
    ],
    "NIFTY IND TOURISM": [
        "CHALET", "EASEMYTRIP", "EIHOTEL", "INDHOTEL", "IRCTC", "JUBLFOOD",
        "LEMONTREE", "SAPPHIRE", "DEVYANI", "WESTLIFE",
    ],
    "NIFTY INDIA MFG": [
        "ABB", "APLAPOLLO", "ASHOKLEY", "BAJAJ-AUTO", "BALKRISIND",
        "BEL", "BHARATFORG", "BHEL", "CIPLA", "DIXON", "DRREDDY",
        "HAL", "HAVELLS", "HINDALCO", "JSWSTEEL", "LT", "MARUTI",
        "MOTHERSON", "PIDILITIND", "SIEMENS", "SUNPHARMA", "TATAMOTORS",
        "TATASTEEL", "TITAN", "TVSMOTOR", "ULTRACEMCO", "VOLTAS",
    ],
    "NIFTY IT": [
        "COFORGE", "HCLTECH", "INFY", "LTIM", "MPHASIS",
        "OFSS", "PERSISTENT", "TCS", "TECHM", "WIPRO",
    ],
    "NIFTY MEDIA": [
        "DISHTV", "HATHWAY", "NAZARA", "NETWORK18", "PVRINOX",
        "SAREGAMA", "SUNTV", "TIPSINDLTD", "TV18BRDCST", "ZEEL",
    ],
    "NIFTY METAL": [
        "ADANIENT", "APLAPOLLO", "HINDALCO", "HINDCOPPER", "HINDZINC",
        "JINDALSTEL", "JSL", "JSWSTEEL", "NATIONALUM", "NMDC",
        "RATNAMANI", "SAIL", "TATASTEEL", "VEDL", "WELCORP",
    ],
    "NIFTY MIDSML HLTH": [
        "AJANTPHARM", "ASTERDM", "BIOCON", "GLENMARK", "IPCALAB",
        "LALPATHLAB", "LAURUSLABS", "METROPOLIS", "NATCOPHARM",
        "SUVENPHAR", "SYNGENE", "VIJAYA", "WOCKPHARMA",
    ],
    "NIFTY MS FIN SERV": [
        "AAVAS", "ANGELONE", "BSE", "CAMS", "CDSL", "CHOLAFIN",
        "CREDITACC", "FIVESTAR", "IIFL", "KFINTECH", "LICHSGFIN",
        "MANAPPURAM", "MCX", "MUTHOOTFIN", "POONAWALLA", "RBLBANK",
    ],
    "NIFTY MS IT TELCM": [
        "AFFLE", "BSOFT", "CYIENT", "HAPPSTMNDS", "INTELLECT", "KPITTECH",
        "LATENTVIEW", "MASTEK", "NEWGEN", "OFSS", "PERSISTENT", "TANLA",
        "TATAELXSI", "ZENSARTECH",
    ],
    "NIFTY OIL AND GAS": [
        "AEGISLOG", "ATGL", "BPCL", "CASTROLIND", "GAIL", "GUJGASLTD",
        "GSPL", "HINDPETRO", "IOC", "IGL", "MGL", "OIL", "ONGC",
        "PETRONET", "RELIANCE",
    ],
    "NIFTY PHARMA": [
        "ABBOTINDIA", "ALKEM", "AUROPHARMA", "BIOCON", "CIPLA",
        "DIVISLAB", "DRREDDY", "GLAND", "GLENMARK", "IPCALAB",
        "LAURUSLABS", "LUPIN", "MANKIND", "SUNPHARMA", "TORNTPHARM",
        "ZYDUSLIFE",
    ],
    "NIFTY PSU BANK": [
        "BANKBARODA", "BANKINDIA", "CANBK", "CENTRALBK", "INDIANB",
        "IOB", "MAHABANK", "PNB", "PSB", "SBIN", "UCOBANK", "UNIONBANK",
    ],
    "NIFTY PVT BANK": [
        "AUBANK", "AXISBANK", "BANDHANBNK", "CUB", "DCBBANK", "FEDERALBNK",
        "HDFCBANK", "ICICIBANK", "IDFCFIRSTB", "INDUSINDBK", "KARURVYSYA",
        "KOTAKBANK", "RBLBANK", "YESBANK",
    ],
}

SECTOR_INDEX_INSTRUMENTS = {
    "NIFTY AUTO": Instrument(symbol="NIFTY AUTO", security_id="14", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY IT": Instrument(symbol="NIFTY IT", security_id="29", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY METAL": Instrument(symbol="NIFTY METAL", security_id="31", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY FINSEREXBNK": Instrument(symbol="NIFTY FINSEREXBNK", security_id="495", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY MS FIN SERV": Instrument(symbol="NIFTY MS FIN SERV", security_id="819", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY HEALTHCARE": Instrument(symbol="NIFTY HEALTHCARE", security_id="447", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY MIDSML HLTH": Instrument(symbol="NIFTY MIDSML HLTH", security_id="471", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY PSU BANK": Instrument(symbol="NIFTY PSU BANK", security_id="33", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY CONSR DURBL": Instrument(symbol="NIFTY CONSR DURBL", security_id="466", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY FMCG": Instrument(symbol="NIFTY FMCG", security_id="28", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY PVT BANK": Instrument(symbol="NIFTY PVT BANK", security_id="15", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY ENERGY": Instrument(symbol="NIFTY ENERGY", security_id="42", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY CPSE": Instrument(symbol="NIFTY CPSE", security_id="45", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY BANK": Instrument(symbol="NIFTY BANK", security_id="25", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY MS IT TELCM": Instrument(symbol="NIFTY MS IT TELCM", security_id="821", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY IND DEFENCE": Instrument(symbol="NIFTY IND DEFENCE", security_id="493", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY MEDIA": Instrument(symbol="NIFTY MEDIA", security_id="30", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY IND DIGITAL": Instrument(symbol="NIFTY IND DIGITAL", security_id="473", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY PHARMA": Instrument(symbol="NIFTY PHARMA", security_id="32", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY IND TOURISM": Instrument(symbol="NIFTY IND TOURISM", security_id="815", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY CAPITAL MKT": Instrument(symbol="NIFTY CAPITAL MKT", security_id="803", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY OIL AND GAS": Instrument(symbol="NIFTY OIL AND GAS", security_id="470", exchange_segment="IDX_I", instrument="INDEX"),
    "NIFTY INDIA MFG": Instrument(symbol="NIFTY INDIA MFG", security_id="474", exchange_segment="IDX_I", instrument="INDEX"),
}

_NOISE_WORDS = {
    "SYMBOL", "OPEN", "HIGH", "LOW", "PREV", "CLOSE", "LTP", "INDICATIVE", "CHANGE",
    "VOLUME", "SHARES", "VALUE", "CRORES", "MARKETCAP", "SECTOR", "DATE", "LARGECAP",
    "MIDCAP", "SMALLCAP", "BANK", "HTML", "STOCKS", "HTTP", "HTTPS", "CHARTINK", "NSE",
    "AM", "PM",
}


def normalize_symbol(value: str) -> str:
    text = str(value or "").strip().upper()
    text = re.sub(r"\.(NS|BO)$", "", text)
    text = text.replace("&AMP;", "&")
    return text


def extract_symbols(raw: str, universe: set[str] | None = None) -> list[str]:
    allowed = None
    if universe:
        allowed = {normalize_symbol(s) for s in universe if s}
        allowed.update(COMMON_NSE_SYMBOLS)
    tokens = re.findall(r"(?<![A-Z0-9&-])([A-Z][A-Z0-9&-]{1,19})(?![A-Z0-9&-])", str(raw or "").upper())
    found: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        symbol = normalize_symbol(token)
        if symbol in _NOISE_WORDS or symbol in KNOWN_INDICES:
            continue
        if allowed is not None and symbol not in allowed:
            continue
        if symbol not in seen:
            found.append(symbol)
            seen.add(symbol)
    return found


class InstrumentResolver:
    def __init__(self, path: Path = SCRIP_MASTER_FILE):
        self.path = path
        self.loaded_at = 0.0
        self.by_symbol: dict[str, Instrument] = {}

    def load(self, force_download: bool = False) -> dict[str, Instrument]:
        if force_download or not self.path.exists():
            self.download()
        if self.by_symbol and time.time() - self.loaded_at < 3600:
            return self.by_symbol
        if not self.path.exists():
            self.by_symbol = {}
            return self.by_symbol
        rows = self.path.read_text(encoding="utf-8", errors="ignore")
        self.by_symbol = self._parse(rows)
        self.loaded_at = time.time()
        return self.by_symbol

    def download(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(DHAN_SCRIP_MASTER_URL, timeout=(10, 60))
        response.raise_for_status()
        self.path.write_bytes(response.content)
        self.loaded_at = 0.0

    def resolve(self, symbols: list[str]) -> tuple[list[Instrument], list[str]]:
        table = self.load()
        resolved: list[Instrument] = []
        missing: list[str] = []
        for symbol in symbols:
            item = table.get(normalize_symbol(symbol))
            if item:
                resolved.append(item)
            else:
                missing.append(symbol)
        return resolved, missing

    def _parse(self, content: str) -> dict[str, Instrument]:
        parsed: dict[str, Instrument] = {}
        reader = csv.DictReader(io.StringIO(content))
        for row in reader:
            exchange = (row.get("SEM_EXM_EXCH_ID") or row.get("EXCH_ID") or "").upper()
            segment_code = (row.get("SEM_SEGMENT") or row.get("SEGMENT") or "").upper()
            instrument_name = (row.get("SEM_INSTRUMENT_NAME") or row.get("INSTRUMENT") or "").upper()
            if exchange != "NSE" or segment_code not in {"E", "NSE_EQ", "EQ"}:
                continue
            if instrument_name and instrument_name not in {"EQUITY", "EQ"}:
                continue
            symbol = normalize_symbol(row.get("SEM_TRADING_SYMBOL") or row.get("SM_SYMBOL_NAME") or row.get("SYMBOL_NAME"))
            security_id = str(row.get("SEM_SMST_SECURITY_ID") or row.get("SECURITY_ID") or row.get("SECURITY_ID_") or "").strip()
            if not symbol or not security_id:
                continue
            parsed.setdefault(symbol, Instrument(symbol=symbol, security_id=security_id))
        return parsed


def sector_for_symbol(symbol: str) -> str:
    sectors = sectors_for_symbol(symbol)
    return sectors[0] if sectors else ""


def sectors_for_symbol(symbol: str) -> list[str]:
    symbol = normalize_symbol(symbol)
    sectors = []
    for sector, members in FALLBACK_SECTOR_MEMBERS.items():
        if symbol in members:
            sectors.append(sector)
    return sectors


def sector_memberships() -> dict[str, list[str]]:
    return FALLBACK_SECTOR_MEMBERS.copy()


def sector_index_instruments() -> dict[str, Instrument]:
    return {name: Instrument(item.symbol, item.security_id, item.exchange_segment, item.instrument) for name, item in SECTOR_INDEX_INSTRUMENTS.items()}
