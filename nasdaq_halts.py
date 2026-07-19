"""Collect and parse official Nasdaq Trader historical halt records."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests


EASTERN = ZoneInfo("America/New_York")
SOURCE_PAGE = "https://www.nasdaqtrader.com/trader.aspx?id=TradingHaltHistory"
RPC_URL = "https://www.nasdaqtrader.com/RPCHandler.axd"
RPC_METHOD = "BL_TradeHalt.GetHaltsByDate"


class NasdaqHaltError(RuntimeError):
    """An official halt-source request or shape failed."""


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._row = []
        elif tag in {"td", "th"} and self._row is not None:
            self._cell = []

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._row is not None and self._cell is not None:
            self._row.append(" ".join("".join(self._cell).split()))
            self._cell = None
        elif tag == "tr" and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None


def parse_halt_html(value: str) -> list[dict[str, Any]]:
    parser = _TableParser()
    parser.feed(value)
    if not parser.rows:
        return []
    header = parser.rows[0]
    expected = [
        "Halt Date",
        "Halt Time",
        "Issue Symbol",
        "Issue Name",
        "Market",
        "Reason Code",
        "Pause Threshold Price",
        "Resumption Date",
        "Resumption Quote Time",
        "Resumption Trade Time",
    ]
    if header != expected:
        raise NasdaqHaltError("Nasdaq halt table header changed")
    records: list[dict[str, Any]] = []
    for values in parser.rows[1:]:
        if len(values) != len(header):
            raise NasdaqHaltError("Nasdaq halt row width changed")
        row = dict(zip(header, values))
        started = _market_timestamp(row["Halt Date"], row["Halt Time"])
        resumed = None
        if row["Resumption Date"] and row["Resumption Trade Time"]:
            resumed = _market_timestamp(
                row["Resumption Date"], row["Resumption Trade Time"]
            )
        records.append(
            {
                "halted_at_et": started.isoformat(),
                "symbol": row["Issue Symbol"].strip().upper(),
                "issue_name": row["Issue Name"],
                "market": row["Market"],
                "reason_code": row["Reason Code"],
                "pause_threshold_price": row["Pause Threshold Price"] or None,
                "resumed_at_et": resumed.isoformat() if resumed else None,
                "resumption_quote_time": row["Resumption Quote Time"] or None,
            }
        )
    return records


def _market_timestamp(day: str, clock: str) -> datetime:
    raw = f"{day} {clock}"
    for pattern in ("%m/%d/%Y %H:%M:%S.%f", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(raw, pattern).replace(tzinfo=EASTERN)
        except ValueError:
            continue
    raise NasdaqHaltError("Nasdaq halt timestamp format changed")


def halt_overlaps(
    record: dict[str, Any], symbol: str, start: datetime, end: datetime
) -> bool:
    if str(record.get("symbol")) != symbol.upper():
        return False
    halted = datetime.fromisoformat(str(record["halted_at_et"]))
    resumed = (
        datetime.fromisoformat(str(record["resumed_at_et"]))
        if record.get("resumed_at_et")
        else datetime.max.replace(tzinfo=EASTERN)
    )
    return halted <= end and resumed > start


class NasdaqHaltClient:
    def __init__(self, cache_root: Path, *, minimum_spacing_seconds: float = 0.25):
        self.cache_root = cache_root
        self.minimum_spacing_seconds = minimum_spacing_seconds
        self._last_request_at = 0.0
        self.cache_hits = 0
        self.downloads = 0

    def fetch_day(self, day: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        parsed = datetime.strptime(day, "%Y-%m-%d")
        path = self.cache_root / f"{day}.json"
        if path.exists():
            self.cache_hits += 1
            payload = json.loads(path.read_text(encoding="utf-8"))
        else:
            remaining = self.minimum_spacing_seconds - (
                time.monotonic() - self._last_request_at
            )
            if remaining > 0:
                time.sleep(remaining)
            response = requests.post(
                RPC_URL,
                headers={
                    "Content-Type": "application/json",
                    "Referer": SOURCE_PAGE,
                    "User-Agent": "robinhood-codex historical research ryan@ensomniac.com",
                },
                json={
                    "id": 1,
                    "method": RPC_METHOD,
                    "params": [parsed.strftime("%Y%m%d")],
                    "version": "1.1",
                },
                timeout=30,
            )
            self._last_request_at = time.monotonic()
            response.raise_for_status()
            payload = response.json()
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            temporary.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            os.replace(temporary, path)
            self.downloads += 1
        if not isinstance(payload, dict) or not isinstance(payload.get("result"), str):
            raise NasdaqHaltError("Nasdaq RPC response is malformed")
        records = parse_halt_html(payload["result"])
        return records, {
            "source_page": SOURCE_PAGE,
            "rpc_url": RPC_URL,
            "rpc_method": RPC_METHOD,
            "query_date": day,
            "cache_path": str(path),
            "record_count": len(records),
        }
