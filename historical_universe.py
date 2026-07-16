"""Freeze a replay universe only after availability-only IBKR preflight.

The draft manifest contains a ranked buffer under ``candidate_pool_by_date``.
This tool resolves symbols in rank order and freezes the first required viable
names without requesting target-session price data. Retired, unresolvable, or
pre-session-history-incomplete symbols may be skipped before the universe is
frozen; provider-wide failures still stop the batch.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ibkr_historical import (
    DEFAULT_ENV_PATH,
    IBKRConfig,
    IBKRHistoricalClient,
    IBKRHistoricalError,
    probe_historical_candidate,
)


class HistoricalUniverseError(RuntimeError):
    """Raised when a draft pool cannot become a valid frozen universe."""


Probe = Callable[[str, str], Mapping[str, Any]]


def _canonical_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _positive_integer(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise HistoricalUniverseError(f"{name} must be a positive integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise HistoricalUniverseError(f"{name} must be a positive integer") from exc
    if result <= 0 or not math.isfinite(float(result)):
        raise HistoricalUniverseError(f"{name} must be a positive integer")
    return result


def _candidate_symbol(value: Any, *, day: str, rank: int) -> str:
    if not isinstance(value, Mapping):
        raise HistoricalUniverseError(f"{day} candidate rank {rank} must be an object")
    symbol = value.get("symbol")
    if not isinstance(symbol, str):
        raise HistoricalUniverseError(
            f"{day} candidate rank {rank} needs a string symbol"
        )
    normalized = symbol.strip().upper()
    if not normalized or not normalized.replace(".", "").isalnum():
        raise HistoricalUniverseError(
            f"{day} candidate rank {rank} has an invalid symbol"
        )
    return normalized


def freeze_candidate_universe(
    draft: Mapping[str, Any],
    probe: Probe,
    *,
    minimum_candidates: int = 10,
    performed_at: str | None = None,
) -> dict[str, Any]:
    """Return a frozen builder manifest from a larger ranked draft pool."""
    required = _positive_integer(minimum_candidates, "minimum_candidates")
    scanner = draft.get("scanner")
    pools = draft.get("candidate_pool_by_date")
    if not isinstance(scanner, Mapping) or not isinstance(pools, Mapping):
        raise HistoricalUniverseError(
            "draft manifest needs scanner and candidate_pool_by_date objects"
        )
    if not pools:
        raise HistoricalUniverseError("candidate_pool_by_date cannot be empty")

    candidates_by_date: dict[str, list[dict[str, Any]]] = {}
    date_reports: dict[str, dict[str, Any]] = {}
    for day, raw_pool in pools.items():
        if not isinstance(day, str):
            raise HistoricalUniverseError("candidate-pool dates must be strings")
        try:
            date.fromisoformat(day)
        except ValueError as exc:
            raise HistoricalUniverseError(
                f"invalid candidate-pool date: {day}"
            ) from exc
        if not isinstance(raw_pool, list) or len(raw_pool) < required:
            size = len(raw_pool) if isinstance(raw_pool, list) else 0
            raise HistoricalUniverseError(
                f"{day} draft pool has {size} candidates; at least {required} are required"
            )

        accepted: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        seen: set[str] = set()
        last_examined = -1
        for index, raw_candidate in enumerate(raw_pool):
            rank = index + 1
            symbol = _candidate_symbol(raw_candidate, day=day, rank=rank)
            if symbol in seen:
                raise HistoricalUniverseError(
                    f"{day} draft pool repeats symbol {symbol}"
                )
            seen.add(symbol)
            last_examined = index
            result = probe(symbol, day)
            if str(result.get("symbol", "")).upper() != symbol:
                raise HistoricalUniverseError(
                    f"{day} {symbol} preflight returned a mismatched symbol"
                )
            if result.get("viable") is True:
                candidate = deepcopy(dict(raw_candidate))
                candidate["symbol"] = symbol
                accepted.append(candidate)
                if len(accepted) == required:
                    break
                continue
            reason = result.get("reason")
            if not isinstance(reason, str) or not reason:
                raise HistoricalUniverseError(
                    f"{day} {symbol} preflight rejection needs a reason"
                )
            skipped.append(
                {
                    "symbol": symbol,
                    "draft_rank": rank,
                    "reason": reason,
                    "error_code": result.get("error_code"),
                }
            )

        if len(accepted) < required:
            rejected = ", ".join(row["symbol"] for row in skipped) or "none"
            raise HistoricalUniverseError(
                f"{day} exhausted its draft pool with only {len(accepted)} viable "
                f"candidates; {required} are required (rejected: {rejected})"
            )
        unused = [
            _candidate_symbol(value, day=day, rank=index + 1)
            for index, value in enumerate(
                raw_pool[last_examined + 1 :], last_examined + 1
            )
        ]
        candidates_by_date[day] = accepted
        date_reports[day] = {
            "accepted_symbols": [row["symbol"] for row in accepted],
            "skipped": skipped,
            "unused_buffer_symbols": unused,
        }

    output = {
        key: deepcopy(value)
        for key, value in draft.items()
        if key != "candidate_pool_by_date"
    }
    output["candidates_by_date"] = candidates_by_date
    output["preflight"] = {
        "schema_version": 1,
        "performed_at": performed_at or datetime.now(UTC).isoformat(),
        "provider": "Interactive Brokers TWS API pre-session history",
        "minimum_candidates": required,
        "availability_only": True,
        "target_session_prices_observed": False,
        "draft_manifest_sha256": _canonical_hash(draft),
        "dates": date_reports,
    }
    return output


def _write_json(value: Mapping[str, Any], output: Path | None) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if output is None:
        print(rendered, end="")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(json.dumps({"written": str(output), "bytes": len(rendered.encode())}))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("draft", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum-candidates", type=int, default=10)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        draft = json.loads(args.draft.read_text(encoding="utf-8"))
        if not isinstance(draft, Mapping):
            raise HistoricalUniverseError("draft manifest must be a JSON object")
        config = IBKRConfig.from_env(args.env_file)
        with IBKRHistoricalClient(config) as client:
            frozen = freeze_candidate_universe(
                draft,
                lambda symbol, day: probe_historical_candidate(client, symbol, day),
                minimum_candidates=args.minimum_candidates,
            )
        _write_json(frozen, args.output)
        return 0
    except (
        HistoricalUniverseError,
        IBKRHistoricalError,
        OSError,
        json.JSONDecodeError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
