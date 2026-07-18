"""Freeze a replay universe only after pre-session IBKR eligibility checks.

The draft manifest contains a ranked buffer under ``candidate_pool_by_date``.
This tool resolves symbols in rank order and freezes the first required viable
names without requesting target-session price data. It applies immutable daily
volume and ATR universe gates before the more expensive opening-history pull.
Retired, unresolvable, ineligible, or pre-session-history-incomplete symbols may
be skipped before the universe is frozen; provider-wide failures still stop the
batch.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import threading
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from time import monotonic
from typing import Any

from historical_concurrency import ordered_bounded_results
from ibkr_historical import (
    DEFAULT_CONTRACT_CACHE_ROOT,
    DEFAULT_ENV_PATH,
    IBKRConfig,
    IBKRHistoricalClient,
    IBKRHistoricalError,
    PRE_SESSION_CACHE_VERSION,
    probe_historical_candidate_with_history,
)
from strategy_engine import StrategyInputError, load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PREFLIGHT_CACHE_ROOT = PROJECT_ROOT / "historical_data" / "preflight"
PREFLIGHT_CACHE_SCHEMA_VERSION = 2
DEFAULT_PREFLIGHT_WORKERS = 4


class HistoricalUniverseError(RuntimeError):
    """Raised when a draft pool cannot become a valid frozen universe."""


Probe = Callable[[str, str], Mapping[str, Any]]
DetailedProbe = Callable[[str, str], tuple[Mapping[str, Any], Mapping[str, Any] | None]]


class ResumablePreflightProbe:
    """Persist every immutable result before its worker reports completion."""

    def __init__(
        self,
        cache_root: Path,
        probe: DetailedProbe,
        *,
        qualification: Mapping[str, Any] | None = None,
    ):
        self.cache_root = cache_root
        self.probe = probe
        self.qualification = dict(qualification or {})
        self.qualification_sha256 = _canonical_hash(self.qualification)
        self._stats_lock = threading.Lock()
        self._cache_hits = 0
        self._cache_misses = 0
        self._cold_probe_seconds = 0.0

    def _path(self, symbol: str, day: str) -> Path:
        return self.cache_root / day / f"{symbol}.json"

    def _cached(self, path: Path, symbol: str, day: str) -> Mapping[str, Any] | None:
        if not path.exists():
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(value, Mapping):
            return None
        if (
            value.get("schema_version") != PREFLIGHT_CACHE_SCHEMA_VERSION
            or value.get("probe_contract_version") != PRE_SESSION_CACHE_VERSION
            or value.get("symbol") != symbol
            or value.get("session_date") != day
            or value.get("qualification_sha256") != self.qualification_sha256
        ):
            return None
        result = value.get("result")
        if not isinstance(result, Mapping) or result.get("symbol") != symbol:
            return None
        if result.get("viable") is True:
            history = value.get("pre_session_history")
            if not isinstance(history, Mapping):
                return None
            expected_hash = result.get("pre_session_history_sha256")
            if not isinstance(expected_hash, str) or expected_hash != _canonical_hash(
                history
            ):
                return None
        return dict(result)

    @staticmethod
    def _status(result: Mapping[str, Any]) -> str:
        if result.get("viable") is True:
            return "ready"
        return f"skipped: {result.get('reason', 'unknown')}"

    def __call__(self, symbol: str, day: str) -> Mapping[str, Any]:
        path = self._path(symbol, day)
        cached = self._cached(path, symbol, day)
        if cached is not None:
            with self._stats_lock:
                self._cache_hits += 1
            print(
                f"  {day} {symbol} cached {self._status(cached)}",
                file=sys.stderr,
                flush=True,
            )
            return cached
        started = monotonic()
        with self._stats_lock:
            self._cache_misses += 1
        result, history = self.probe(symbol, day)
        duration_seconds = monotonic() - started
        with self._stats_lock:
            self._cold_probe_seconds += duration_seconds
        normalized = dict(result)
        if normalized.get("viable") is True:
            if not isinstance(history, Mapping):
                raise HistoricalUniverseError(
                    f"{day} {symbol} viable preflight omitted reusable history"
                )
            normalized["pre_session_history_sha256"] = _canonical_hash(history)
        payload = {
            "schema_version": PREFLIGHT_CACHE_SCHEMA_VERSION,
            "probe_contract_version": PRE_SESSION_CACHE_VERSION,
            "provider": "Interactive Brokers TWS API pre-session history",
            "captured_at": datetime.now(UTC).isoformat(),
            "symbol": symbol,
            "session_date": day,
            "qualification": self.qualification,
            "qualification_sha256": self.qualification_sha256,
            "duration_seconds": duration_seconds,
            "result": normalized,
            "pre_session_history": dict(history) if history is not None else None,
        }
        rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)
        print(
            f"  {day} {symbol} {self._status(normalized)}",
            file=sys.stderr,
            flush=True,
        )
        return normalized

    def stats(self) -> dict[str, Any]:
        with self._stats_lock:
            return {
                "cache_hits": self._cache_hits,
                "cache_misses": self._cache_misses,
                "cold_probe_symbol_seconds": self._cold_probe_seconds,
            }


def _canonical_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _is_sha256(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        bytes.fromhex(value)
    except ValueError:
        return False
    return True


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
    cache_metadata: Mapping[str, Any] | None = None,
    max_workers: int = 1,
    continue_on_exhausted: bool = False,
) -> dict[str, Any]:
    """Return a frozen builder manifest from a larger ranked draft pool."""
    required = _positive_integer(minimum_candidates, "minimum_candidates")
    workers = _positive_integer(max_workers, "max_workers")
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
            if continue_on_exhausted:
                date_reports[day] = {
                    "accepted_symbols": [],
                    "accepted": [],
                    "skipped": [],
                    "unused_buffer_symbols": [],
                    "examined_count": 0,
                    "max_workers": workers,
                    "speculatively_cached_symbols": [],
                    "blocked": True,
                    "blocked_reason": (
                        f"draft_pool_exhausted:{size}_of_{required}_required"
                    ),
                }
                continue
            raise HistoricalUniverseError(
                f"{day} draft pool has {size} candidates; at least {required} are required"
            )

        prepared: list[tuple[int, str, Mapping[str, Any]]] = []
        all_symbols: set[str] = set()
        for index, raw_candidate in enumerate(raw_pool):
            rank = index + 1
            symbol = _candidate_symbol(raw_candidate, day=day, rank=rank)
            if symbol in all_symbols:
                raise HistoricalUniverseError(
                    f"{day} draft pool repeats symbol {symbol}"
                )
            all_symbols.add(symbol)
            prepared.append((index, symbol, raw_candidate))

        accepted: list[dict[str, Any]] = []
        accepted_preflight: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        last_examined = -1
        provider_started: list[tuple[int, str]] = []
        provider_started_lock = threading.Lock()

        def run_preflight(
            item: tuple[int, str, Mapping[str, Any]],
        ) -> dict[str, Any]:
            index, symbol, raw_candidate = item
            rank = index + 1
            pre_session_reason = None
            if raw_candidate.get("is_common_stock") is not True:
                pre_session_reason = "not_us_listed_common_stock"
            elif raw_candidate.get("dilution_conflict") is True:
                pre_session_reason = "known_dilution_conflict"
            if pre_session_reason is not None:
                return {
                    "symbol": symbol,
                    "draft_rank": rank,
                    "local_skip_reason": pre_session_reason,
                    "provider_called": False,
                }
            with provider_started_lock:
                provider_started.append((index, symbol))
            result = probe(symbol, day)
            return {
                "symbol": symbol,
                "draft_rank": rank,
                "result": dict(result),
                "provider_called": True,
            }

        outcomes = ordered_bounded_results(
            prepared,
            run_preflight,
            max_workers=workers,
        )
        try:
            for outcome in outcomes:
                index, symbol, raw_candidate = outcome.item
                rank = index + 1
                last_examined = index
                value = outcome.unwrap()
                local_skip_reason = value.get("local_skip_reason")
                if isinstance(local_skip_reason, str):
                    skipped.append(
                        {
                            "symbol": symbol,
                            "draft_rank": rank,
                            "reason": local_skip_reason,
                            "error_code": None,
                        }
                    )
                    continue
                result = value["result"]
                if str(result.get("symbol", "")).upper() != symbol:
                    raise HistoricalUniverseError(
                        f"{day} {symbol} preflight returned a mismatched symbol"
                    )
                if result.get("viable") is True:
                    history_hash = result.get("pre_session_history_sha256")
                    if (
                        isinstance(cache_metadata, Mapping)
                        and cache_metadata.get("reusable_pre_session_history") is True
                        and not _is_sha256(history_hash)
                    ):
                        raise HistoricalUniverseError(
                            f"{day} {symbol} preflight omitted its history hash"
                        )
                    candidate = deepcopy(dict(raw_candidate))
                    candidate["symbol"] = symbol
                    accepted.append(candidate)
                    accepted_preflight.append(
                        {
                            key: deepcopy(result[key])
                            for key in (
                                "symbol",
                                "reason",
                                "prior_opening_sessions",
                                "prior_daily_sessions",
                                "average_daily_volume_14",
                                "daily_atr_14",
                                "pre_session_history_sha256",
                            )
                            if key in result
                        }
                        | {"draft_rank": rank}
                    )
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
        finally:
            outcomes.close()

        unused = [symbol for index, symbol, _ in prepared if index > last_examined]
        speculative = sorted(
            (
                (index, symbol)
                for index, symbol in provider_started
                if index > last_examined
            ),
            key=lambda value: value[0],
        )
        report = {
            "accepted_symbols": [row["symbol"] for row in accepted],
            "accepted": accepted_preflight,
            "skipped": skipped,
            "unused_buffer_symbols": unused,
            "examined_count": last_examined + 1,
            "max_workers": workers,
            "speculatively_cached_symbols": [symbol for _, symbol in speculative],
        }
        if len(accepted) < required:
            rejected = ", ".join(row["symbol"] for row in skipped) or "none"
            if not continue_on_exhausted:
                raise HistoricalUniverseError(
                    f"{day} exhausted its draft pool with only {len(accepted)} viable "
                    f"candidates; {required} are required (rejected: {rejected})"
                )
            report["blocked"] = True
            report["blocked_reason"] = (
                f"preflight_exhausted:{len(accepted)}_of_{required}_required"
            )
            date_reports[day] = report
            continue
        report["blocked"] = False
        candidates_by_date[day] = accepted
        date_reports[day] = report

    output = {
        key: deepcopy(value)
        for key, value in draft.items()
        if key != "candidate_pool_by_date"
    }
    output["candidates_by_date"] = candidates_by_date
    output["preflight"] = {
        "schema_version": 2,
        "performed_at": performed_at or datetime.now(UTC).isoformat(),
        "provider": "Interactive Brokers TWS API pre-session history",
        "minimum_candidates": required,
        "availability_only": False,
        "pre_session_only": True,
        "target_session_prices_observed": False,
        "draft_manifest_sha256": _canonical_hash(draft),
        "dates": date_reports,
        "blocked_dates": [
            day for day, report in date_reports.items() if report.get("blocked") is True
        ],
    }
    if cache_metadata is not None:
        output["preflight"]["cache"] = deepcopy(dict(cache_metadata))
    return output


def _write_json(value: Mapping[str, Any], output: Path | None) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if output is None:
        print(rendered, end="")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(output)
    print(json.dumps({"written": str(output), "bytes": len(rendered.encode())}))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("draft", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--minimum-candidates", type=int, default=10)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    parser.add_argument("--cache-root", type=Path, default=DEFAULT_PREFLIGHT_CACHE_ROOT)
    parser.add_argument(
        "--contract-cache-root",
        type=Path,
        default=DEFAULT_CONTRACT_CACHE_ROOT,
        help="expiring integrity-checked cross-date symbol proof cache",
    )
    parser.add_argument(
        "--fresh-contracts",
        action="store_true",
        help="bypass cached contract details and refresh provider truth",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_PREFLIGHT_WORKERS,
        help=(
            "bounded rank-ordered IBKR probes to overlap "
            f"(default: {DEFAULT_PREFLIGHT_WORKERS})"
        ),
    )
    parser.add_argument(
        "--continue-on-exhausted",
        action="store_true",
        help="record exhausted dates as blocked and continue freezing other dates",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        draft = json.loads(args.draft.read_text(encoding="utf-8"))
        if not isinstance(draft, Mapping):
            raise HistoricalUniverseError("draft manifest must be a JSON object")
        config = IBKRConfig.from_env(args.env_file)
        strategy_config = load_config()
        universe = strategy_config.raw["universe"]
        qualification = {
            "strategy_version": strategy_config.version,
            "rules_hash": strategy_config.rules_hash,
            "minimum_average_daily_volume_14": float(
                universe["minimum_average_daily_volume_14"]
            ),
            "minimum_daily_atr_14": float(universe["minimum_daily_atr_14"]),
        }
        started_at = monotonic()
        with IBKRHistoricalClient(
            config,
            contract_cache_root=args.contract_cache_root,
            refresh_contract_details=args.fresh_contracts,
        ) as client:
            cache_root = args.cache_root.resolve()
            try:
                cache_root_text = str(cache_root.relative_to(PROJECT_ROOT.resolve()))
            except ValueError:
                cache_root_text = str(cache_root)
            cached_probe = ResumablePreflightProbe(
                cache_root,
                lambda symbol, day: probe_historical_candidate_with_history(
                    client,
                    symbol,
                    day,
                    minimum_average_daily_volume_14=qualification[
                        "minimum_average_daily_volume_14"
                    ],
                    minimum_daily_atr_14=qualification["minimum_daily_atr_14"],
                ),
                qualification=qualification,
            )
            frozen = freeze_candidate_universe(
                draft,
                cached_probe,
                minimum_candidates=args.minimum_candidates,
                cache_metadata={
                    "schema_version": PREFLIGHT_CACHE_SCHEMA_VERSION,
                    "probe_contract_version": PRE_SESSION_CACHE_VERSION,
                    "root": cache_root_text,
                    "reusable_pre_session_history": True,
                    "qualification_sha256": cached_probe.qualification_sha256,
                    "strategy_version": strategy_config.version,
                    "rules_hash": strategy_config.rules_hash,
                },
                max_workers=args.workers,
                continue_on_exhausted=args.continue_on_exhausted,
            )
            frozen["preflight"]["performance"] = {
                "elapsed_seconds": monotonic() - started_at,
                "max_workers": args.workers,
                "cache": cached_probe.stats(),
                "ibkr_requests": client.request_telemetry(),
            }
        _write_json(frozen, args.output)
        return 0
    except (
        HistoricalUniverseError,
        IBKRHistoricalError,
        StrategyInputError,
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
