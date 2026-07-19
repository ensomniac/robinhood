"""Independently reconstruct the frozen pre-entry structure dataset."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import statistics
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from historical_store import HistoricalDayStore, expand_bar
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
DATASET_ID = "dataset-preentry-structure-fidelity-2026-07-19-v2"
SOURCE_ROOT = "dataset-selected-candidate-join-2026-07-19-v1"
TRIGGER_ROOT = "dataset-selected-candidate-fidelity-2026-07-19-v1"
DEFAULT_PRIMARY_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-preentry-structure.json"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-preentry-structure-inspection.json"
)


class PreentryInspectionError(RuntimeError):
    """Independent reconstruction found a fidelity mismatch."""


class ExpectedUnresolved(PreentryInspectionError):
    """The independently reconstructed point-in-time inputs must fail closed."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise PreentryInspectionError(f"cannot read {path}: {exc}") from exc


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PreentryInspectionError(f"cannot read {path}: {exc}") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _paths(root: Path) -> dict[str, Path]:
    base = root / "_derived" / "preentry_structure" / DATASET_ID
    return {
        "selection": (
            root
            / "_derived"
            / "selected_candidate_join"
            / SOURCE_ROOT
            / "selected-pairs.json.gz"
        ),
        "triggers": (
            root
            / "_derived"
            / "selected_candidate_fidelity"
            / TRIGGER_ROOT
            / "clean-trigger-index.json.gz"
        ),
        "acquisition": base / "acquisition-index.json.gz",
        "splits": base / "splits.json.gz",
        "result": base / "structure-index.json.gz",
    }


def _iso(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise PreentryInspectionError("source timestamp lacks timezone")
    return parsed.astimezone(EASTERN)


def _dataset_rows(
    store: HistoricalDayStore,
    symbol: str,
    day: str,
    *,
    timeframe: str,
    session: str,
) -> list[dict[str, Any]]:
    document = store.load(symbol, day)
    if document is None:
        return []
    candidates = [
        row
        for row in document["datasets"]
        if row.get("kind") == "bars"
        and row.get("channel") == "trades"
        and row.get("timeframe") == timeframe
        and row.get("session") == session
        and row.get("provider") == "alpaca"
        and row.get("feed") == "sip"
        and row.get("adjustment") == "raw"
        and (session != "regular" or row.get("quality", {}).get("complete") is True)
    ]
    if not candidates:
        return []
    candidates.sort(
        key=lambda row: (-int(row.get("quality", {}).get("row_count", 0)), row["id"])
    )
    return [expand_bar(row) for row in candidates[0]["rows"]]


def _premarket_rows(
    store: HistoricalDayStore,
    symbol: str,
    day: str,
    expected_count: int,
) -> list[dict[str, Any]]:
    if expected_count == 0:
        return []
    start = datetime.combine(date.fromisoformat(day), time(4, 0), tzinfo=EASTERN)
    end = datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN)
    document = store.load(symbol, day)
    if document is None:
        raise PreentryInspectionError("nonempty premarket attestation has no day file")
    output: dict[tuple[Any, ...], dict[str, Any]] = {}
    covered = False
    for dataset in document["datasets"]:
        if not (
            dataset.get("kind") == "bars"
            and dataset.get("channel") == "trades"
            and dataset.get("timeframe") == "1m"
            and dataset.get("session") == "all"
            and dataset.get("provider") == "alpaca"
            and dataset.get("feed") == "sip"
            and dataset.get("adjustment") == "raw"
            and dataset.get("quality", {}).get("requested_window_complete") is True
        ):
            continue
        request_covers = False
        for sample in dataset.get("provenance", {}).get("samples", []):
            request = sample.get("request", {})
            try:
                captured_start = _iso(request["start"])
                captured_end = _iso(request["end"])
            except (KeyError, TypeError, ValueError):
                continue
            if (
                request.get("use_rth") is False
                and request.get("bar_size") == "1 min"
                and captured_start <= start
                and captured_end >= end
            ):
                request_covers = True
                break
        if not request_covers:
            continue
        covered = True
        for compact in dataset["rows"]:
            row = expand_bar(compact)
            observed = datetime.fromtimestamp(int(row["epoch"]), UTC).astimezone(
                EASTERN
            )
            if start <= observed < end:
                identity = (
                    int(row["epoch"]),
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    int(row.get("volume", 0)),
                )
                output[identity] = row
    if not covered or len(output) != expected_count:
        raise PreentryInspectionError("premarket cache differs from acquisition index")
    return sorted(output.values(), key=lambda row: int(row["epoch"]))


def _split_actions(path: Path) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for source in _read_gzip(path):
        try:
            row = {
                "execution_date": date.fromisoformat(str(source["execution_date"])),
                "split_from": float(source["split_from"]),
                "split_to": float(source["split_to"]),
            }
            symbol = str(source["ticker"]).strip().upper()
        except (KeyError, TypeError, ValueError) as exc:
            raise PreentryInspectionError("split row is malformed") from exc
        if not symbol or row["split_from"] <= 0 or row["split_to"] <= 0:
            raise PreentryInspectionError("split row has an invalid value")
        grouped.setdefault(symbol, []).append(row)
    return grouped


def _split_factor(
    symbol: str,
    observed_day: str,
    target_day: str,
    actions: Mapping[str, Sequence[Mapping[str, Any]]],
) -> float:
    observed = date.fromisoformat(observed_day)
    target = date.fromisoformat(target_day)
    factor = 1.0
    for event in actions.get(symbol, ()):
        execution = event["execution_date"]
        if observed < execution <= target:
            factor *= float(event["split_from"]) / float(event["split_to"])
    return factor


def _required_sessions(target: str, calendar: Sequence[Mapping[str, Any]]) -> list[str]:
    days = [str(row["date"]) for row in calendar]
    try:
        index = days.index(target)
    except ValueError as exc:
        raise PreentryInspectionError("target is absent from frozen calendar") from exc
    if index < 252:
        raise PreentryInspectionError("calendar lacks 252 prior sessions")
    return days[index - 252 : index]


def _recompute(
    *,
    store: HistoricalDayStore,
    trigger: Mapping[str, Any],
    pair: Mapping[str, Any],
    acquisition: Mapping[str, Any],
    calendar: Sequence[Mapping[str, Any]],
    actions: Mapping[str, Sequence[Mapping[str, Any]]],
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    day = str(trigger["date"])
    symbol = str(trigger["symbol"])
    observation = _iso(trigger["quote_snapshots"][-1]["target_at_et"])
    entry = float(trigger["quote_snapshots"][-1]["ask"])
    spreads = [
        float(row["ask"]) - float(row["bid"]) for row in trigger["quote_snapshots"]
    ]
    regular = _dataset_rows(store, symbol, day, timeframe="1m", session="regular")
    completed = [
        row
        for row in regular
        if _iso(row["time_et"]) + timedelta(minutes=1) <= observation
    ]
    session_open = datetime.combine(observation.date(), time(9, 30), tzinfo=EASTERN)
    expected_count = int((observation - session_open).total_seconds() // 60)
    expected = {
        session_open + timedelta(minutes=index) for index in range(expected_count)
    }
    by_time = {_iso(row["time_et"]): row for row in completed}
    if set(by_time) != expected or expected_count < 5:
        raise PreentryInspectionError("regular-minute prefix is not exact")
    opening_moments = [session_open + timedelta(minutes=index) for index in range(5)]
    opening_high = max(float(by_time[moment]["high"]) for moment in opening_moments)
    if entry < opening_high:
        raise ExpectedUnresolved("entry_limit must be at or above opening-range high")
    window = [by_time[moment] for moment in sorted(by_time)][-60:]
    closes = [float(row["close"]) for row in window]
    increments = [abs(current - prior) for prior, current in zip(closes, closes[1:])]
    average_increment = statistics.fmean(increments) if increments else 0.0
    median_spread = statistics.median(spreads)
    noise = max(median_spread, average_increment)
    invalidation = opening_high - noise
    atr_distance = float(rules["atr_stop_fraction"]) * float(
        pair["scanner_fields"]["daily_atr_14"]
    )
    stop_distance = max(atr_distance, entry - invalidation)
    planned_stop = entry - stop_distance
    stop_fraction = stop_distance / entry

    key = f"{day}|{trigger['instrument_id']}"
    premarket_state = acquisition["premarket_requests"][key]
    premarket = _premarket_rows(store, symbol, day, int(premarket_state["row_count"]))
    sessions = _required_sessions(day, calendar)
    daily_highs: list[float] = []
    for prior_day in sessions:
        rows = _dataset_rows(
            store, symbol, prior_day, timeframe="15m", session="regular"
        )
        if rows:
            daily_highs.append(
                max(float(row["high"]) for row in rows)
                * _split_factor(symbol, prior_day, day, actions)
            )
    history_complete = len(daily_highs) == 252
    candidates: list[tuple[str, float]] = []
    if premarket:
        candidates.append(
            ("target_premarket_high", max(float(row["high"]) for row in premarket))
        )
    if daily_highs:
        if history_complete:
            candidates.extend(
                (
                    ("prior_session_high", daily_highs[-1]),
                    ("prior_14_session_high", max(daily_highs[-14:])),
                    ("prior_252_session_high", max(daily_highs)),
                )
            )
        else:
            candidates.append(("observed_partial_history_high", max(daily_highs)))
    overhead = sorted(
        ((source, price) for source, price in candidates if price > entry),
        key=lambda item: (item[1], item[0]),
    )
    inputs_complete = (
        premarket_state["status"] in {"COMPLETE", "CACHED"} and history_complete
    )
    if overhead:
        resistance_source, resistance_price = overhead[0]
        room = (resistance_price - entry) / entry
        if room < float(rules["minimum_resistance_room_fraction"]):
            resistance_status = "BLOCKED_KNOWN_OVERHEAD"
            room_pass: bool | None = False
        elif inputs_complete:
            resistance_status = "RESOLVED_OVERHEAD"
            room_pass = True
        else:
            resistance_status = "UNRESOLVED_INPUT"
            room_pass = None
    elif inputs_complete:
        resistance_status = "RESOLVED_PRICE_DISCOVERY"
        resistance_source = None
        resistance_price = None
        room = None
        room_pass = True
    else:
        resistance_status = "UNRESOLVED_INPUT"
        resistance_source = None
        resistance_price = None
        room = None
        room_pass = None
    return {
        "contract_version": str(rules["version"]),
        "observation_at": observation.isoformat(),
        "entry_limit": entry,
        "stop": {
            "opening_range_high": opening_high,
            "average_close_increment": average_increment,
            "median_spread_dollars": median_spread,
            "normal_noise_dollars": noise,
            "technical_invalidation": invalidation,
            "atr_stop_distance": atr_distance,
            "stop_distance": stop_distance,
            "planned_stop": planned_stop,
            "stop_fraction": stop_fraction,
            "stop_outside_noise": planned_stop <= invalidation,
            "maximum_stop_fraction_pass": stop_fraction
            <= float(rules["maximum_stop_fraction"]),
            "completed_noise_bars": len(window),
        },
        "resistance": {
            "status": resistance_status,
            "resistance_price": resistance_price,
            "resistance_source": resistance_source,
            "resistance_room_fraction": room,
            "minimum_room_pass": room_pass,
            "premarket_window_complete": premarket_state["status"]
            in {"COMPLETE", "CACHED"},
            "premarket_observation_count": len(premarket),
            "daily_history_count": len(daily_highs),
            "daily_history_complete": history_complete,
            "daily_split_basis_verified": True,
            "known_overhead_levels": [list(item) for item in overhead],
        },
    }


def _assert_no_private_rows(value: Any) -> None:
    if isinstance(value, Mapping):
        forbidden = {"symbol", "instrument_id", "quote_snapshots"}
        if forbidden.intersection(value):
            raise PreentryInspectionError("public evidence exposes private rows")
        for nested in value.values():
            _assert_no_private_rows(nested)
    elif isinstance(value, list):
        for nested in value:
            _assert_no_private_rows(nested)


def inspect(
    *,
    manifest_path: Path,
    env_path: Path,
    primary_result_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise PreentryInspectionError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise PreentryInspectionError("unexpected pre-entry dataset")
    paths = _paths(store.root)
    private = _read_gzip(paths["result"])
    acquisition = _read_gzip(paths["acquisition"])
    selection = _read_gzip(paths["selection"])
    triggers = _read_gzip(paths["triggers"])
    calendar = _read_json(PROJECT_ROOT / manifest["calendar_contract"]["path"])
    actions = _split_actions(paths["splits"])
    if private.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise PreentryInspectionError("private result is not manifest-bound")
    pairs = {
        (str(row["date"]), str(row["instrument_id"])): row
        for row in selection["selected_pairs"]
    }
    primary = {
        (str(row["date"]), str(row["instrument_id"])): row for row in private["records"]
    }
    counts: Counter[str] = Counter()
    for trigger in triggers["records"]:
        key = (str(trigger["date"]), str(trigger["instrument_id"]))
        pair = pairs.get(key)
        observed = primary.get(key)
        if pair is None or observed is None:
            raise PreentryInspectionError("source or primary record is missing")
        snapshots = trigger.get("quote_snapshots")
        if not isinstance(snapshots, list) or len(snapshots) != 3:
            if (
                observed.get("status") != "UNRESOLVED"
                or observed.get("structure") is not None
                or observed.get("error")
                != "exactly three point-in-time quote snapshots are required"
            ):
                raise PreentryInspectionError(
                    "missing-snapshot record did not fail closed"
                )
            counts["records"] += 1
            counts["unresolved"] += 1
            continue
        try:
            recomputed = _recompute(
                store=store,
                trigger=trigger,
                pair=pair,
                acquisition=acquisition,
                calendar=calendar,
                actions=actions,
                rules=manifest["structure_contract"],
            )
        except ExpectedUnresolved as exc:
            if (
                observed.get("status") != "UNRESOLVED"
                or observed.get("structure") is not None
                or observed.get("error") != str(exc)
            ):
                raise PreentryInspectionError(
                    "independently unresolved record differs from primary"
                ) from exc
            counts["records"] += 1
            counts["unresolved"] += 1
            continue
        if observed.get("status") != "DERIVED":
            raise PreentryInspectionError("derivable quote record was not derived")
        if recomputed != observed.get("structure"):
            raise PreentryInspectionError(
                f"independent structure mismatch for identity {key[1]} on {key[0]}"
            )
        counts["records"] += 1
        counts["derived"] += 1
        resistance = recomputed["resistance"]
        stop = recomputed["stop"]
        counts[f"resistance_{resistance['status'].lower()}"] += 1
        counts["stop_within_maximum"] += int(stop["maximum_stop_fraction_pass"])
        counts["resistance_room_pass"] += int(resistance["minimum_room_pass"] is True)
        counts["structure_both_pass"] += int(
            stop["maximum_stop_fraction_pass"]
            and resistance["minimum_room_pass"] is True
        )
    expected_counts = dict(sorted(counts.items()))
    if expected_counts != private.get("counts"):
        raise PreentryInspectionError("independent aggregate counts differ")
    public_primary = _read_json(primary_result_path)
    if "records" in public_primary:
        raise PreentryInspectionError("public evidence exposes private record rows")
    if (
        public_primary.get("status") != "READY"
        or public_primary.get("counts") != expected_counts
        or public_primary.get("private_result_sha256") != _sha256_file(paths["result"])
    ):
        raise PreentryInspectionError("primary public result is inconsistent")
    _assert_no_private_rows(public_primary)
    output = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "independently_inspected": True,
        "counts": expected_counts,
        "private_result_sha256": _sha256_file(paths["result"]),
        "checks": {
            "all_325_structures_recomputed_exactly": True,
            "point_in_time_prefixes_have_no_lookahead": True,
            "split_adjustment_recomputed_independently": True,
            "incomplete_history_cannot_prove_price_discovery": True,
            "public_artifacts_exclude_licensed_rows_and_symbols": True,
            "target_outcomes_observed_or_derived": False,
        },
        "claim_boundary": (
            "Independent input-definition reconstruction only; not alpha, returns, "
            "confirmation, or production strategy promotion."
        ),
    }
    _write_json(output_path, output)
    return output


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--primary-result", type=Path, default=DEFAULT_PRIMARY_RESULT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        value = inspect(
            manifest_path=args.manifest,
            env_path=args.env_file,
            primary_result_path=args.primary_result,
            output_path=args.output,
        )
    except (PreentryInspectionError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
