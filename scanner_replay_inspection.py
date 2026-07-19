"""Adversarially inspect a built dynamic 09:35 scanner replay.

The build engine emits a private per-symbol detail artifact and a compact public
summary.  This validator recomputes the scanner decisions from detail, verifies
the external source attestations, and emits only aggregate public evidence.  It
does not collect data, change strategy rules, or access a broker.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import re
import subprocess
import sys
import tomllib
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time as wall_time
from pathlib import Path
from typing import Any

from historical_store import (
    HistoricalDayStore,
    HistoricalStoreConfig,
    HistoricalStoreError,
    expand_bar,
)
from learning_data import (
    LearningDataError,
    load_security_master,
    security_record_covers,
    security_master_sha256,
)
from scanner_replay import (
    EASTERN,
    PROJECT_ROOT,
    ScannerReplayError,
    _sha256_file,
    _sha256_json,
    _write_json,
    load_calendar,
    validate_rules,
)
from scanner_replay_alpaca import (
    CSV_FIELDS,
    DEFAULT_CALENDAR,
    DEFAULT_RULES,
    DEFAULT_RUN_ROOT,
    DEFAULT_SPLITS,
    DEFAULT_SUMMARY,
    collection_status,
    index_root,
    load_contract,
    verify_contract_inputs,
)


DEFAULT_DETAIL = DEFAULT_RUN_ROOT / "scanner-replay-detail.json"
DEFAULT_OUTPUT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-scanner-replay-inspection.json"
)
DEFAULT_SPLIT_ATTESTATION = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "split-actions-source.json"
)
DEFAULT_CALENDAR_ATTESTATION = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "session-calendar-source.json"
)
DEFAULT_STRATEGY_ATTESTATION = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "production-strategy-source.json"
)
EARLY_REJECTIONS = {
    "exchange_not_allowed",
    "incomplete_target_opening_bar",
    "incomplete_prior_session_history",
    "incomplete_prior_opening_history",
}
COMPUTED_REJECTIONS = {
    "below_minimum_open_price",
    "below_minimum_average_daily_volume",
    "below_minimum_atr",
    "below_minimum_opening_rvol",
    "non_bullish_opening_candle",
    "non_positive_opening_return",
    "eligible",
}
METRIC_FIELDS = {
    "open_price",
    "opening_high",
    "opening_low",
    "opening_close",
    "opening_volume",
    "prior_opening_volume_mean_14",
    "opening_relative_volume",
    "average_daily_volume_14",
    "daily_atr_14",
    "split_adjustment_factor_oldest_session",
    "prior_close",
    "opening_return",
    "bullish_opening_candle",
}


class ScannerInspectionError(ValueError):
    """The built scanner artifact cannot support READY status."""


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScannerInspectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ScannerInspectionError(f"{path} must contain an object")
    return value


def _number(row: Mapping[str, Any], field: str) -> float:
    value = row.get(field)
    if isinstance(value, bool):
        raise ScannerInspectionError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ScannerInspectionError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise ScannerInspectionError(f"{field} must be finite")
    return result


def _computed_disposition(row: Mapping[str, Any], thresholds: Mapping[str, Any]) -> str:
    missing = METRIC_FIELDS - set(row)
    if missing:
        raise ScannerInspectionError(f"computed scanner row lacks {sorted(missing)}")
    opened = _number(row, "open_price")
    high = _number(row, "opening_high")
    low = _number(row, "opening_low")
    closed = _number(row, "opening_close")
    opening_volume = _number(row, "opening_volume")
    opening_mean = _number(row, "prior_opening_volume_mean_14")
    rvol = _number(row, "opening_relative_volume")
    adv = _number(row, "average_daily_volume_14")
    atr = _number(row, "daily_atr_14")
    split_factor = _number(row, "split_adjustment_factor_oldest_session")
    prior_close = _number(row, "prior_close")
    opening_return = _number(row, "opening_return")
    if (
        min(opened, high, low, closed, opening_mean, atr, split_factor, prior_close)
        <= 0
    ):
        raise ScannerInspectionError(
            "computed scanner row has non-positive market inputs"
        )
    if (
        opening_volume < 0
        or adv < 0
        or not low <= min(opened, closed) <= max(opened, closed) <= high
    ):
        raise ScannerInspectionError("computed scanner row has invalid OHLCV")
    expected_rvol = opening_volume / opening_mean
    expected_return = closed / prior_close - 1.0
    if not math.isclose(rvol, expected_rvol, rel_tol=1e-12, abs_tol=1e-12):
        raise ScannerInspectionError(
            "opening RVOL does not match its frozen denominator"
        )
    if not math.isclose(opening_return, expected_return, rel_tol=1e-12, abs_tol=1e-12):
        raise ScannerInspectionError("opening return does not match prior close")
    bullish = closed > opened
    if row.get("bullish_opening_candle") is not bullish:
        raise ScannerInspectionError("bullish opening flag disagrees with OHLC")
    if opened < float(thresholds["minimum_open_price"]):
        return "below_minimum_open_price"
    if adv < float(thresholds["minimum_average_daily_volume_14"]):
        return "below_minimum_average_daily_volume"
    if atr < float(thresholds["minimum_daily_atr_14"]):
        return "below_minimum_atr"
    if rvol < float(thresholds["minimum_opening_relative_volume"]):
        return "below_minimum_opening_rvol"
    if not bullish:
        return "non_bullish_opening_candle"
    if opening_return <= 0:
        return "non_positive_opening_return"
    return "eligible"


def _verify_source_attestations(
    manifest: Mapping[str, Any], source_root: Path
) -> dict[str, Any]:
    try:
        frozen_at = datetime.fromisoformat(
            str(manifest["registered_at"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError) as exc:
        raise ScannerInspectionError("manifest registration time is malformed") from exc
    if frozen_at.tzinfo is None:
        raise ScannerInspectionError("manifest registration time lacks a timezone")
    dataset_id = str(manifest["dataset_id"])
    collection = manifest["collection_contract"]
    expected_symbols = int(collection["target_symbol_union_count"])
    reusable = collection.get("reusable_source")
    reusable_days = (
        set(str(day) for day in reusable["session_dates"])
        if isinstance(reusable, Mapping)
        else set()
    )
    source_hashes: list[str] = []
    totals: Counter[str] = Counter()
    captured_times: list[datetime] = []
    for day in collection["required_session_dates"]:
        source_path = source_root / "minute_aggs" / day[:4] / f"{day}.csv.gz"
        sidecar = _read_object(
            source_root / "attestations" / day[:4] / f"{day}.json"
        )
        source = sidecar.get("source")
        try:
            captured_at = datetime.fromisoformat(
                str(sidecar["captured_at"]).replace("Z", "+00:00")
            )
        except (KeyError, ValueError) as exc:
            raise ScannerInspectionError(
                f"{day}: source capture time is malformed"
            ) from exc
        if captured_at.tzinfo is None or captured_at <= frozen_at:
            raise ScannerInspectionError(
                f"{day}: source artifact does not postdate the frozen contract"
            )
        captured_times.append(captured_at)
        if not isinstance(source, Mapping) or any(
            (
                sidecar.get("schema_version") != 1,
                sidecar.get("dataset_id") != dataset_id,
                sidecar.get("date") != day,
                sidecar.get("status") != "READY",
                sidecar.get("requested_symbols") != expected_symbols,
                sidecar.get("target_symbol_union_count", expected_symbols)
                != expected_symbols,
                source.get("provider") != "Alpaca",
                source.get("endpoint")
                != "https://data.alpaca.markets/v2/stocks/bars",
                source.get("feed") != "sip",
                source.get("adjustment") != "raw",
                source.get("asof") != "-",
            )
        ):
            raise ScannerInspectionError(f"{day}: source attestation contract differs")
        try:
            daily_symbols = int(sidecar["daily_symbols"])
            source_symbol_union = int(
                sidecar.get("source_symbol_union_count", expected_symbols)
            )
            opening_symbols = int(sidecar["opening_symbols"])
            exact_openings = int(sidecar["complete_opening_symbols"])
            derived_rows = int(sidecar["derived_rows"])
            provider_requests = int(sidecar["provider_requests"])
            provider_retries = int(sidecar["provider_retries"])
            canonical_merges = int(sidecar["canonical_files_changed"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ScannerInspectionError(
                f"{day}: source attestation counts are malformed"
            ) from exc
        if not (
            source_symbol_union >= expected_symbols
            and 0 < daily_symbols <= source_symbol_union
            and 0 <= exact_openings <= opening_symbols <= source_symbol_union
            and derived_rows >= daily_symbols
            and provider_requests >= 0
            and provider_retries >= 0
            and canonical_merges >= 0
        ):
            raise ScannerInspectionError(
                f"{day}: source attestation counts are inconsistent"
            )
        source_hash = str(sidecar.get("source_sha256") or "")
        if (
            not re.fullmatch(r"[0-9a-f]{64}", source_hash)
            or not source_path.exists()
            or _sha256_file(source_path) != source_hash
        ):
            raise ScannerInspectionError(f"{day}: source hash differs")
        reused = sidecar.get("reused_source")
        if day in reusable_days:
            if not isinstance(reused, Mapping):
                raise ScannerInspectionError(
                    f"{day}: reusable source provenance differs"
                )
            inherited_rows = reused.get("inherited_derived_rows")
            delta_symbols = reused.get("delta_symbols_requested")
            if (
                reused.get("dataset_id") != reusable.get("dataset_id")
                or not re.fullmatch(
                    r"[0-9a-f]{64}", str(reused.get("source_sha256") or "")
                )
                or not isinstance(inherited_rows, int)
                or inherited_rows < 0
                or not isinstance(delta_symbols, int)
                or delta_symbols < 0
            ):
                raise ScannerInspectionError(
                    f"{day}: reusable source provenance differs"
                )
            reusable_root = source_root.parent / str(reusable["dataset_id"])
            inherited_path = (
                reusable_root / "minute_aggs" / day[:4] / f"{day}.csv.gz"
            )
            inherited_sidecar = _read_object(
                reusable_root / "attestations" / day[:4] / f"{day}.json"
            )
            inherited_hash = str(reused["source_sha256"])
            if any(
                (
                    not inherited_path.exists(),
                    inherited_sidecar.get("schema_version") != 1,
                    inherited_sidecar.get("dataset_id") != reusable["dataset_id"],
                    inherited_sidecar.get("date") != day,
                    inherited_sidecar.get("status") != "READY",
                    inherited_sidecar.get("source_sha256") != inherited_hash,
                    inherited_sidecar.get("derived_rows")
                    != reused["inherited_derived_rows"],
                )
            ) or _sha256_file(inherited_path) != inherited_hash:
                raise ScannerInspectionError(
                    f"{day}: inherited source artifact differs"
                )
            if provider_requests == 0 and reused["delta_symbols_requested"] != 0:
                raise ScannerInspectionError(
                    f"{day}: nonempty symbol delta made no provider request"
                )
            totals.update(
                {
                    "reused_sessions": 1,
                    "inherited_derived_rows": inherited_rows,
                    "delta_symbols_requested": delta_symbols,
                }
            )
        elif reused is not None or provider_requests <= 0:
            raise ScannerInspectionError(
                f"{day}: fresh source provenance is inconsistent"
            )
        source_hashes.append(source_hash)
        totals.update(
            {
                "provider_requests": provider_requests,
                "provider_retries": provider_retries,
                "canonical_day_merges": canonical_merges,
                "derived_rows": derived_rows,
            }
        )
    return {
        "source_hashes": source_hashes,
        "provider_requests": totals["provider_requests"],
        "provider_retries": totals["provider_retries"],
        "canonical_day_merges": totals["canonical_day_merges"],
        "derived_rows": totals["derived_rows"],
        "reused_sessions": totals["reused_sessions"],
        "inherited_derived_rows": totals["inherited_derived_rows"],
        "delta_symbols_requested": totals["delta_symbols_requested"],
        "earliest_captured_at": min(captured_times).isoformat(),
        "all_captured_after_freeze": True,
    }


def _load_independent_split_actions(
    path: Path,
) -> dict[str, list[dict[str, Any]]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            raw = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ScannerInspectionError(f"cannot independently read split history: {exc}") from exc
    if not isinstance(raw, list):
        raise ScannerInspectionError("split history must be an array")
    result: dict[str, list[dict[str, Any]]] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            raise ScannerInspectionError("split history contains a malformed row")
        try:
            symbol = str(item["ticker"]).strip().upper()
            execution = date.fromisoformat(str(item["execution_date"]))
            split_from = float(item["split_from"])
            split_to = float(item["split_to"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ScannerInspectionError("split history contains invalid fields") from exc
        if not symbol or split_from <= 0 or split_to <= 0:
            raise ScannerInspectionError("split history contains an invalid ratio")
        result.setdefault(symbol, []).append(
            {
                "execution_date": execution,
                "split_from": split_from,
                "split_to": split_to,
            }
        )
    for values in result.values():
        values.sort(key=lambda row: row["execution_date"])
    return result


def _historical_git_blob(evidence: Mapping[str, Any]) -> bytes:
    commit = str(evidence.get("commit") or "")
    historical_path = str(evidence.get("path") or "")
    if (
        not re.fullmatch(r"[0-9a-f]{40}", commit)
        or not historical_path
        or Path(historical_path).is_absolute()
        or ".." in Path(historical_path).parts
    ):
        raise ScannerInspectionError("historical evidence locator is malformed")
    completed = subprocess.run(
        ["git", "show", f"{commit}:{historical_path}"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise ScannerInspectionError("pre-collection evidence commit is unavailable")
    return completed.stdout


def _verify_pre_collection_split_attestation(
    attestation_path: Path, split_path: Path
) -> dict[str, Any]:
    attestation = _read_object(attestation_path)
    if attestation.get("schema_version") != 1:
        raise ScannerInspectionError("split attestation schema is unsupported")
    artifact = attestation.get("artifact")
    evidence = attestation.get("pre_target_collection_evidence")
    if not isinstance(artifact, Mapping) or not isinstance(evidence, Mapping):
        raise ScannerInspectionError("split attestation is incomplete")
    expected_hash = str(artifact.get("sha256") or "")
    expected_events = artifact.get("events")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_hash) or not isinstance(
        expected_events, int
    ):
        raise ScannerInspectionError("split attestation identity is malformed")
    actual_hash = _sha256_file(split_path)
    actions = _load_independent_split_actions(split_path)
    actual_events = sum(len(values) for values in actions.values())
    if actual_hash != expected_hash or actual_events != expected_events:
        raise ScannerInspectionError(
            "split actions differ from their pre-collection attestation"
        )
    try:
        historical = json.loads(_historical_git_blob(evidence))
    except json.JSONDecodeError as exc:
        raise ScannerInspectionError(
            "pre-collection split evidence is malformed"
        ) from exc
    corporate_actions = historical.get(str(evidence.get("field") or ""), {})
    if not isinstance(corporate_actions, Mapping) or any(
        (
            corporate_actions.get("sha256") != expected_hash,
            corporate_actions.get("events") != expected_events,
            corporate_actions.get("local_ignored_artifact")
            != artifact.get("local_ignored_path"),
        )
    ):
        raise ScannerInspectionError(
            "historical Git evidence does not bind the inspected split actions"
        )
    return {
        "sha256": actual_hash,
        "events": actual_events,
        "pre_collection_commit_verified": True,
    }


def _verify_manifest_bound_split_actions(
    *,
    manifest_path: Path,
    manifest: Mapping[str, Any],
    split_path: Path,
    earliest_source_capture: str,
) -> dict[str, Any]:
    universe = manifest["dataset_payload"]["universe_contract"]
    attestation_path = PROJECT_ROOT / str(
        universe["split_actions_attestation_path"]
    )
    attestation = _read_object(attestation_path)
    artifact = attestation.get("artifact")
    source = attestation.get("source")
    query_range = source.get("query_range") if isinstance(source, Mapping) else None
    actions = _load_independent_split_actions(split_path)
    actual_events = sum(len(values) for values in actions.values())
    actual_hash = _sha256_file(split_path)
    required = manifest["collection_contract"]["required_session_dates"]
    if (
        attestation.get("schema_version") != 1
        or not isinstance(artifact, Mapping)
        or not isinstance(source, Mapping)
        or not isinstance(query_range, Mapping)
        or source.get("provider") != "Massive"
        or source.get("endpoint") != "https://api.massive.com/stocks/v1/splits"
        or str(query_range.get("execution_date_gte") or "") > required[0]
        or str(query_range.get("execution_date_lte") or "")
        < max(manifest["requested_dates"])
        or artifact.get("events") != actual_events
        or artifact.get("sha256") != actual_hash
        or universe.get("split_actions_sha256") != actual_hash
        or universe.get("split_actions_attestation_sha256")
        != _sha256_file(attestation_path)
        or (PROJECT_ROOT / str(universe["split_actions_path"])).resolve()
        != split_path.resolve()
    ):
        raise ScannerInspectionError(
            "manifest-bound split actions or attestation differ"
        )
    relative_manifest = manifest_path.resolve().relative_to(PROJECT_ROOT).as_posix()
    completed = subprocess.run(
        [
            "git",
            "log",
            "--diff-filter=A",
            "-1",
            "--format=%H%x00%cI",
            "--",
            relative_manifest,
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        commit, committed_at_text = completed.stdout.strip().split("\x00", 1)
        committed_at = datetime.fromisoformat(committed_at_text.replace("Z", "+00:00"))
        captured_at = datetime.fromisoformat(
            earliest_source_capture.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ScannerInspectionError(
            "frozen scanner manifest lacks committed pre-collection evidence"
        ) from exc
    if (
        completed.returncode != 0
        or not re.fullmatch(r"[0-9a-f]{40}", commit)
        or committed_at.tzinfo is None
        or captured_at.tzinfo is None
        or committed_at >= captured_at
        or _historical_git_blob({"commit": commit, "path": relative_manifest})
        != manifest_path.read_bytes()
    ):
        raise ScannerInspectionError(
            "scanner manifest was not immutably committed before source collection"
        )
    return {
        "sha256": actual_hash,
        "events": actual_events,
        "pre_collection_commit": commit,
        "pre_collection_commit_verified": True,
        "manifest_bound": True,
    }


def _verify_pre_collection_calendar_attestation(
    attestation_path: Path, calendar_path: Path
) -> dict[str, Any]:
    attestation = _read_object(attestation_path)
    if attestation.get("schema_version") != 1:
        raise ScannerInspectionError("calendar attestation schema is unsupported")
    artifact = attestation.get("artifact")
    evidence = attestation.get("pre_target_collection_evidence")
    verification = attestation.get("provider_verification")
    if not all(isinstance(item, Mapping) for item in (artifact, evidence, verification)):
        raise ScannerInspectionError("calendar attestation is incomplete")
    expected_hash = str(artifact.get("sha256") or "")
    sessions = load_calendar(calendar_path)
    if (
        not re.fullmatch(r"[0-9a-f]{64}", expected_hash)
        or _sha256_file(calendar_path) != expected_hash
        or artifact.get("sessions") != len(sessions)
        or artifact.get("first_session") != sessions[0]
        or artifact.get("last_session") != sessions[-1]
    ):
        raise ScannerInspectionError(
            "session calendar differs from its pre-collection attestation"
        )
    historical = _historical_git_blob(evidence)
    if hashlib.sha256(historical).hexdigest() != expected_hash:
        raise ScannerInspectionError(
            "historical Git evidence does not bind the inspected calendar"
        )
    if (
        verification.get("dates_match_exactly") is not True
        or verification.get("provider_sessions") != len(sessions)
    ):
        raise ScannerInspectionError(
            "session calendar lacks an exact provider verification"
        )
    return {
        "sha256": expected_hash,
        "sessions": len(sessions),
        "pre_collection_commit_verified": True,
        "provider_dates_verified": True,
    }


def _verify_pre_collection_strategy_attestation(
    attestation_path: Path, rules: Mapping[str, Any]
) -> dict[str, Any]:
    attestation = _read_object(attestation_path)
    if attestation.get("schema_version") != 1:
        raise ScannerInspectionError("strategy attestation schema is unsupported")
    artifact = attestation.get("artifact")
    evidence = attestation.get("pre_target_collection_evidence")
    if not isinstance(artifact, Mapping) or not isinstance(evidence, Mapping):
        raise ScannerInspectionError("strategy attestation is incomplete")
    expected_file_hash = str(artifact.get("file_sha256") or "")
    historical = _historical_git_blob(evidence)
    if (
        not re.fullmatch(r"[0-9a-f]{64}", expected_file_hash)
        or hashlib.sha256(historical).hexdigest() != expected_file_hash
        or artifact.get("path") != evidence.get("path")
    ):
        raise ScannerInspectionError(
            "production strategy differs from its pre-collection attestation"
        )
    try:
        raw_config = tomllib.loads(historical.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ScannerInspectionError(
            "attested production strategy cannot be parsed"
        ) from exc
    canonical = json.dumps(
        raw_config, sort_keys=True, separators=(",", ":")
    ).encode()
    canonical_hash = hashlib.sha256(canonical).hexdigest()
    universe = raw_config["universe"]
    strategy = raw_config["strategy"]
    strategy_version = str(strategy["version"])
    thresholds = rules["thresholds"]
    lookbacks = rules["lookbacks"]
    comparisons = (
        (rules.get("strategy_version"), strategy_version),
        (rules.get("selection_time_et"), strategy["entry_start_et"]),
        (thresholds.get("minimum_open_price"), universe["minimum_open_price"]),
        (
            thresholds.get("minimum_average_daily_volume_14"),
            universe["minimum_average_daily_volume_14"],
        ),
        (thresholds.get("minimum_daily_atr_14"), universe["minimum_daily_atr_14"]),
        (
            thresholds.get("minimum_opening_relative_volume"),
            universe["minimum_opening_relative_volume"],
        ),
        (
            lookbacks.get("opening_relative_volume_sessions"),
            universe["opening_relative_volume_lookback"],
        ),
    )
    if (
        artifact.get("canonical_rules_hash") != canonical_hash
        or artifact.get("strategy_version") != strategy_version
        or any(left != right for left, right in comparisons)
    ):
        raise ScannerInspectionError(
            "scanner rules do not reproduce the attested production inputs"
        )
    return {
        "strategy_version": strategy_version,
        "canonical_rules_hash": canonical_hash,
        "pre_collection_commit_verified": True,
        "scanner_fields_match": True,
    }


def _independent_split_factor(
    symbol: str,
    observed_day: str,
    target_day: str,
    split_actions: Mapping[str, Sequence[Mapping[str, Any]]],
) -> float:
    observed = date.fromisoformat(observed_day)
    target = date.fromisoformat(target_day)
    factor = 1.0
    for event in split_actions.get(symbol, []):
        execution = event["execution_date"]
        if observed < execution <= target:
            factor *= float(event["split_from"]) / float(event["split_to"])
    return factor


def _load_independent_source_day(path: Path, day: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    seen: set[tuple[str, datetime]] = set()
    try:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source)
            if not set(CSV_FIELDS).issubset(set(reader.fieldnames or [])):
                raise ScannerInspectionError(f"{day}: source index columns are incomplete")
            for raw in reader:
                try:
                    symbol = str(raw["ticker"]).strip().upper()
                    observed = datetime.fromtimestamp(
                        int(raw["window_start"]) / 1e9, UTC
                    ).astimezone(EASTERN)
                    opened = float(raw["open"])
                    high = float(raw["high"])
                    low = float(raw["low"])
                    closed = float(raw["close"])
                    volume = int(float(raw["volume"]))
                except (KeyError, TypeError, ValueError) as exc:
                    raise ScannerInspectionError(
                        f"{day}: source index contains a malformed row"
                    ) from exc
                if (
                    not symbol
                    or observed.date().isoformat() != day
                    or not wall_time(9, 30) <= observed.time() < wall_time(16)
                ):
                    raise ScannerInspectionError(
                        f"{day}: source index row is outside its session"
                    )
                values = (opened, high, low, closed)
                if (
                    any(not math.isfinite(value) or value <= 0 for value in values)
                    or volume < 0
                    or not low <= min(opened, closed) <= max(opened, closed) <= high
                ):
                    raise ScannerInspectionError(
                        f"{day}: source index contains invalid OHLCV"
                    )
                identity = (symbol, observed)
                if identity in seen:
                    raise ScannerInspectionError(
                        f"{day}: source index contains a duplicate symbol timestamp"
                    )
                seen.add(identity)
                grouped.setdefault(symbol, []).append(
                    {
                        "observed": observed,
                        "open": opened,
                        "high": high,
                        "low": low,
                        "close": closed,
                        "volume": volume,
                    }
                )
    except OSError as exc:
        raise ScannerInspectionError(f"cannot read source index {day}: {exc}") from exc

    result: dict[str, dict[str, Any]] = {}
    for symbol, unsorted in grouped.items():
        rows = sorted(unsorted, key=lambda row: row["observed"])
        residual = [row for row in rows if row["observed"].time() >= wall_time(9, 35)]
        if len(residual) != 1 or residual[0]["observed"].time() != wall_time(15, 59):
            raise ScannerInspectionError(
                f"{day}: {symbol} does not have exactly one derived residual row"
            )
        opening = [row for row in rows if row["observed"].time() < wall_time(9, 35)]
        expected_opening = [
            datetime.combine(date.fromisoformat(day), wall_time(9, 30), tzinfo=EASTERN)
            .replace(minute=30 + offset)
            for offset in range(5)
        ]
        exact_opening = [row["observed"] for row in opening] == expected_opening
        result[symbol] = {
            "open": rows[0]["open"],
            "high": max(row["high"] for row in rows),
            "low": min(row["low"] for row in rows),
            "close": rows[-1]["close"],
            "volume": sum(row["volume"] for row in rows),
            "opening_exact": exact_opening,
            "opening_open": opening[0]["open"] if opening else None,
            "opening_high": max((row["high"] for row in opening), default=None),
            "opening_low": min((row["low"] for row in opening), default=None),
            "opening_close": opening[-1]["close"] if opening else None,
            "opening_volume": sum(row["volume"] for row in opening),
            "opening_timestamps": [row["observed"].isoformat() for row in opening],
            "provider_open": residual[0]["open"],
            "provider_high": residual[0]["high"],
            "provider_low": residual[0]["low"],
            "provider_close": residual[0]["close"],
        }
    return result


def _one_canonical_dataset(
    document: Mapping[str, Any], *, day: str, symbol: str, fields: Mapping[str, Any]
) -> Mapping[str, Any]:
    matches = [
        dataset
        for dataset in document.get("datasets", [])
        if all(dataset.get(key) == value for key, value in fields.items())
    ]
    if len(matches) != 1:
        raise ScannerInspectionError(
            f"{day}: canonical {symbol} needs exactly one {dict(fields)} dataset"
        )
    return matches[0]


def _canonical_bar_aggregate(
    dataset: Mapping[str, Any], *, day: str, symbol: str
) -> dict[str, Any]:
    rows = [expand_bar(row) for row in dataset.get("rows", [])]
    if not rows:
        raise ScannerInspectionError(f"{day}: canonical {symbol} bar set is empty")
    rows.sort(key=lambda row: int(row["epoch"]))
    return {
        "open": float(rows[0]["open"]),
        "high": max(float(row["high"]) for row in rows),
        "low": min(float(row["low"]) for row in rows),
        "close": float(rows[-1]["close"]),
        "volume": sum(int(row["volume"]) for row in rows),
        "timestamps": [str(row["time_et"]) for row in rows],
    }


def _same_market_value(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, int) and isinstance(right, int):
        return left == right
    try:
        return math.isclose(float(left), float(right), rel_tol=1e-12, abs_tol=1e-12)
    except (TypeError, ValueError):
        return False


def _verify_canonical_persistence(
    manifest: Mapping[str, Any], *, store: HistoricalDayStore, source_root: Path
) -> dict[str, Any]:
    documents = 0
    regular_datasets = 0
    opening_datasets = 0
    derived_datasets = 0
    for day in manifest["collection_contract"]["required_session_dates"]:
        source = _load_independent_source_day(
            source_root / "minute_aggs" / day[:4] / f"{day}.csv.gz", day
        )
        for symbol, expected in source.items():
            document = store.load(symbol, day)
            if document is None:
                raise ScannerInspectionError(
                    f"{day}: canonical source document is missing for {symbol}"
                )
            regular = _one_canonical_dataset(
                document,
                day=day,
                symbol=symbol,
                fields={
                    "kind": "bars",
                    "provider": "alpaca",
                    "channel": "trades",
                    "timeframe": "15m",
                    "feed": "sip",
                    "adjustment": "raw",
                    "session": "regular",
                    "scope": "full_session",
                },
            )
            if (
                regular.get("quality", {}).get("complete") is not True
                or regular.get("quality", {}).get("sparse_intervals_allowed")
                is not True
            ):
                raise ScannerInspectionError(
                    f"{day}: canonical 15-minute quality is wrong for {symbol}"
                )
            regular_aggregate = _canonical_bar_aggregate(
                regular, day=day, symbol=symbol
            )
            for field in ("open", "high", "low", "close"):
                if not _same_market_value(
                    regular_aggregate[field], expected[f"provider_{field}"]
                ):
                    raise ScannerInspectionError(
                        f"{day}: canonical 15-minute {field} differs for {symbol}"
                    )
            if regular_aggregate["volume"] != expected["volume"]:
                raise ScannerInspectionError(
                    f"{day}: canonical 15-minute volume differs for {symbol}"
                )
            derived = _one_canonical_dataset(
                document,
                day=day,
                symbol=symbol,
                fields={
                    "kind": "derived",
                    "provider": "alpaca",
                    "channel": "minute_aggregate_regular",
                    "timeframe": "1d",
                    "feed": "sip",
                    "adjustment": "raw",
                    "session": "regular",
                    "scope": "full_session",
                },
            )
            if derived.get("quality", {}).get("complete") is not True:
                raise ScannerInspectionError(
                    f"{day}: canonical daily derivation is incomplete for {symbol}"
                )
            derived_aggregate = _canonical_bar_aggregate(
                derived, day=day, symbol=symbol
            )
            for field in ("open", "high", "low", "close"):
                if not _same_market_value(
                    derived_aggregate[field], expected[f"provider_{field}"]
                ):
                    raise ScannerInspectionError(
                        f"{day}: canonical daily {field} differs for {symbol}"
                    )
            if derived_aggregate["volume"] != expected["volume"]:
                raise ScannerInspectionError(
                    f"{day}: canonical daily volume differs for {symbol}"
                )
            if expected["opening_timestamps"]:
                opening = _one_canonical_dataset(
                    document,
                    day=day,
                    symbol=symbol,
                    fields={
                        "kind": "bars",
                        "provider": "alpaca",
                        "channel": "trades",
                        "timeframe": "1m",
                        "feed": "sip",
                        "adjustment": "raw",
                        "session": "regular",
                        "scope": "opening_window_09_30_09_35",
                    },
                )
                quality = opening.get("quality", {})
                if (
                    quality.get("complete") is not False
                    or quality.get("opening_window_complete")
                    is not expected["opening_exact"]
                ):
                    raise ScannerInspectionError(
                        f"{day}: canonical opening quality differs for {symbol}"
                    )
                opening_aggregate = _canonical_bar_aggregate(
                    opening, day=day, symbol=symbol
                )
                if opening_aggregate["timestamps"] != expected["opening_timestamps"]:
                    raise ScannerInspectionError(
                        f"{day}: canonical opening timestamps differ for {symbol}"
                    )
                for field in ("open", "high", "low", "close", "volume"):
                    if not _same_market_value(
                        opening_aggregate[field], expected[f"opening_{field}"]
                    ):
                        raise ScannerInspectionError(
                            f"{day}: canonical opening {field} differs for {symbol}"
                        )
                opening_datasets += 1
            documents += 1
            regular_datasets += 1
            derived_datasets += 1
    return {
        "documents_verified": documents,
        "regular_15m_datasets_verified": regular_datasets,
        "opening_1m_datasets_verified": opening_datasets,
        "derived_1d_datasets_verified": derived_datasets,
        "valid": True,
    }


def _master_for_day(
    records: Sequence[Mapping[str, Any]], day: str
) -> dict[str, Mapping[str, Any]]:
    observed = date.fromisoformat(day)
    result: dict[str, Mapping[str, Any]] = {}
    for record in records:
        if record.get("security_type") != "COMMON" or not security_record_covers(
            record, observed
        ):
            continue
        symbol = str(record["symbol"])
        if symbol in result:
            raise ScannerInspectionError(
                f"security master duplicates {symbol} on {day}"
            )
        result[symbol] = record
    return result


def _independently_recompute_detail(
    *,
    manifest: Mapping[str, Any],
    rules: Mapping[str, Any],
    security_path: Path,
    calendar_path: Path,
    split_path: Path,
    source_root: Path,
) -> dict[str, Any]:
    records = load_security_master(security_path)
    calendar = load_calendar(calendar_path)
    positions = {day: index for index, day in enumerate(calendar)}
    split_actions = _load_independent_split_actions(split_path)
    required = list(manifest["collection_contract"]["required_session_dates"])
    bars = {
        day: _load_independent_source_day(
            source_root / "minute_aggs" / day[:4] / f"{day}.csv.gz", day
        )
        for day in required
    }
    thresholds = rules["thresholds"]
    allowed_exchanges = set(rules["allowed_primary_exchanges"])
    dates: dict[str, Any] = {}
    for target in sorted(manifest["requested_dates"]):
        position = positions.get(target)
        if position is None or position < 15:
            raise ScannerInspectionError(f"calendar cannot support target {target}")
        lookback = calendar[position - 15 : position]
        if any(day not in bars for day in [*lookback, target]):
            raise ScannerInspectionError(
                f"source contract cannot support target {target}"
            )
        master = _master_for_day(records, target)
        evaluations: list[dict[str, Any]] = []
        for symbol, identity in sorted(master.items()):
            target_bar = bars[target].get(symbol)
            prior = [(day, bars[day].get(symbol)) for day in lookback]
            if str(identity["primary_exchange"]) not in allowed_exchanges:
                disposition = "exchange_not_allowed"
            elif target_bar is None or not target_bar["opening_exact"]:
                disposition = "incomplete_target_opening_bar"
            elif any(item is None for _, item in prior):
                disposition = "incomplete_prior_session_history"
            elif any(not item["opening_exact"] for _, item in prior[-14:]):
                disposition = "incomplete_prior_opening_history"
            else:
                disposition = ""
            row: dict[str, Any] = {
                "symbol": symbol,
                "instrument_id": identity["instrument_id"],
                "primary_exchange": identity["primary_exchange"],
            }
            if not disposition and target_bar is not None:
                complete_prior = [(day, item) for day, item in prior if item is not None]
                factors = {
                    day: _independent_split_factor(
                        symbol, day, target, split_actions
                    )
                    for day, _ in complete_prior
                }
                previous_day, previous = complete_prior[-1]
                previous_close = previous["close"] * factors[previous_day]
                opening_mean = sum(
                    item["opening_volume"] / factors[day]
                    for day, item in complete_prior[-14:]
                ) / 14
                adv = sum(
                    item["volume"] / factors[day]
                    for day, item in complete_prior[-14:]
                ) / 14
                true_ranges: list[float] = []
                for offset, (day, item) in enumerate(complete_prior[-14:], 1):
                    earlier_day, earlier = complete_prior[-15 + offset - 1]
                    factor = factors[day]
                    earlier_close = earlier["close"] * factors[earlier_day]
                    true_ranges.append(
                        max(
                            (item["high"] - item["low"]) * factor,
                            abs(item["high"] * factor - earlier_close),
                            abs(item["low"] * factor - earlier_close),
                        )
                    )
                atr = sum(true_ranges) / 14
                rvol = target_bar["opening_volume"] / opening_mean if opening_mean else 0.0
                opening_return = (
                    target_bar["opening_close"] / previous_close - 1.0
                    if previous_close
                    else 0.0
                )
                row.update(
                    {
                        "open_price": target_bar["opening_open"],
                        "opening_high": target_bar["opening_high"],
                        "opening_low": target_bar["opening_low"],
                        "opening_close": target_bar["opening_close"],
                        "opening_volume": target_bar["opening_volume"],
                        "prior_opening_volume_mean_14": opening_mean,
                        "opening_relative_volume": rvol,
                        "average_daily_volume_14": adv,
                        "daily_atr_14": atr,
                        "split_adjustment_factor_oldest_session": factors[
                            complete_prior[0][0]
                        ],
                        "prior_close": previous_close,
                        "opening_return": opening_return,
                        "bullish_opening_candle": target_bar["opening_close"]
                        > target_bar["opening_open"],
                    }
                )
                disposition = _computed_disposition(row, thresholds)
            row["disposition"] = disposition
            evaluations.append(row)
        eligible = [row for row in evaluations if row["disposition"] == "eligible"]
        eligible.sort(
            key=lambda row: (
                -float(row["opening_relative_volume"]),
                -float(row["opening_return"]),
                str(row["symbol"]),
            )
        )
        for rank, row in enumerate(eligible, 1):
            row["opening_rvol_rank"] = rank
        dates[target] = {
            "master_common_stock_count": len(master),
            "evaluations": evaluations,
            "selected_symbols": [
                row["symbol"] for row in eligible[: int(rules["shortlist_size"])]
            ],
        }
    return {"dates": dates}


def _compare_independent_detail(
    actual: Mapping[str, Any], expected: Mapping[str, Any]
) -> None:
    actual_dates = actual.get("dates")
    expected_dates = expected.get("dates")
    if not isinstance(actual_dates, Mapping) or not isinstance(expected_dates, Mapping):
        raise ScannerInspectionError("independent scanner details are malformed")
    if sorted(actual_dates) != sorted(expected_dates):
        raise ScannerInspectionError("independent scanner dates do not match")
    for day, expected_day in expected_dates.items():
        actual_day = actual_dates[day]
        if actual_day.get("master_common_stock_count") != expected_day.get(
            "master_common_stock_count"
        ):
            raise ScannerInspectionError(
                f"{day}: independently recomputed master count differs"
            )
        if actual_day.get("selected_symbols") != expected_day.get("selected_symbols"):
            raise ScannerInspectionError(
                f"{day}: independently recomputed shortlist differs"
            )
        actual_rows = {
            str(row.get("symbol")): row for row in actual_day.get("evaluations", [])
        }
        expected_rows = {
            str(row.get("symbol")): row for row in expected_day.get("evaluations", [])
        }
        if not actual_rows or set(actual_rows) != set(expected_rows):
            raise ScannerInspectionError(
                f"{day}: independently recomputed population differs"
            )
        for symbol, expected_row in expected_rows.items():
            actual_row = actual_rows[symbol]
            if set(actual_row) != set(expected_row):
                raise ScannerInspectionError(
                    f"{day}: independently recomputed fields differ for {symbol}"
                )
            for field, expected_value in expected_row.items():
                actual_value = actual_row[field]
                if isinstance(expected_value, float):
                    if isinstance(actual_value, bool) or not math.isclose(
                        float(actual_value), expected_value, rel_tol=1e-12, abs_tol=1e-12
                    ):
                        raise ScannerInspectionError(
                            f"{day}: independently recomputed {field} differs for {symbol}"
                        )
                elif actual_value != expected_value:
                    raise ScannerInspectionError(
                        f"{day}: independently recomputed {field} differs for {symbol}"
                    )


def inspect_payloads(
    *,
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    detail: Mapping[str, Any],
    rules: Mapping[str, Any],
    source_status: Mapping[str, Any],
    independent_detail: Mapping[str, Any],
    detail_sha256: str,
    summary_sha256: str,
) -> dict[str, Any]:
    if (
        not str(manifest.get("dataset_id") or "").startswith(
            "dataset-production-scanner-replay-"
        )
        or summary.get("dataset_id") != manifest.get("dataset_id")
    ):
        raise ScannerInspectionError(
            "manifest and summary must name the active dataset"
        )
    if summary.get("status") != "READY" or summary.get("complete_universe") is not True:
        raise ScannerInspectionError("scanner summary is not a complete built replay")
    if detail.get("dataset_id", manifest.get("dataset_id")) != manifest.get(
        "dataset_id"
    ):
        raise ScannerInspectionError("scanner detail names a different dataset")
    if summary.get("selection_is_dynamic") is not True:
        raise ScannerInspectionError("scanner summary is not dynamically selected")
    if summary.get("source", {}).get("contract_sha256") != manifest.get(
        "manifest_sha256"
    ):
        raise ScannerInspectionError(
            "scanner summary is not bound to the frozen manifest"
        )
    if not source_status.get("complete") or not source_status.get("valid"):
        raise ScannerInspectionError("external scanner source collection is incomplete")
    required_sessions = int(manifest["collection_contract"]["required_session_count"])
    if source_status.get("session_files", {}).get("ready") != required_sessions:
        raise ScannerInspectionError("source session count does not match the manifest")
    requested = sorted(str(item) for item in manifest["requested_dates"])
    if summary.get("requested_dates") != requested:
        raise ScannerInspectionError(
            "scanner summary changed the frozen requested-date contract"
        )
    if (
        summary.get("selection_time_et") != "09:35:00"
        or summary.get("information_cutoff") != "TARGET_SESSION_09:35_ET"
    ):
        raise ScannerInspectionError("scanner summary changed the information cutoff")
    summary_dates = summary.get("dates")
    detail_dates = detail.get("dates")
    if not isinstance(summary_dates, list) or not isinstance(detail_dates, Mapping):
        raise ScannerInspectionError("scanner results have invalid date collections")
    if [item.get("date") for item in summary_dates] != requested or sorted(
        detail_dates
    ) != requested:
        raise ScannerInspectionError(
            "scanner results changed the frozen date selection"
        )
    if int(summary.get("completed_dates", -1)) != len(requested):
        raise ScannerInspectionError("scanner completed-date count is wrong")
    if detail.get("scanner_rules_sha256") != summary.get("scanner_rules_sha256"):
        raise ScannerInspectionError("detail and summary scanner-rule hashes differ")
    if detail.get("security_master_sha256") != summary.get("security_master_sha256"):
        raise ScannerInspectionError("detail and summary security-master hashes differ")
    if detail.get("split_actions_sha256") != summary.get("split_actions_sha256"):
        raise ScannerInspectionError("detail and summary split hashes differ")
    if summary.get("detailed_artifact", {}).get("sha256") != detail_sha256:
        raise ScannerInspectionError(
            "summary does not hash the inspected detail artifact"
        )
    _compare_independent_detail(detail, independent_detail)

    thresholds = rules.get("thresholds")
    if not isinstance(thresholds, Mapping):
        raise ScannerInspectionError("scanner rules lack thresholds")
    aggregate_rejections: Counter[str] = Counter()
    total_evaluated = 0
    total_eligible = 0
    total_selected = 0
    split_adjusted = 0
    date_evidence: list[dict[str, Any]] = []
    for public in summary_dates:
        day = str(public["date"])
        private = detail_dates[day]
        evaluations = private.get("evaluations")
        selected_symbols = private.get("selected_symbols")
        if not isinstance(evaluations, list) or not isinstance(selected_symbols, list):
            raise ScannerInspectionError(f"{day}: detail rows are malformed")
        symbols = [str(item.get("symbol") or "") for item in evaluations]
        if any(not item for item in symbols) or len(symbols) != len(set(symbols)):
            raise ScannerInspectionError(f"{day}: symbols are empty or duplicated")
        reasons: Counter[str] = Counter()
        eligible: list[Mapping[str, Any]] = []
        for row in evaluations:
            if not isinstance(row, Mapping):
                raise ScannerInspectionError(f"{day}: evaluation row is malformed")
            disposition = str(row.get("disposition") or "")
            if disposition not in EARLY_REJECTIONS | COMPUTED_REJECTIONS:
                raise ScannerInspectionError(
                    f"{day}: unsupported rejection {disposition!r}"
                )
            reasons[disposition] += 1
            if disposition in COMPUTED_REJECTIONS:
                expected = _computed_disposition(row, thresholds)
                if disposition != expected:
                    raise ScannerInspectionError(
                        f"{day}: {row.get('symbol')} disposition {disposition} should be {expected}"
                    )
                split_adjusted += int(
                    not math.isclose(
                        _number(row, "split_adjustment_factor_oldest_session"), 1.0
                    )
                )
            if disposition == "eligible":
                eligible.append(row)
        eligible.sort(
            key=lambda row: (
                -_number(row, "opening_relative_volume"),
                -_number(row, "opening_return"),
                str(row["symbol"]),
            )
        )
        for rank, row in enumerate(eligible, 1):
            if int(row.get("opening_rvol_rank", -1)) != rank:
                raise ScannerInspectionError(
                    f"{day}: eligible rank is not deterministic"
                )
        selected = eligible[: int(rules["shortlist_size"])]
        expected_symbols = [str(row["symbol"]) for row in selected]
        if selected_symbols != expected_symbols:
            raise ScannerInspectionError(
                f"{day}: selected symbols do not match ranking"
            )
        shortlist_hash = _sha256_json(
            [
                {
                    "symbol": row["symbol"],
                    "instrument_id": row["instrument_id"],
                    "opening_relative_volume": row["opening_relative_volume"],
                    "opening_return": row["opening_return"],
                    "rank": row["opening_rvol_rank"],
                }
                for row in selected
            ]
        )
        expected_public = {
            "master_common_stock_count": len(evaluations),
            "evaluated_count": len(evaluations),
            "eligible_count": len(eligible),
            "rejection_counts": dict(sorted(reasons.items())),
            "shortlist_count": len(selected),
            "shortlist_sha256": shortlist_hash,
        }
        for field, expected in expected_public.items():
            if public.get(field) != expected:
                raise ScannerInspectionError(f"{day}: public {field} is inconsistent")
        if private.get("master_common_stock_count") != len(evaluations):
            raise ScannerInspectionError(f"{day}: master denominator is inconsistent")
        aggregate_rejections.update(reasons)
        total_evaluated += len(evaluations)
        total_eligible += len(eligible)
        total_selected += len(selected)
        date_evidence.append(
            {
                "date": day,
                "evaluated": len(evaluations),
                "eligible": len(eligible),
                "selected": len(selected),
                "shortlist_sha256": shortlist_hash,
                "valid": True,
            }
        )

    return {
        "schema_version": 1,
        "dataset_id": str(manifest["dataset_id"]),
        "status": "INSPECTED",
        "valid": True,
        "manifest_sha256": manifest["manifest_sha256"],
        "summary_sha256": summary_sha256,
        "detail_sha256": detail_sha256,
        "source_sessions_verified": required_sessions,
        "completed_dates": len(requested),
        "total_evaluated": total_evaluated,
        "total_eligible": total_eligible,
        "total_selected": total_selected,
        "split_adjusted_evaluations": split_adjusted,
        "aggregate_rejection_counts": dict(sorted(aggregate_rejections.items())),
        "date_evidence": date_evidence,
        "invariants": {
            "frozen_dates_unchanged": True,
            "source_artifacts_rehashed": True,
            "denominators_recomputed": True,
            "threshold_dispositions_recomputed": True,
            "raw_source_metrics_recomputed": True,
            "security_master_population_recomputed": True,
            "split_adjustments_recomputed": True,
            "rankings_recomputed": True,
            "shortlist_hashes_recomputed": True,
            "broker_actions_allowed": False,
            "strategy_rules_changed": False,
        },
        "claim_boundary": summary.get("claim_boundary"),
    }


def inspect_replay(
    *,
    manifest_path: Path,
    summary_path: Path,
    detail_path: Path,
    rules_path: Path,
    strategy_attestation_path: Path,
    calendar_path: Path,
    calendar_attestation_path: Path,
    split_path: Path,
    split_attestation_path: Path,
    env_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    manifest = load_contract(manifest_path)
    rules, security_path = verify_contract_inputs(manifest, rules_path=rules_path)
    validate_rules(rules_path)
    summary = _read_object(summary_path)
    detail = _read_object(detail_path)
    config = HistoricalStoreConfig.from_env(env_path)
    store = HistoricalDayStore(config.root)
    source_status = collection_status(manifest, store=store)
    source_root = index_root(store, str(manifest["dataset_id"]))
    source_attestations = _verify_source_attestations(manifest, source_root)
    source_hashes = source_attestations["source_hashes"]
    for field in (
        "provider_requests",
        "provider_retries",
        "canonical_day_merges",
        "derived_rows",
        "reused_sessions",
        "inherited_derived_rows",
        "delta_symbols_requested",
    ):
        if source_status.get(field) != source_attestations[field]:
            raise ScannerInspectionError(
                f"source collection aggregate {field} is inconsistent"
            )
    source_summary = summary.get("source")
    if not isinstance(source_summary, Mapping) or any(
        (
            source_summary.get("provider") != "Alpaca",
            source_summary.get("feed") != "sip",
            source_summary.get("adjustment") != "raw",
            source_summary.get("session_artifact_count") != len(source_hashes),
            source_summary.get("session_artifacts_sha256")
            != _sha256_json(source_hashes),
        )
    ):
        raise ScannerInspectionError(
            "scanner summary does not bind the verified source collection"
        )
    if summary.get("scanner_rules_sha256") != _sha256_json(rules):
        raise ScannerInspectionError("summary does not match the frozen scanner rules")
    if summary.get("security_master_sha256") != security_master_sha256(security_path):
        raise ScannerInspectionError("summary does not match the frozen security master")
    if summary.get("split_actions_sha256") != _sha256_file(split_path):
        raise ScannerInspectionError("summary does not match the inspected split actions")
    if "split_actions_path" in manifest["dataset_payload"]["universe_contract"]:
        split_attestation = _verify_manifest_bound_split_actions(
            manifest_path=manifest_path,
            manifest=manifest,
            split_path=split_path,
            earliest_source_capture=str(
                source_attestations["earliest_captured_at"]
            ),
        )
    else:
        split_attestation = _verify_pre_collection_split_attestation(
            split_attestation_path, split_path
        )
    frozen_strategy_source = manifest["dataset_payload"]["universe_contract"].get(
        "production_strategy_attestation_path"
    )
    if frozen_strategy_source is not None and (
        PROJECT_ROOT / str(frozen_strategy_source)
    ).resolve() != strategy_attestation_path.resolve():
        raise ScannerInspectionError(
            "inspector strategy attestation differs from the frozen manifest"
        )
    strategy_attestation = _verify_pre_collection_strategy_attestation(
        strategy_attestation_path, rules
    )
    calendar_attestation = _verify_pre_collection_calendar_attestation(
        calendar_attestation_path, calendar_path
    )
    independent_detail = _independently_recompute_detail(
        manifest=manifest,
        rules=rules,
        security_path=security_path,
        calendar_path=calendar_path,
        split_path=split_path,
        source_root=source_root,
    )
    canonical_persistence = _verify_canonical_persistence(
        manifest, store=store, source_root=source_root
    )
    result = inspect_payloads(
        manifest=manifest,
        summary=summary,
        detail=detail,
        rules=rules,
        source_status=source_status,
        independent_detail=independent_detail,
        detail_sha256=_sha256_file(detail_path),
        summary_sha256=_sha256_file(summary_path),
    )
    result["split_actions"] = split_attestation
    result["session_calendar"] = calendar_attestation
    result["production_strategy"] = strategy_attestation
    result["source_collection"] = {
        key: value
        for key, value in source_attestations.items()
        if key != "source_hashes"
    }
    result["canonical_persistence"] = canonical_persistence
    result["invariants"]["split_actions_pre_collection_attested"] = True
    result["invariants"]["session_calendar_pre_collection_attested"] = True
    result["invariants"]["summary_source_collection_bound"] = True
    result["invariants"]["production_strategy_pre_collection_attested"] = True
    result["invariants"]["source_collection_post_freeze_attested"] = True
    result["invariants"]["canonical_source_observations_verified"] = True
    _write_json(output_path, result)
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--detail", type=Path, default=DEFAULT_DETAIL)
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument(
        "--strategy-attestation", type=Path, default=DEFAULT_STRATEGY_ATTESTATION
    )
    parser.add_argument("--calendar", type=Path, default=DEFAULT_CALENDAR)
    parser.add_argument(
        "--calendar-attestation", type=Path, default=DEFAULT_CALENDAR_ATTESTATION
    )
    parser.add_argument("--splits", type=Path, default=DEFAULT_SPLITS)
    parser.add_argument(
        "--split-attestation", type=Path, default=DEFAULT_SPLIT_ATTESTATION
    )
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = inspect_replay(
            manifest_path=args.manifest,
            summary_path=args.summary,
            detail_path=args.detail,
            rules_path=args.rules,
            strategy_attestation_path=args.strategy_attestation,
            calendar_path=args.calendar,
            calendar_attestation_path=args.calendar_attestation,
            split_path=args.splits,
            split_attestation_path=args.split_attestation,
            env_path=args.env,
            output_path=args.output,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        KeyError,
        OSError,
        ScannerInspectionError,
        ScannerReplayError,
        HistoricalStoreError,
        LearningDataError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
