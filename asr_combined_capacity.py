"""Combine inspected ASR source tiers into one tradable-capacity disposition."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import asr_security_identity as tier2a
import asr_tier1_security_identity as tier1


PROJECT_ROOT = Path(__file__).resolve().parent
TIER2A_RESULT_PATH = tier2a.DEFAULT_RESULT_ROOT / (
    f"{tier2a.resolution.tier2a.capacity.CANDIDATE_ID}-"
    "07a513cdfcc85a42d1b1d501be4a0c3da814132b077e62c78fc39f7c5848bed6.json"
)
TIER2A_INSPECTION_PATH = (
    tier2a.DEFAULT_RESULT_ROOT
    / "inspections"
    / "identity-3b432fbe620005096f54e9954f1ffbc9a109121565a654449f63c767924e9ede.json"
)
TIER1_RESULT_PATH = tier1.DEFAULT_RESULT_ROOT / (
    f"{tier1.semantic.tier.capacity.CANDIDATE_ID}-"
    "66c8dbf9a7143d9b0cefb4b74bbee3ced5a2689fa47226807ddc605c9f54fa74.json"
)
TIER1_INSPECTION_PATH = (
    tier1.DEFAULT_RESULT_ROOT
    / "inspections"
    / "tier1-identity-7395be4f63ee56485f6aac3abc6a47858127113227fd3b9c1d23596af935e00e.json"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/asr/combined-capacity"


class AsrCombinedCapacityError(RuntimeError):
    """The combined ASR capacity result is invalid."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrCombinedCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrCombinedCapacityError(f"{path} must contain an object")
    return value


def _hash(value: Mapping[str, Any], field: str) -> str:
    return tier2a.self_hash(value, field)


def _private(result: Mapping[str, Any], root: Path, reader: Any) -> dict[str, Any]:
    info = result["private_result"]
    path = root / str(info["cache_relative_path"])
    raw = path.read_bytes() if path.is_file() else b""
    value = reader(path)
    if not (
        hashlib.sha256(raw).hexdigest() == info["file_sha256"]
        and len(raw) == info["bytes"]
        and value.get("private_result_sha256") == info["private_result_sha256"]
        and value.get("private_result_sha256") == _hash(value, "private_result_sha256")
    ):
        raise AsrCombinedCapacityError("private identity result drifted")
    return value


def build_result(*, store_root: Path | None = None) -> dict[str, Any]:
    root = store_root or tier2a.resolution.tier1.shared._store().root
    result2 = _read(TIER2A_RESULT_PATH)
    inspect2 = _read(TIER2A_INSPECTION_PATH)
    result1 = _read(TIER1_RESULT_PATH)
    inspect1 = _read(TIER1_INSPECTION_PATH)
    private2 = _private(result2, root, tier2a._read_gzip)
    private1 = _private(result1, root, tier1._read_gzip)
    if not (
        result2.get("verified_event_count") == 183
        and result2.get("result_sha256") == _hash(result2, "result_sha256")
        and inspect2.get("verified_event_count") == 183
        and inspect2.get("inspection_sha256") == _hash(inspect2, "inspection_sha256")
        and result1.get("verified_event_count") == 2
        and result1.get("independent_disclosure_signal_count") == 1
        and result1.get("result_sha256") == _hash(result1, "result_sha256")
        and inspect1.get("verified_event_count") == 2
        and inspect1.get("independent_disclosure_signal_count") == 1
        and inspect1.get("inspection_sha256") == _hash(inspect1, "inspection_sha256")
        and result2.get("market_outcomes_accessed") is False
        and result1.get("market_outcomes_accessed") is False
    ):
        raise AsrCombinedCapacityError("inspected source-tier lineage differs")
    events2 = list(private2["verified_events"])
    events1 = list(private1["verified_events"])
    accessions2 = {str(event["accession"]) for event in events2}
    accessions1 = {str(event["accession"]) for event in events1}
    if accessions1 & accessions2:
        raise AsrCombinedCapacityError("ASR source tiers overlap by accession")
    signals = {
        (
            str(event["accession"]),
            str(event["ticker"]),
            str(event["acceptance_datetime_raw"]),
        )
        for event in events2 + events1
    }
    signal_count = len(signals)
    if signal_count < 50:
        disposition = "RETIRED_INSUFFICIENT_FORMAL_CAPACITY"
    elif signal_count < 100:
        disposition = "PRESERVED_LATER_SINGLE_RULE_RESEARCH"
    else:
        disposition = "ADMITTED_TO_DEVELOPMENT_SEARCH_PIPELINE"
    result: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "outcome-blind-asr-combined-capacity-disposition",
        "campaign_id": tier2a.resolution.tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": tier2a.resolution.tier2a.capacity.CANDIDATE_ID,
        "source_tiers": {
            "tier1_result_sha256": result1["result_sha256"],
            "tier1_inspection_sha256": inspect1["inspection_sha256"],
            "tier2a_result_sha256": result2["result_sha256"],
            "tier2a_inspection_sha256": inspect2["inspection_sha256"],
            "accession_sets_disjoint": True,
        },
        "verified_agreement_count": len(events1) + len(events2),
        "independent_disclosure_signal_count": signal_count,
        "capacity_counting_rule": (
            "one accession, ticker, and exact acceptance timestamp is one "
            "tradable signal regardless of agreement multiplicity"
        ),
        "capacity_policy": {
            "retire_below": 50,
            "preserve_later_below": 100,
            "fast_lane_at": 100,
        },
        "capacity_disposition": disposition,
        "development_search_contract_freeze_permitted": disposition
        == "ADMITTED_TO_DEVELOPMENT_SEARCH_PIPELINE",
        "market_price_access_permitted": False,
        "forward_return_access_permitted": False,
        "verified_event_outcomes_accessed": False,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "state": "COMBINED_CAPACITY_PENDING_INSPECTION",
        "valid": True,
    }
    result["result_sha256"] = _hash(result, "result_sha256")
    return result


def evaluate(*, output_root: Path = DEFAULT_ROOT) -> tuple[Path, dict[str, Any]]:
    result = build_result()
    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / f"asr-combined-capacity-{result['result_sha256']}.json"
    path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return path, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("evaluate",))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    _parser().parse_args(argv)
    try:
        path, result = evaluate()
    except (AsrCombinedCapacityError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {**result, "written": str(path.relative_to(PROJECT_ROOT))},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
