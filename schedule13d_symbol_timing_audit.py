"""Audit SEC UTC versus complete-submission Eastern acceptance semantics."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import schedule13d_capacity as capacity
import schedule13d_semantic_capacity as semantic
import schedule13d_symbol_submissions as submissions
import schedule13d_symbol_supplemental as supplemental
from historical_discovery import _accepted_at, _submission_recent
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
EASTERN = ZoneInfo("America/New_York")
OUTPUT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/symbols/corrections"


class Schedule13dSymbolTimingAuditError(RuntimeError):
    """The symbol-source acceptance-time audit is incomplete or inconsistent."""


def _events() -> dict[str, list[dict[str, Any]]]:
    state = semantic._read_gzip_object(semantic._private_result_path())
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in state["records"]:
        if row.get("status") == "PENDING_CAUSAL_SYMBOL_SUPPLEMENTAL":
            result[str(row["subject_cik"])].append(dict(row))
    if sum(len(rows) for rows in result.values()) != 317:
        raise Schedule13dSymbolTimingAuditError("pending event denominator drifted")
    return result


def _main_graph() -> dict[str, Any]:
    matches = sorted(
        PROJECT_ROOT.glob(
            "strategy_tournament/v2/schedule13d/symbols/submissions/manifests/"
            f"{capacity.CANDIDATE_ID}-*.json"
        )
    )
    if len(matches) != 1:
        raise Schedule13dSymbolTimingAuditError(
            f"expected one main submissions graph; found {len(matches)}"
        )
    return submissions.load_request_graph(matches[0])


def _supplemental_graph() -> dict[str, Any]:
    matches = sorted(
        PROJECT_ROOT.glob(
            "strategy_tournament/v2/schedule13d/symbols/supplemental/manifests/"
            f"{capacity.CANDIDATE_ID}-*.json"
        )
    )
    if len(matches) != 1:
        raise Schedule13dSymbolTimingAuditError(
            f"expected one supplemental graph; found {len(matches)}"
        )
    return supplemental.load_request_graph(matches[0])


def _cutoff_utc(accepted_at: str) -> datetime:
    parsed = datetime.fromisoformat(accepted_at)
    if parsed.tzinfo is not None:
        raise Schedule13dSymbolTimingAuditError(
            "complete-submission acceptance unexpectedly has an offset"
        )
    return parsed.replace(tzinfo=EASTERN).astimezone(UTC)


def _has_prior_utc(payload: Mapping[str, Any], accepted_at: str) -> bool:
    cutoff = _cutoff_utc(accepted_at)
    allowed = set(submissions.ALLOWED_FORMS)
    for row in _submission_recent(payload).values():
        accepted = _accepted_at(row.get("acceptanceDateTime"))
        if (
            row.get("form") in allowed
            and accepted is not None
            and accepted.astimezone(UTC) <= cutoff
        ):
            return True
    return False


def build_audit() -> dict[str, Any]:
    events = _events()
    main = _main_graph()
    old = _supplemental_graph()
    store = submissions._store_config()
    covered = 0
    missing_without_descriptor = 0
    required_descriptors: set[str] = set()
    lexical_mismatches = 0
    for request in main["request_contract"]["requests"]:
        cik = str(request["subject_cik"])
        payload = json.loads(
            submissions._resolve_cache_path(store.root, request).read_text(
                encoding="utf-8"
            )
        )
        descriptors = [
            value
            for value in payload.get("filings", {}).get("files", [])
            if isinstance(value, Mapping) and value.get("name")
        ]
        for event in events[cik]:
            accepted_at = str(event["accepted_at"])
            old_lexical = supplemental._has_prior_main(payload, accepted_at)
            corrected = _has_prior_utc(payload, accepted_at)
            lexical_mismatches += old_lexical != corrected
            if corrected:
                covered += 1
            elif descriptors:
                required_descriptors.update(
                    f"https://data.sec.gov/submissions/{value['name']}"
                    for value in descriptors
                )
            else:
                missing_without_descriptor += 1
    old_urls = {
        str(row["url"]) for row in old["request_contract"]["requests"]
    }
    if not (
        covered == 280
        and missing_without_descriptor == 35
        and 317 - covered - missing_without_descriptor == 2
        and lexical_mismatches == 1
        and required_descriptors == old_urls
        and len(required_descriptors) == 3
    ):
        raise Schedule13dSymbolTimingAuditError(
            "corrected acceptance-time partition differs from audited facts"
        )
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "audit_kind": "sec-acceptance-time-source-partition-correction",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": semantic.documents.indexes.CONTRACT_SHA256,
        "main_request_graph_sha256": main["request_graph_sha256"],
        "superseded_partition_graph_sha256": old["request_graph_sha256"],
        "time_semantics": {
            "complete_submission_acceptance": "America/New_York local wall time",
            "sec_submissions_acceptance": "UTC Z timestamp",
            "comparison": "convert complete-submission wall time to UTC before ordering",
        },
        "superseded_partition": {
            "events_with_prior_in_main_metadata": 279,
            "events_without_prior_and_without_historical_descriptor": 36,
            "events_requiring_historical_metadata": 2,
            "valid": False,
        },
        "corrected_partition": {
            "events_with_prior_in_main_metadata": covered,
            "events_without_prior_and_without_historical_descriptor": (
                missing_without_descriptor
            ),
            "events_requiring_historical_metadata": 2,
            "lexical_comparison_mismatches": lexical_mismatches,
            "valid": True,
        },
        "source_scope": {
            "corrected_required_historical_url_count": len(required_descriptors),
            "superseded_graph_url_count": len(old_urls),
            "url_sets_identical": required_descriptors == old_urls,
            "retained_collection_is_complete_for_corrected_scope": True,
            "new_provider_requests_required": 0,
        },
        "corrected_primary_document_freeze_permitted": True,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "valid": True,
    }
    result["audit_sha256"] = capacity.successor._self_hash(
        result, "audit_sha256"
    )
    return result


def audit_path(value: Mapping[str, Any]) -> Path:
    return OUTPUT_ROOT / f"symbol-time-correction-{value['audit_sha256']}.json"


def write_audit() -> tuple[Path, dict[str, Any]]:
    value = build_audit()
    path = audit_path(value)
    semantic._write_json(value, path)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("write", "check"))
    args = parser.parse_args(argv)
    try:
        if args.command == "write":
            path, value = write_audit()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                **value,
            }
        else:
            value = build_audit()
            path = audit_path(value)
            if not path.is_file() or semantic._read_object(path) != value:
                raise Schedule13dSymbolTimingAuditError(
                    "published timing correction does not rebuild"
                )
            result = value
    except (Schedule13dSymbolTimingAuditError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
