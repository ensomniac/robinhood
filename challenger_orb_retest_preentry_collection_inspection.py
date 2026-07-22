"""Independently inspect challenger causal pre-entry collection integrity."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import challenger_orb_retest_preentry as preentry
from historical_providers import HistoricalProviderError
from historical_service import LocalHistoricalClient
from historical_store import HistoricalDayStore, HistoricalStoreError
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-21-challenger-orb-retest-preentry-collection-inspection.json"
)


class ChallengerPreentryCollectionInspectionError(RuntimeError):
    """The frozen causal collection does not independently reconcile."""


def inspect_collection(
    *, manifest_path: Path, env_path: Path, status_path: Path, result_path: Path
) -> dict[str, Any]:
    manifest, config, selection = preentry._load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=False,
    )
    index_path = preentry._index_path(config.root)
    index = preentry._read_gzip(index_path)
    rebuilt_index = preentry._build_index(manifest, selection, config.root)
    if rebuilt_index != index or index.get("status") != "COLLECTION_COMPLETE":
        raise ChallengerPreentryCollectionInspectionError(
            "private collection index does not rebuild as complete"
        )

    public_status = preentry._read_json(status_path)
    if not (
        public_status.get("dataset_id") == preentry.DATASET_ID
        and public_status.get("manifest_sha256") == manifest["manifest_sha256"]
        and public_status.get("status") == "COLLECTION_COMPLETE"
        and public_status.get("counts") == index.get("counts")
        and public_status.get("private_collection_sha256")
        == preentry._sha256_file(index_path)
        and public_status.get("post_entry_data_accessed") is False
        and public_status.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerPreentryCollectionInspectionError(
            "public collection status does not bind the private collection"
        )

    expected = {
        str(row["request_sha256"]): row for row in selection.get("requests", [])
    }
    records = {
        str(row["request_sha256"]): row for row in index.get("records", [])
    }
    if set(records) != set(expected):
        raise ChallengerPreentryCollectionInspectionError(
            "terminal request denominator differs"
        )

    local = LocalHistoricalClient(
        HistoricalDayStore(config.root), "alpaca", feed="sip", adjustment="raw"
    )
    row_count = 0
    origins: Counter[str] = Counter()
    kinds: Counter[str] = Counter()
    wrapper_hashes: list[dict[str, str]] = []
    canonical_row_hashes: list[dict[str, Any]] = []
    for request_sha256, request in sorted(expected.items()):
        record = records[request_sha256]
        wrapper = preentry._read_gzip(
            preentry._wrapper_path(config.root, request_sha256)
        )
        if not (
            wrapper.get("status") == "SUCCESS"
            and wrapper.get("dataset_id") == preentry.DATASET_ID
            and wrapper.get("request_sha256") == request_sha256
            and wrapper.get("manifest_sha256") == manifest["manifest_sha256"]
            and wrapper.get("error_category") is None
            and wrapper.get("target_outcomes_observed_or_derived") is False
            and preentry._sha256_json(wrapper) == record.get("wrapper_sha256")
            and record.get("status") == "SUCCESS"
            and record.get("kind") == request.get("kind")
            and record.get("origin") == wrapper.get("origin")
            and record.get("row_count") == wrapper.get("row_count")
            and record.get("rows_sha256") == wrapper.get("rows_sha256")
            and record.get("error_category") is None
        ):
            raise ChallengerPreentryCollectionInspectionError(
                "terminal wrapper or collection record differs"
            )
        rows = preentry._load_rows(request=request, wrapper=wrapper, local=local)
        observed_rows_sha256 = preentry._sha256_json(rows)
        if observed_rows_sha256 != wrapper.get("rows_sha256"):
            raise ChallengerPreentryCollectionInspectionError(
                "canonical row digest differs"
            )
        row_count += len(rows)
        origins[str(wrapper.get("origin"))] += 1
        kinds[str(request.get("kind"))] += 1
        wrapper_hashes.append(
            {
                "request_sha256": request_sha256,
                "wrapper_sha256": str(record["wrapper_sha256"]),
            }
        )
        canonical_row_hashes.append(
            {
                "request_sha256": request_sha256,
                "row_count": len(rows),
                "rows_sha256": observed_rows_sha256,
            }
        )

    counts = index.get("counts", {})
    if not (
        counts.get("expected_requests") == len(expected)
        and counts.get("terminal_requests") == len(expected)
        and counts.get("successful_requests") == len(expected)
        and counts.get("failed_requests") == 0
        and counts.get("pending_requests") == 0
        and counts.get("rows") == row_count
        and counts.get("candidate_bars_requests") == kinds["candidate_bars"]
        and counts.get("candidate_trades_requests") == kinds["candidate_trades"]
        and counts.get("benchmark_bars_requests") == kinds["benchmark_bars"]
        and index.get("request_graph_sha256")
        == selection.get("request_graph_sha256")
        and index.get("post_entry_data_accessed") is False
        and index.get("target_outcomes_observed_or_derived") is False
        and not preentry._trigger_path(config.root).exists()
    ):
        raise ChallengerPreentryCollectionInspectionError(
            "collection completeness or pre-trigger boundary differs"
        )

    result = {
        "schema_version": 1,
        "dataset_id": preentry.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_INSPECTED",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": dict(counts),
        "request_kind_counts": dict(sorted(kinds.items())),
        "source_origins": dict(sorted(origins.items())),
        "private_collection_file_sha256": preentry._sha256_file(index_path),
        "private_collection_content_sha256": preentry._sha256_json(index),
        "wrapper_set_sha256": preentry._sha256_json(wrapper_hashes),
        "canonical_row_set_sha256": preentry._sha256_json(canonical_row_hashes),
        "inspection": {
            "selection_rebuilt": True,
            "collection_index_rebuilt": True,
            "public_private_binding_rebuilt": True,
            "complete_request_denominator_reconciled": True,
            "all_terminal_wrappers_rehashed": True,
            "all_canonical_rows_reloaded_and_rehashed": True,
            "zero_trigger_artifacts_verified": True,
            "outcome_lock_verified": True,
        },
        "next_required_stage": (
            "commit and push this collection inspection before causal trigger "
            "derivation"
        ),
        "claim_boundary": (
            "Complete frozen causal input transport only. No trigger, fill, "
            "return, outcome, alpha, maturity, production, or broker claim is "
            "established."
        ),
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "production_rule_change_earned": False,
        "symbols_dates_rows_and_sources_public": False,
        "valid": True,
    }
    preentry._write_json(result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--status", type=Path, default=preentry.DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--result", type=Path, default=DEFAULT_RESULT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        value = inspect_collection(
            manifest_path=args.manifest,
            env_path=args.env_file,
            status_path=args.status,
            result_path=args.result,
        )
    except (
        ChallengerPreentryCollectionInspectionError,
        preentry.ChallengerPreentryError,
        HistoricalProviderError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
