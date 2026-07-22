"""Independently inspect second-tranche accession transport before semantics."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import challenger_sec_accession_chain_recovery2 as challenger
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-sec-accession-chain-"
    "tranche2-collection-inspection.json"
)


class ChallengerSecAccessionCollectionInspection2Error(RuntimeError):
    """The frozen second-tranche recovery collection does not reconcile."""


def inspect_collection(
    *, manifest_path: Path, env_path: Path, result_path: Path
) -> dict[str, Any]:
    challenger.configure_base()
    base = challenger.base
    manifest, config, selection, _publication = base._load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=False,
    )
    index_path = base._index_path(config.root)
    index = base._read_gzip(index_path)
    rebuilt_index = base._build_index(
        manifest=manifest,
        selection=selection,
        store_root=config.root,
    )
    if rebuilt_index != index:
        raise ChallengerSecAccessionCollectionInspection2Error(
            "private collection index does not rebuild"
        )
    records = {
        str(row["request_sha256"]): row for row in index.get("records", [])
    }
    expected = {
        str(row["request_sha256"]): row for row in selection.get("requests", [])
    }
    if set(records) != set(expected):
        raise ChallengerSecAccessionCollectionInspection2Error(
            "terminal request denominator differs"
        )
    source_bytes = 0
    source_origins: dict[str, int] = {}
    wrapper_hashes: list[dict[str, str]] = []
    for request_sha256, request in sorted(expected.items()):
        record = records[request_sha256]
        wrapper_path = base._wrapper_path(config.root, request_sha256)
        wrapper = base._read_gzip(wrapper_path)
        if not (
            wrapper.get("status") == "SUCCESS"
            and wrapper.get("request_sha256") == request_sha256
            and wrapper.get("manifest_sha256") == manifest["manifest_sha256"]
            and wrapper.get("error") is None
        ):
            raise ChallengerSecAccessionCollectionInspection2Error(
                "terminal wrapper is not a successful frozen request"
            )
        raw = base._shared_path(config.root, request).read_bytes()
        base._transport_integrity(raw)
        raw_sha256 = hashlib.sha256(raw).hexdigest()
        if not (
            len(raw) == int(wrapper.get("source_bytes") or 0)
            and raw_sha256 == wrapper.get("source_sha256")
            and base._sha256_json(wrapper) == record.get("wrapper_sha256")
        ):
            raise ChallengerSecAccessionCollectionInspection2Error(
                "raw accession or wrapper hash differs"
            )
        source_bytes += len(raw)
        origin = str(wrapper.get("source_origin"))
        source_origins[origin] = source_origins.get(origin, 0) + 1
        wrapper_hashes.append(
            {
                "request_sha256": request_sha256,
                "wrapper_sha256": record["wrapper_sha256"],
            }
        )
    counts = index.get("counts", {})
    if not (
        counts.get("expected_requests") == challenger.EXPECTED_UNIQUE_ACCESSIONS
        and counts.get("terminal_requests") == challenger.EXPECTED_UNIQUE_ACCESSIONS
        and counts.get("successful_requests") == challenger.EXPECTED_UNIQUE_ACCESSIONS
        and counts.get("failed_requests") == 0
        and counts.get("pending_requests") == 0
        and counts.get("source_bytes") == source_bytes
        and not base._reviewed_path(config.root).exists()
    ):
        raise ChallengerSecAccessionCollectionInspection2Error(
            "collection completeness or pre-review boundary differs"
        )
    result = {
        "schema_version": 1,
        "dataset_id": challenger.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_INSPECTED",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": dict(counts),
        "source_origins": dict(sorted(source_origins.items())),
        "wrapper_set_sha256": base._sha256_json(wrapper_hashes),
        "private_collection_content_sha256": base._sha256_json(index),
        "inspection": {
            "selection_rebuilt": True,
            "collection_index_rebuilt": True,
            "complete_request_denominator_reconciled": True,
            "all_terminal_wrappers_rehashed": True,
            "all_raw_source_bytes_rehashed": True,
            "transport_integrity_revalidated": True,
            "zero_review_artifacts_verified": True,
        },
        "next_required_stage": (
            "commit and push this collection inspection before deterministic "
            "issuer-filed EX-99 review"
        ),
        "claim_boundary": (
            "Complete accession transport and frozen request provenance only. "
            "No exhibit semantic, return, outcome, alpha, maturity, production, "
            "or broker claim is established."
        ),
        "source_semantics_classified": False,
        "outcome_contract_permitted": False,
        "production_rule_change_earned": False,
        "symbols_ciks_accessions_urls_sources_text_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    base._write_json(result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--result", type=Path, default=DEFAULT_RESULT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        value = inspect_collection(
            manifest_path=args.manifest,
            env_path=args.env_file,
            result_path=args.result,
        )
    except (
        ChallengerSecAccessionCollectionInspection2Error,
        challenger.ChallengerSecAccessionChain2Error,
        challenger.base.DevelopmentSecAccessionChainError,
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
