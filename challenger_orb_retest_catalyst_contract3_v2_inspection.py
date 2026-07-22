"""Inspect the repaired third ORB-retest tranche's frozen source contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import challenger_orb_retest_catalyst_contract3_v2 as contract
from historical_store import HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent


def _verify_publication(
    manifest: Mapping[str, Any],
    *,
    paths: Mapping[str, Path],
    require_published: bool,
) -> None:
    publication = manifest.get("publication_contract")
    expected_paths = {**contract.implementation_paths(), **paths}
    if not isinstance(publication, Mapping):
        raise contract.ChallengerCatalystContract3V2Error(
            "source publication contract is missing"
        )
    if not require_published:
        if publication:
            raise contract.ChallengerCatalystContract3V2Error(
                "test source contract unexpectedly claims publication"
            )
        return
    if set(publication) != set(expected_paths):
        raise contract.ChallengerCatalystContract3V2Error(
            "source publication surface is incomplete"
        )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    upstream = subprocess.run(
        ["git", "rev-parse", "@{upstream}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if head != upstream:
        raise contract.ChallengerCatalystContract3V2Error(
            "inspection requires HEAD to equal its pushed upstream"
        )
    for name, path in expected_paths.items():
        value = publication.get(name)
        expected_path = contract.base._repo_path(path)
        expected_hash = contract.base._sha256_file(path)
        if (
            not isinstance(value, Mapping)
            or not re.fullmatch(r"[0-9a-f]{40}", str(value.get("commit") or ""))
            or value.get("path") != expected_path
            or value.get("sha256") != expected_hash
        ):
            raise contract.ChallengerCatalystContract3V2Error(
                f"publication binding differs for {name}"
            )
        commit = str(value["commit"])
        if (
            subprocess.run(
                ["git", "merge-base", "--is-ancestor", commit, head],
                cwd=PROJECT_ROOT,
                check=False,
                capture_output=True,
            ).returncode
            != 0
        ):
            raise contract.ChallengerCatalystContract3V2Error(
                f"publication commit is not a pushed ancestor for {name}"
            )
        historical = subprocess.run(
            ["git", "show", f"{commit}:{expected_path}"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
        ).stdout
        if hashlib.sha256(historical).hexdigest() != expected_hash:
            raise contract.ChallengerCatalystContract3V2Error(
                f"published source blob differs for {name}"
            )
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--", str(value["path"])],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        if dirty.strip():
            raise contract.ChallengerCatalystContract3V2Error(
                f"published source input is dirty: {value['path']}"
            )


def inspect_contract(
    *,
    manifest_path: Path,
    env_path: Path = PROJECT_ROOT / ".env",
    status_path: Path = contract.DEFAULT_PUBLIC_STATUS,
    source_manifest_path: Path = contract.SOURCE_MANIFEST,
    scanner_manifest_path: Path = contract.SCANNER_MANIFEST,
    scanner_summary_path: Path = contract.SCANNER_SUMMARY,
    scanner_inspection_path: Path = contract.SCANNER_INSPECTION,
    security_master_source_path: Path = contract.SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = contract.STRATEGY_SOURCE,
    selection_doc_path: Path = contract.DEFAULT_SELECTION_DOC,
    dataset_id: str = contract.DATASET_ID,
    require_published: bool = True,
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != dataset_id:
        raise contract.ChallengerCatalystContract3V2Error(
            "unexpected repaired third-tranche source contract"
        )
    stable, config = contract.stable_contract(
        source_manifest_path=source_manifest_path,
        env_path=env_path,
        dataset_id=dataset_id,
        scanner_manifest_path=scanner_manifest_path,
        scanner_summary_path=scanner_summary_path,
        scanner_inspection_path=scanner_inspection_path,
        security_master_source_path=security_master_source_path,
        strategy_source_path=strategy_source_path,
    )
    for key, value in stable.items():
        if manifest.get(key) != value:
            raise contract.ChallengerCatalystContract3V2Error(
                f"source contract {key} drifted"
            )
    paths = contract.source_paths(
        source_manifest_path=source_manifest_path,
        scanner_manifest_path=scanner_manifest_path,
        scanner_summary_path=scanner_summary_path,
        scanner_inspection_path=scanner_inspection_path,
        security_master_source_path=security_master_source_path,
        strategy_source_path=strategy_source_path,
        selection_doc_path=selection_doc_path,
    )
    _verify_publication(manifest, paths=paths, require_published=require_published)
    capacity = manifest.get("capacity_contract")
    payload = manifest.get("dataset_payload")
    try:
        registered = datetime.fromisoformat(str(manifest.get("registered_at") or ""))
    except ValueError as exc:
        raise contract.ChallengerCatalystContract3V2Error(
            "source registration timestamp is malformed"
        ) from exc
    free_bytes = shutil.disk_usage(config.root).free
    if any(
        (
            registered.tzinfo is None,
            not isinstance(payload, Mapping),
            payload.get("lane") != "development" if isinstance(payload, Mapping) else True,
            payload.get("claim_scope") != "DEVELOPMENT_ONLY"
            if isinstance(payload, Mapping)
            else True,
            payload.get("status") != "COLLECTING"
            if isinstance(payload, Mapping)
            else True,
            payload.get("inspected") is not False
            if isinstance(payload, Mapping)
            else True,
            not isinstance(capacity, Mapping),
            capacity.get("capacity_ready") is not True
            if isinstance(capacity, Mapping)
            else True,
            capacity.get("historical_store_outside_repository") is not True
            if isinstance(capacity, Mapping)
            else True,
            capacity.get("historical_deletion_allowed") is not False
            if isinstance(capacity, Mapping)
            else True,
            int(capacity.get("reserve_bytes", -1)) != config.min_free_bytes
            if isinstance(capacity, Mapping)
            else True,
            free_bytes < config.min_free_bytes,
        )
    ):
        raise contract.ChallengerCatalystContract3V2Error(
            "source payload or capacity contract differs"
        )
    selection = stable["selection_contract"]
    result = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "status": "FROZEN_READY",
        "manifest_path": contract.base._repo_path(manifest_path),
        "manifest_sha256": manifest["manifest_sha256"],
        "requested_dates": selection["requested_date_count"],
        "selected_pair_count": selection["selected_pair_count"],
        "daily_shortlists_sha256": selection["daily_shortlists_sha256"],
        "private_selection_content_sha256": selection[
            "private_selection_content_sha256"
        ],
        "pre_freeze_target_artifact_count": 0,
        "capacity_ready": True,
        "primary_evidence_only": True,
        "substitutions_allowed": False,
        "target_sources_accessed": False,
        "selected_symbol_detail_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "source_rules_sha256": contract.base._sha256_json(stable["source_rules"]),
        "implementation_contract_sha256": contract.base._sha256_json(
            stable["implementation_contract"]
        ),
        "valid": True,
    }
    contract.base._write_json(status_path, result)
    return result


def default_manifest() -> Path:
    matches = sorted(contract.DEFAULT_OUTPUT_ROOT.glob(f"{contract.DATASET_ID}-*.json"))
    if len(matches) != 1:
        raise contract.ChallengerCatalystContract3V2Error(
            "exactly one third-tranche source manifest is required"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", nargs="?", type=Path)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--status", type=Path, default=contract.DEFAULT_PUBLIC_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_contract(
            manifest_path=args.manifest or default_manifest(),
            env_path=args.env,
            status_path=args.status,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        contract.ChallengerCatalystContract3V2Error,
        contract.base.DevelopmentCatalystContractError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        subprocess.SubprocessError,
        TypeError,
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
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
