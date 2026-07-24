"""Freeze the pre-outcome empty-series adapter revision of residual V6."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import strategy_discovery
from historical_store import canonical_json_bytes, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "two-to-three-day-cross-sectional-reversal-v6-disjoint-long-history"
)
PREDECESSOR = (
    ROOT
    / "family-contract/"
    "contract-fbb19be5110111ad975daf3866a0f4e3d82668fba7da87d1664128b346875f4c.json"
)


class ResidualReplicationFamilyRevisionError(RuntimeError):
    """The structural successor family contract cannot be frozen safely."""


def _timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResidualReplicationFamilyRevisionError(
            "created_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise ResidualReplicationFamilyRevisionError(
            "created_at needs a timezone"
        )


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise ResidualReplicationFamilyRevisionError(
            f"path escaped repository: {path}"
        ) from exc


def freeze_revision(
    *,
    created_at: str,
    root: Path = ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(created_at)
    if enforce_commit:
        strategy_discovery.require_committed(PREDECESSOR)
    predecessor = strategy_discovery._validate_family_contract(
        json.loads(PREDECESSOR.read_text(encoding="utf-8"))
    )
    revision = copy.deepcopy(predecessor)
    revision["experiment_id"] = (
        "experiment-two-to-three-day-cross-sectional-reversal-v6-"
        "disjoint-long-history-empty-series-normalized"
    )
    revision["parent_experiment_id"] = predecessor["experiment_id"]
    revision["created_at"] = created_at
    revision["status"] = "INVENTED"
    revision["successor_id"] = (
        "two-to-three-day-cross-sectional-reversal-v6-disjoint-long-history-"
        "empty-series-normalized"
    )
    revision["structural_predecessor"] = {
        "path": _repo_path(PREDECESSOR),
        "file_sha256": sha256_file(PREDECESSOR),
        "failure": "provider_empty_daily_series_rejected_by_runtime_schema",
        "trial_returns_computed": 0,
        "confirmation_prices_accessed": False,
        "parameters_changed": False,
        "dates_changed": False,
    }
    revision["input_normalization"] = {
        "stage": "after_manifest_hash_verification",
        "rule": "remove_only_empty_daily_series",
        "expected_development_empty_series": 35,
        "nonempty_rows_changed": 0,
    }
    revision["material_difference_rationale"] = (
        predecessor["material_difference_rationale"]
        + " This pre-outcome structural revision removes only provider-returned "
        "empty daily arrays after the frozen dataset hash is verified; all "
        "non-empty rows, identities, dates, trials, costs, and selection gates "
        "are byte-for-byte or semantically unchanged."
    )
    revision["contamination_risks"] = [
        *predecessor["contamination_risks"],
        (
            "The predecessor evaluation stopped at input-schema validation "
            "before any candidate or trial return was computed."
        ),
    ]
    revision["implementation_files"] = [
        *predecessor["implementation_files"],
        "residual_replication_normalized_plugin.py",
        "residual_replication_family_revision.py",
    ]
    revision["plugin"] = {
        **predecessor["plugin"],
        "module": "residual_replication_normalized_plugin",
    }
    revision.pop("implementation_hashes", None)
    revision.pop("rolling_origin_plan", None)
    revision.pop("trial_family", None)
    revision.pop("primary_trial_id", None)
    validated = strategy_discovery._validate_family_contract(revision)
    if (
        validated["parameter_grid"] != predecessor["parameter_grid"]
        or validated["development_dates"] != predecessor["development_dates"]
        or validated["confirmation_dates"] != predecessor["confirmation_dates"]
        or validated["winner_selection"] != predecessor["winner_selection"]
    ):
        raise ResidualReplicationFamilyRevisionError(
            "structural revision changed frozen research semantics"
        )
    digest = hashlib.sha256(canonical_json_bytes(validated)).hexdigest()
    path = root / "family-contract" / f"contract-{digest}.json"
    rendered = json.dumps(validated, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_text(encoding="utf-8") != rendered:
            raise ResidualReplicationFamilyRevisionError(
                "immutable revision contract drifted"
            )
    else:
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(rendered, encoding="utf-8")
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    return path, validated


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--created-at", required=True)
    parser.add_argument("--root", type=Path, default=ROOT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, contract = freeze_revision(
            created_at=args.created_at,
            root=args.root,
        )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "contract_sha256": hashlib.sha256(
                        canonical_json_bytes(contract)
                    ).hexdigest(),
                    "family_id": contract["family_id"],
                    "trial_count": len(contract["trial_family"]),
                    "trial_returns_accessed_before_freeze": 0,
                    "confirmation_prices_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ResidualReplicationFamilyRevisionError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
