"""Freeze the exact residual-reversal v7 temporal-expansion family contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import outcome_exposure
import residual_temporal_data as data
import strategy_discovery
from historical_store import canonical_json_bytes, sha256_file
from learning_data import load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
ROOT = data.ROOT
V6_FAMILY_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "two-to-three-day-cross-sectional-reversal-v6-disjoint-long-history/"
    "family-contract/contract-"
    "df35c971a11fac6314279e47a6d230866fc35324d20be2d01b562dfadc899d2b.json"
)
V6_DEVELOPMENT_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-equity-market-residual-reversal-replication/"
    "development-inspection/"
    "liquid-equity-market-residual-reversal-replication-development-inspection-"
    "5af65a86d919ed857c6d36adfc0df7e29b1ec0781b8099027595f5131aa275a9.json"
)


class ResidualTemporalDiscoveryError(RuntimeError):
    """The v7 family cannot be frozen from the inspected evidence graph."""


def _timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResidualTemporalDiscoveryError(
            "created_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise ResidualTemporalDiscoveryError(
            "created_at needs a timezone"
        )


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise ResidualTemporalDiscoveryError(
            f"path escaped repository: {path}"
        ) from exc


def _write_contract(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != rendered:
            raise ResidualTemporalDiscoveryError(
                "immutable v7 family contract drifted"
            )
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _single(root: Path, directory: str, pattern: str) -> Path:
    matches = sorted((root / directory).glob(pattern))
    if len(matches) != 1:
        raise ResidualTemporalDiscoveryError(
            f"expected exactly one {directory} artifact; found {len(matches)}"
        )
    return matches[0]


def freeze_family(
    *,
    created_at: str,
    root: Path = ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(created_at)
    inspection_path = _single(
        root, "development-data-inspection", "*.json"
    )
    manifest_path = _single(root, "development-dataset", "dataset-*.json")
    predecessors = (
        inspection_path,
        manifest_path,
        V6_FAMILY_CONTRACT,
        V6_DEVELOPMENT_INSPECTION,
    )
    if enforce_commit:
        for path in predecessors:
            strategy_discovery.require_committed(path)
    inspection = strategy_discovery.load_artifact(
        inspection_path, expected_kind=data.COLLECTION_INSPECTION_KIND
    )
    if not (
        inspection.get("state") == data.COLLECTION_INSPECTION_STATE
        and isinstance(inspection.get("checks"), Mapping)
        and all(inspection["checks"].values())
        and inspection.get("strategy_metrics_accessed") is False
        and inspection.get("confirmation_prices_accessed") is False
    ):
        raise ResidualTemporalDiscoveryError(
            "v7 development data is not independently inspected"
        )
    contract_path = PROJECT_ROOT / str(inspection["contract_path"])
    contract = data.load_contract(
        contract_path, enforce_commit=enforce_commit
    )
    manifest = load_frozen_dataset_contract(manifest_path)
    binding = manifest["dataset_payload"].get("dense_runtime")
    if not (
        isinstance(binding, Mapping)
        and binding.get("family_id") == data.FAMILY_ID
        and binding.get("formal_capacity")
        == len(contract["development_signal_dates"])
        and manifest.get("requested_dates") == contract["development_dates"]
        and manifest["dataset_payload"].get(
            "collection_inspection_sha256"
        )
        == inspection["artifact_sha256"]
    ):
        raise ResidualTemporalDiscoveryError(
            "v7 development dataset manifest drifted"
        )
    v6_family = json.loads(V6_FAMILY_CONTRACT.read_text(encoding="utf-8"))
    if not isinstance(v6_family, dict):
        raise ResidualTemporalDiscoveryError(
            "v6 family contract is malformed"
        )
    v6_inspection = strategy_discovery.load_artifact(
        V6_DEVELOPMENT_INSPECTION,
        expected_kind="development-search-inspection",
    )
    if not (
        v6_inspection.get("state") == "REJECTED"
        and v6_inspection.get("family_id")
        == "liquid-equity-market-residual-reversal-replication"
    ):
        raise ResidualTemporalDiscoveryError(
            "v6 adverse development disposition drifted"
        )
    records = outcome_exposure.read_index()
    fresh_scope, _exact_scope, exposure_scope = (
        data._rebuild_development_scopes(contract)
    )
    if outcome_exposure.find_overlaps(fresh_scope, records):
        raise ResidualTemporalDiscoveryError(
            "v7 fresh development scope is no longer untouched"
        )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    family = dict(v6_family)
    family.update(
        {
            "experiment_id": (
                "experiment-two-to-three-day-cross-sectional-reversal-v7-"
                "temporal-expansion"
            ),
            "family_id": data.FAMILY_ID,
            "strategy_id": data.MECHANISM_FAMILY,
            "parent_experiment_id": v6_family["experiment_id"],
            "created_at": created_at,
            "status": "INVENTED",
            "research_generation": "existing_family_temporal_expansion",
            "successor_id": data.SUCCESSOR_ID,
            "new_mechanism_family_slot_consumed": False,
            "predecessor": {
                "family_contract_path": _repo_path(V6_FAMILY_CONTRACT),
                "family_contract_file_sha256": sha256_file(
                    V6_FAMILY_CONTRACT
                ),
                "development_inspection_path": _repo_path(
                    V6_DEVELOPMENT_INSPECTION
                ),
                "development_inspection_file_sha256": sha256_file(
                    V6_DEVELOPMENT_INSPECTION
                ),
                "development_inspection_artifact_sha256": v6_inspection[
                    "artifact_sha256"
                ],
                "state": v6_inspection["state"],
                "evaluated_corpus_reused_as_contaminated_training": True,
                "outcome_guided_parameter_change": False,
            },
            "material_difference_rationale": (
                "This exact temporal expansion preserves the v6 mechanism and "
                "all 48 parameters, adds 200 deterministic and previously "
                "untouched 2021-2022 common-stock decision dates, explicitly "
                "labels the 200 v6 dates contaminated training, and retains "
                "the same untouched 93-date confirmation reserve. It targets "
                "v6 rolling-stability, DSR, PBO, and drawdown uncertainty by "
                "adding disjoint observations rather than repairing rules."
            ),
            "development_dates": list(contract["development_dates"]),
            "development_signal_dates": list(
                contract["development_signal_dates"]
            ),
            "fresh_development_signal_count": contract[
                "fresh_development_signal_count"
            ],
            "contaminated_training_signal_count": contract[
                "contaminated_training_signal_count"
            ],
            "embargo_dates": list(contract["embargo_dates"]),
            "confirmation_dates": list(contract["confirmation_dates"]),
            "confirmation_signal_dates": list(
                contract["confirmation_signal_dates"]
            ),
            "confirmation_signal_capacity": len(
                contract["confirmation_signal_dates"]
            ),
            "development_scope": dict(contract["development_scope"]),
            "fresh_development_scope_sha256": contract[
                "fresh_development_scope_sha256"
            ],
            "fresh_development_identity_pairs": contract[
                "fresh_development_identity_pairs"
            ],
            "exact_development_scope_sha256": contract[
                "exact_development_scope_sha256"
            ],
            "development_exposure_scope_is_conservative": (
                contract["development_scope"] == exposure_scope
            ),
            "contaminated_training_exposure": dict(
                contract["contaminated_training_exposure"]
            ),
            "confirmation_scope": dict(contract["confirmation_scope"]),
            "partitions": {
                "rolling_origin": True,
                "confirmation_untouched": True,
                "fresh_development_disjoint": True,
                "contaminated_training_explicit": True,
                "account_calendar_includes_zero_and_mark_to_market_days": True,
            },
            "contamination_risks": [
                "The 200 v6 development signals are explicitly contaminated "
                "training and cannot independently support confirmation.",
                "The 200 new early signals were untouched before the data "
                "contract and remain disjoint from all prior exact identities.",
                "The 93 v6 confirmation signal dates remain unopened and may "
                "be evaluated only for one frozen exact winner.",
            ],
            "implementation_files": [
                "residual_temporal_data.py",
                "residual_temporal_data_inspection.py",
                "residual_temporal_discovery.py",
                "residual_temporal_plugin.py",
                "residual_replication_plugin.py",
                "dense_strategy_runtime.py",
                "dense_strategy_plugin.py",
                "learning_statistics.py",
                "learning_experiment.py",
                "strategy_discovery.py",
                "outcome_exposure.py",
                "portfolio_maturity.py",
                "portfolio_config.toml",
            ],
            "plugin": {
                "module": "residual_temporal_plugin",
                "preflight": "preflight",
                "evaluate_development": "evaluate_development",
                "evaluate_confirmation": "evaluate_confirmation",
                "evaluate_production": "evaluate_production",
            },
            "capacity_manifest": _repo_path(manifest_path),
            "dataset_manifest": _repo_path(manifest_path),
            "data_contract_path": _repo_path(contract_path),
            "data_contract_sha256": contract["artifact_sha256"],
            "data_inspection_path": _repo_path(inspection_path),
            "data_inspection_sha256": inspection["artifact_sha256"],
            "input_normalization": {
                "stage": "after_manifest_hash_verification",
                "rule": "remove_only_empty_daily_series",
                "expected_development_empty_series": binding[
                    "expected_empty_series"
                ],
                "nonempty_rows_changed": 0,
            },
            "rules_or_parameter_grid_changed": False,
            "confirmation_access_permitted": False,
        }
    )
    validated = strategy_discovery._validate_family_contract(family)
    digest = hashlib.sha256(canonical_json_bytes(validated)).hexdigest()
    path = root / "family-contract" / f"contract-{digest}.json"
    _write_contract(path, validated)
    return path, validated


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, contract = freeze_family(
            created_at=args.created_at,
            root=args.root,
        )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "family_id": contract["family_id"],
                    "trial_count": len(contract["trial_family"]),
                    "development_signal_count": len(
                        contract["development_signal_dates"]
                    ),
                    "confirmation_signal_capacity": contract[
                        "confirmation_signal_capacity"
                    ],
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ResidualTemporalDiscoveryError,
        data.ResidualTemporalDataError,
        outcome_exposure.OutcomeExposureError,
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
