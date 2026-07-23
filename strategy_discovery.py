"""Manifest-driven, selection-aware strategy discovery for the active v2 campaign.

The controller never contacts a broker. Provider access is delegated only to a
frozen plugin contract after an inspected preflight opens that exact scope.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib
import json
import math
import re
import subprocess
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import portfolio_maturity
import next_week_discovery_batch
import outcome_exposure
from learning_experiment import (
    DEVELOPMENT_SEARCH_RULE,
    LearningExperimentError,
    build_rolling_origin_plan,
    select_development_winner,
    validate_complete_evaluation,
    validate_hypothesis_contract,
)
from learning_statistics import maximum_drawdown_fraction, stationary_bootstrap_summary
from learning_data import LearningDataError, load_frozen_dataset_contract
from historical_store import HistoricalStoreConfig, HistoricalStoreError


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
SCHEMA_VERSION = 1
MODULE_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
PROHIBITED_PREFLIGHT_KEYS = re.compile(
    r"(?:price|return|pnl|profit|drawdown|sharpe|outcome)", re.IGNORECASE
)
FULL_EVALUATION_NAMESPACE = Path(
    "_derived/strategy_discovery_evaluations"
)
FULL_EVALUATION_STORAGE = "LOCAL_HISTORICAL_DATA_ROOT"
TEST_EVALUATION_STORAGE = "DISCOVERY_ARTIFACT_ROOT_PARENT"


class StrategyDiscoveryError(RuntimeError):
    """A discovery artifact or transition is incomplete, stale, or unsafe."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StrategyDiscoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise StrategyDiscoveryError(f"{path} must contain an object")
    return value


def _write_artifact(
    payload: Mapping[str, Any], directory: Path, stem: str
) -> tuple[Path, dict[str, Any]]:
    content = dict(payload)
    content.pop("artifact_sha256", None)
    digest = _hash(content)
    artifact = {**content, "artifact_sha256": digest}
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stem}-{digest}.json"
    rendered = json.dumps(artifact, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise StrategyDiscoveryError("content-addressed artifact has other content")
    if not path.exists():
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)
    return path, artifact


def load_artifact(path: Path, *, expected_kind: str | None = None) -> dict[str, Any]:
    value = _read_object(path)
    supplied = value.get("artifact_sha256")
    content = {key: item for key, item in value.items() if key != "artifact_sha256"}
    expected = _hash(content)
    if supplied != expected or not path.name.endswith(f"-{expected}.json"):
        raise StrategyDiscoveryError(f"artifact was mutated or renamed: {path}")
    if expected_kind is not None and value.get("artifact_kind") != expected_kind:
        raise StrategyDiscoveryError(
            f"expected {expected_kind}, found {value.get('artifact_kind')}"
        )
    return value


def _evaluation_storage_base(
    *, root: Path, storage: str | None = None, production: bool = False
) -> tuple[Path, str]:
    if storage == TEST_EVALUATION_STORAGE or (
        storage is None and not production
    ):
        return root.parent / ".strategy-discovery-test-evidence", (
            TEST_EVALUATION_STORAGE
        )
    if storage not in {None, FULL_EVALUATION_STORAGE}:
        raise StrategyDiscoveryError("development evidence storage is invalid")
    try:
        configured = HistoricalStoreConfig.from_env()
    except HistoricalStoreError as exc:
        raise StrategyDiscoveryError(
            f"cannot open the external development evidence store: {exc}"
        ) from exc
    return configured.root, FULL_EVALUATION_STORAGE


def _external_evaluation_path(
    binding: Mapping[str, Any], *, root: Path
) -> Path:
    relative = binding.get("relative_path")
    if not isinstance(relative, str) or not relative:
        raise StrategyDiscoveryError(
            "external development evaluation path is missing"
        )
    relative_path = Path(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise StrategyDiscoveryError(
            "external development evaluation path is unsafe"
        )
    base, _storage = _evaluation_storage_base(
        root=root,
        storage=str(binding.get("storage")),
    )
    return base / relative_path


def _store_development_evaluation(
    evaluation: Mapping[str, Any],
    *,
    root: Path,
    production: bool,
) -> dict[str, Any]:
    normalized = dict(evaluation)
    content_sha256 = _hash(normalized)
    base, storage = _evaluation_storage_base(
        root=root,
        production=production,
    )
    relative = (
        FULL_EVALUATION_NAMESPACE
        / content_sha256
        / "evaluation.json.gz"
    )
    path = base / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temporary = path.with_suffix(".json.gz.tmp")
        with temporary.open("wb") as target:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=target,
                mtime=0,
            ) as compressed:
                compressed.write(_canonical(normalized))
        temporary.replace(path)
    binding = {
        "schema_version": 1,
        "kind": "external-development-evaluation",
        "storage": storage,
        "relative_path": str(relative),
        "format": "canonical-json",
        "compression": "gzip",
        "content_sha256": content_sha256,
        "file_sha256": _file_hash(path),
    }
    _load_development_evaluation(
        {"evaluation_binding": binding},
        root=root,
    )
    return binding


def _load_development_evaluation(
    result: Mapping[str, Any],
    *,
    root: Path,
) -> dict[str, Any]:
    embedded = result.get("evaluation")
    if isinstance(embedded, Mapping):
        return dict(embedded)
    binding = result.get("evaluation_binding")
    if not isinstance(binding, Mapping):
        raise StrategyDiscoveryError(
            "development result lacks full evaluation evidence"
        )
    if not (
        binding.get("schema_version") == 1
        and binding.get("kind") == "external-development-evaluation"
        and binding.get("format") == "canonical-json"
        and binding.get("compression") == "gzip"
        and isinstance(binding.get("content_sha256"), str)
        and SHA256_PATTERN.fullmatch(str(binding["content_sha256"]))
        and isinstance(binding.get("file_sha256"), str)
        and SHA256_PATTERN.fullmatch(str(binding["file_sha256"]))
    ):
        raise StrategyDiscoveryError(
            "external development evaluation binding is invalid"
        )
    path = _external_evaluation_path(binding, root=root)
    if not path.is_file():
        raise StrategyDiscoveryError(
            "external development evaluation is missing"
        )
    if _file_hash(path) != binding["file_sha256"]:
        raise StrategyDiscoveryError(
            "external development evaluation file hash drifted"
        )
    try:
        with gzip.open(path, "rb") as source:
            value = json.loads(source.read())
    except (OSError, json.JSONDecodeError) as exc:
        raise StrategyDiscoveryError(
            f"cannot read external development evaluation: {exc}"
        ) from exc
    if not isinstance(value, dict) or _hash(value) != binding["content_sha256"]:
        raise StrategyDiscoveryError(
            "external development evaluation content hash drifted"
        )
    return value


def _compact_development_evaluation(
    evaluation: Mapping[str, Any],
) -> dict[str, Any]:
    trial_summaries: list[dict[str, Any]] = []
    for trial in evaluation.get("trials", []):
        metrics = trial.get("metrics", {})
        arrays = {
            key: len(value)
            for key, value in metrics.items()
            if isinstance(value, list)
        }
        trial_summaries.append(
            {
                "trial_id": trial.get("trial_id"),
                "parameters": trial.get("parameters"),
                "metrics": {
                    key: value
                    for key, value in metrics.items()
                    if not isinstance(value, list)
                },
                "evidence_array_lengths": arrays,
                "trial_accounting_rows": len(
                    trial.get("trial_accounting", [])
                ),
                "maturity_rows": len(trial.get("maturity_rows", [])),
            }
        )
    return {
        "experiment_id": evaluation.get("experiment_id"),
        "dataset_manifest": evaluation.get("dataset_manifest"),
        "implementation_sha256": evaluation.get("implementation_sha256"),
        "trial_count": len(trial_summaries),
        "trials": trial_summaries,
    }


def _compact_selection(selection: Mapping[str, Any]) -> dict[str, Any]:
    compact = {
        key: value
        for key, value in selection.items()
        if key != "trial_classifications"
    }
    classifications: list[dict[str, Any]] = []
    for classification in selection.get("trial_classifications", []):
        rebuilt = classification.get("rebuilt_metrics", {})
        classifications.append(
            {
                **{
                    key: value
                    for key, value in classification.items()
                    if key != "rebuilt_metrics"
                },
                "rebuilt_metrics": {
                    key: value
                    for key, value in rebuilt.items()
                    if key
                    not in {
                        "oof_daily_account_returns",
                        "oof_filled_account_returns",
                        "oof_net_pnl_dollars",
                    }
                },
                "evidence_array_lengths": {
                    key: len(rebuilt.get(key, []))
                    for key in (
                        "oof_daily_account_returns",
                        "oof_filled_account_returns",
                        "oof_net_pnl_dollars",
                    )
                },
            }
        )
    compact["trial_classifications"] = classifications
    return compact


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise StrategyDiscoveryError("artifact path is outside the repository") from exc


def require_committed(path: Path) -> None:
    relative = _relative(path)
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    clean = subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", relative],
        cwd=PROJECT_ROOT,
    )
    if tracked.returncode != 0 or clean.returncode != 0:
        raise StrategyDiscoveryError(
            f"predecessor artifact must be committed and unchanged: {relative}"
        )


def _find_single(directory: Path, pattern: str, *, kind: str) -> tuple[Path, dict[str, Any]]:
    paths = sorted(directory.glob(pattern))
    if len(paths) != 1:
        raise StrategyDiscoveryError(
            f"expected exactly one {kind} artifact in {directory}; found {len(paths)}"
        )
    return paths[0], load_artifact(paths[0], expected_kind=kind)


def _find_contract_preflight(
    directory: Path,
    family_contract_path: Path,
) -> tuple[Path, dict[str, Any]]:
    expected_path = _relative(family_contract_path)
    expected_file_hash = _file_hash(family_contract_path)
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(directory.glob("*.json")):
        artifact = load_artifact(
            path, expected_kind="discovery-preflight-inspection"
        )
        if (
            artifact.get("family_contract_path") == expected_path
            and artifact.get("family_contract_sha256") == expected_file_hash
        ):
            matches.append((path, artifact))
    if len(matches) != 1:
        raise StrategyDiscoveryError(
            "expected exactly one discovery-preflight-inspection artifact "
            f"bound to {expected_path}; found {len(matches)}"
        )
    return matches[0]


def _validate_family_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    try:
        contract = validate_hypothesis_contract(value)
    except LearningExperimentError as exc:
        raise StrategyDiscoveryError(str(exc)) from exc
    if contract.get("campaign_id") != CAMPAIGN_ID:
        raise StrategyDiscoveryError("family contract is not bound to active v2")
    if contract.get("selection_mode") != "development_search":
        raise StrategyDiscoveryError("active discovery requires development_search")
    mechanism_family = contract.get("mechanism_family", contract.get("family_id"))
    if (
        not isinstance(mechanism_family, str)
        or not mechanism_family
        or not MODULE_PATTERN.fullmatch(mechanism_family.replace("-", "_"))
    ):
        raise StrategyDiscoveryError("mechanism_family is invalid")
    contract["mechanism_family"] = mechanism_family
    if contract.get("winner_selection") != DEVELOPMENT_SEARCH_RULE:
        raise StrategyDiscoveryError("winner-selection rule is not frozen")
    for field in (
        "development_dates",
        "embargo_dates",
        "confirmation_dates",
        "universe",
        "costs_bps_per_side",
        "partitions",
        "falsifiers",
        "implementation_files",
        "plugin",
        "capacity_policy",
    ):
        if not contract.get(field):
            raise StrategyDiscoveryError(f"family contract needs {field}")
    if contract["costs_bps_per_side"] != [5, 10, 20]:
        raise StrategyDiscoveryError("cost grid must remain [5, 10, 20]")
    development = _date_list(contract["development_dates"], "development_dates")
    embargo = _date_list(contract["embargo_dates"], "embargo_dates")
    confirmation = _date_list(contract["confirmation_dates"], "confirmation_dates")
    if len(embargo) < 5:
        raise StrategyDiscoveryError("confirmation embargo needs five sessions")
    if set(development) & set(embargo) or set(development) & set(confirmation) or set(embargo) & set(confirmation):
        raise StrategyDiscoveryError("development, embargo, and confirmation overlap")
    if not (max(development) < min(embargo) < min(confirmation)):
        raise StrategyDiscoveryError("evidence partitions are not chronological")
    if not (
        isinstance(contract["partitions"], Mapping)
        and contract["partitions"].get("rolling_origin") is True
        and contract["partitions"].get("confirmation_untouched") is True
    ):
        raise StrategyDiscoveryError(
            "development needs rolling-origin and untouched-confirmation partitions"
        )
    try:
        contract["rolling_origin_plan"] = build_rolling_origin_plan(
            contract["development_dates"]
        )
    except LearningExperimentError as exc:
        raise StrategyDiscoveryError(str(exc)) from exc
    plugin = contract["plugin"]
    if not isinstance(plugin, Mapping) or not MODULE_PATTERN.fullmatch(
        str(plugin.get("module", ""))
    ):
        raise StrategyDiscoveryError("plugin module is unsafe")
    for field in (
        "preflight",
        "evaluate_development",
        "evaluate_confirmation",
        "evaluate_production",
    ):
        if not isinstance(plugin.get(field), str) or not plugin[field].isidentifier():
            raise StrategyDiscoveryError(f"plugin.{field} is invalid")
    implementation_hashes: dict[str, str] = {}
    for raw_path in contract["implementation_files"]:
        path = (PROJECT_ROOT / str(raw_path)).resolve()
        try:
            path.relative_to(PROJECT_ROOT.resolve())
        except ValueError as exc:
            raise StrategyDiscoveryError(
                f"implementation file is outside the repository: {raw_path}"
            ) from exc
        if not path.is_file():
            raise StrategyDiscoveryError(f"implementation file is missing: {raw_path}")
        implementation_hashes[str(raw_path)] = _file_hash(path)
    contract["implementation_hashes"] = implementation_hashes
    return contract


def _assert_implementation_current(
    value: Mapping[str, Any],
    *,
    enforce_commit: bool,
) -> None:
    hashes = value.get("implementation_hashes")
    if not isinstance(hashes, Mapping) or not hashes:
        raise StrategyDiscoveryError("frozen implementation hashes are missing")
    for raw_path, expected in hashes.items():
        path = (PROJECT_ROOT / str(raw_path)).resolve()
        try:
            path.relative_to(PROJECT_ROOT.resolve())
        except ValueError as exc:
            raise StrategyDiscoveryError(
                f"frozen implementation path is unsafe: {raw_path}"
            ) from exc
        if (
            not isinstance(expected, str)
            or not SHA256_PATTERN.fullmatch(expected)
            or not path.is_file()
            or _file_hash(path) != expected
        ):
            raise StrategyDiscoveryError(
                f"frozen implementation drifted: {raw_path}"
            )
        if enforce_commit:
            require_committed(path)


def _development_evidence_dates(contract: Mapping[str, Any]) -> list[str]:
    plan = contract.get("rolling_origin_plan")
    if not isinstance(plan, list) or not plan:
        raise StrategyDiscoveryError("development rolling-origin plan is missing")
    dates: list[str] = []
    for fold in plan:
        if not isinstance(fold, Mapping) or not isinstance(
            fold.get("test_dates"), list
        ):
            raise StrategyDiscoveryError(
                "development rolling-origin fold is invalid"
            )
        dates.extend(fold["test_dates"])
    if (
        not dates
        or dates != sorted(dates)
        or len(dates) != len(set(dates))
        or not set(dates).issubset(contract["development_dates"])
    ):
        raise StrategyDiscoveryError("development OOF evidence dates are invalid")
    return dates


def _date_list(value: Any, field: str) -> list[date]:
    if not isinstance(value, list) or not value:
        raise StrategyDiscoveryError(f"{field} must be a non-empty array")
    try:
        parsed = [date.fromisoformat(str(item)) for item in value]
    except ValueError as exc:
        raise StrategyDiscoveryError(f"{field} must contain ISO dates") from exc
    if parsed != sorted(parsed) or len(parsed) != len(set(parsed)):
        raise StrategyDiscoveryError(f"{field} must be unique and chronological")
    return parsed


def _load_plugin(contract: Mapping[str, Any], function_field: str) -> Callable[..., Any]:
    plugin = contract["plugin"]
    try:
        module = importlib.import_module(str(plugin["module"]))
        function = getattr(module, str(plugin[function_field]))
    except (ImportError, AttributeError, KeyError) as exc:
        raise StrategyDiscoveryError(
            f"cannot load plugin function {function_field}"
        ) from exc
    if not callable(function):
        raise StrategyDiscoveryError(f"plugin {function_field} is not callable")
    return function


def _assert_no_preflight_outcomes(value: Any, path: str = "preflight") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if PROHIBITED_PREFLIGHT_KEYS.search(str(key)):
                raise StrategyDiscoveryError(
                    f"outcome-like preflight field is forbidden: {path}.{key}"
                )
            _assert_no_preflight_outcomes(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_no_preflight_outcomes(item, f"{path}[{index}]")


def run_preflight(
    family_contract_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        require_committed(family_contract_path)
    raw = _read_object(family_contract_path)
    contract = _validate_family_contract(raw)
    _assert_implementation_current(contract, enforce_commit=enforce_commit)
    function = _load_plugin(contract, "preflight")
    started = time.monotonic()
    result = function(contract)
    if not isinstance(result, Mapping):
        raise StrategyDiscoveryError("preflight plugin must return an object")
    _assert_no_preflight_outcomes(result)
    eligible = result.get("verified_capacity")
    if isinstance(eligible, bool) or not isinstance(eligible, int) or eligible < 0:
        raise StrategyDiscoveryError("preflight verified_capacity must be nonnegative")
    if result.get("point_in_time_complete") is not True:
        raise StrategyDiscoveryError("preflight point-in-time inputs are incomplete")
    policy = contract["capacity_policy"]
    retire_below = int(policy["retire_below"])
    fast_lane_at = int(policy["fast_lane_at"])
    if eligible < retire_below:
        state = "RETIRED_INSUFFICIENT_FORMAL_CAPACITY"
    elif eligible < fast_lane_at:
        state = "PRESERVED_LATER_SINGLE_RULE"
    else:
        state = "CAPACITY_READY"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "discovery-preflight-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": contract["family_id"],
        "family_contract_path": _relative(family_contract_path),
        "family_contract_sha256": _file_hash(family_contract_path),
        "validated_contract_sha256": _hash(contract),
        "implementation_sha256": _hash(contract["implementation_hashes"]),
        "state": state,
        "verified_capacity": eligible,
        "capacity_policy": dict(policy),
        "point_in_time_complete": True,
        "outcomes_accessed": False,
        "provider_telemetry": dict(result.get("provider_telemetry", {})),
        "preflight_details": dict(result),
        "inspection": {
            "family_identity_rebuilt": True,
            "capacity_threshold_rebuilt": True,
            "point_in_time_boundary_rebuilt": True,
            "zero_outcome_boundary_rebuilt": True,
            "valid": True,
        },
        "elapsed_seconds": time.monotonic() - started,
    }
    return _write_artifact(
        payload,
        root / str(contract["family_id"]) / "preflight",
        f"{contract['family_id']}-preflight",
    )


def freeze_search(
    family_contract_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        require_committed(family_contract_path)
    contract = _validate_family_contract(_read_object(family_contract_path))
    _assert_implementation_current(contract, enforce_commit=enforce_commit)
    preflight_path, preflight = _find_contract_preflight(
        root / str(contract["family_id"]) / "preflight",
        family_contract_path,
    )
    if enforce_commit:
        require_committed(preflight_path)
    if preflight["state"] != "CAPACITY_READY":
        raise StrategyDiscoveryError(
            f"family is not in the first-pilot fast lane: {preflight['state']}"
        )
    if preflight["family_contract_sha256"] != _file_hash(family_contract_path):
        raise StrategyDiscoveryError("family contract drifted after preflight")
    if preflight.get("family_contract_path") != _relative(family_contract_path):
        raise StrategyDiscoveryError("family contract path drifted after preflight")
    if preflight.get("validated_contract_sha256") != _hash(contract):
        raise StrategyDiscoveryError(
            "validated family contract or implementation drifted after preflight"
        )
    if preflight.get("implementation_sha256") != _hash(
        contract["implementation_hashes"]
    ):
        raise StrategyDiscoveryError("preflight implementation binding drifted")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "frozen-development-search",
        "campaign_id": CAMPAIGN_ID,
        "state": "SEARCH_FROZEN",
        "family_contract": contract,
        "preflight_path": _relative(preflight_path),
        "preflight_sha256": preflight["artifact_sha256"],
        "trial_count": len(contract["trial_family"]),
        "outcomes_accessed": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
    }
    return _write_artifact(
        payload,
        root / str(contract["family_id"]) / "search",
        f"{contract['family_id']}-search",
    )


def evaluate_development(
    search_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        require_committed(search_path)
    search = load_artifact(search_path, expected_kind="frozen-development-search")
    if search["state"] != "SEARCH_FROZEN":
        raise StrategyDiscoveryError("development search is not frozen")
    contract = search["family_contract"]
    _assert_implementation_current(contract, enforce_commit=enforce_commit)
    function = _load_plugin(contract, "evaluate_development")
    started = time.monotonic()
    plugin_contract = {
        **contract,
        "development_search_sha256": search["artifact_sha256"],
    }
    result = function(plugin_contract, list(contract["trial_family"]))
    if not isinstance(result, Mapping):
        raise StrategyDiscoveryError("development plugin must return an object")
    if contract.get("dataset_manifest") and result.get(
        "dataset_manifest"
    ) != contract["dataset_manifest"]:
        raise StrategyDiscoveryError("development dataset manifest was substituted")
    development_manifest = result.get("dataset_manifest")
    if not isinstance(development_manifest, str) or not development_manifest:
        raise StrategyDiscoveryError("development dataset manifest is missing")
    development_manifest_path = Path(development_manifest)
    if enforce_commit:
        require_committed(development_manifest_path)
    try:
        development_dataset = load_frozen_dataset_contract(
            development_manifest_path
        )
    except (LearningDataError, OSError) as exc:
        raise StrategyDiscoveryError(
            f"development dataset manifest is invalid: {exc}"
        ) from exc
    development_payload = development_dataset["dataset_payload"]
    if not (
        development_dataset["requested_dates"] == contract["development_dates"]
        and development_payload.get("lane") == "development"
        and (
            contract.get("dataset_manifest")
            or development_payload.get("development_search_sha256")
            == search["artifact_sha256"]
        )
    ):
        raise StrategyDiscoveryError(
            "development dataset is not bound to the frozen search"
        )
    evidence_dates = _development_evidence_dates(contract)
    evaluation = {
        "experiment_id": contract["experiment_id"],
        "dataset_manifest": str(result.get("dataset_manifest")),
        "implementation_sha256": _hash(contract["implementation_hashes"]),
        "trials": result.get("trials"),
    }
    try:
        validate_complete_evaluation(contract, evaluation)
    except LearningExperimentError as exc:
        raise StrategyDiscoveryError(str(exc)) from exc
    for trial in result.get("trials", []):
        rows = trial.get("maturity_rows")
        if not isinstance(rows, list) or [
            row.get("date") if isinstance(row, Mapping) else None for row in rows
        ] != evidence_dates:
            raise StrategyDiscoveryError(
                "development maturity rows must match every frozen OOF test date"
            )
        maturity_accounting = _maturity_rows_accounting(
            rows,
            expected_dates=evidence_dates,
            phase="development",
        )["stress_20bps"]
        metrics = trial["metrics"]
        for field in (
            "oof_daily_account_returns",
            "oof_filled_account_returns",
            "oof_net_pnl_dollars",
        ):
            if not isinstance(metrics.get(field), list):
                raise StrategyDiscoveryError(
                    f"development {field} must be a complete array"
                )
        if any(
            [float(value) for value in metrics[metric_field]]
            != maturity_accounting[accounting_field]
            for metric_field, accounting_field in (
                ("oof_daily_account_returns", "daily_account_returns"),
                ("oof_filled_account_returns", "filled_account_returns"),
                ("oof_net_pnl_dollars", "net_pnl_dollars"),
            )
        ):
            raise StrategyDiscoveryError(
                "development account evidence differs from maturity rows"
            )
        accounting_dates = [
            row.get("date") if isinstance(row, Mapping) else None
            for row in trial["trial_accounting"]
        ]
        if accounting_dates != evidence_dates:
            raise StrategyDiscoveryError(
                "development trial accounting must match every frozen OOF test date"
            )
    telemetry = result.get("provider_telemetry", {})
    if not isinstance(telemetry, Mapping):
        raise StrategyDiscoveryError("provider telemetry must be an object")
    evaluation_binding = _store_development_evaluation(
        evaluation,
        root=root,
        production=enforce_commit,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "development-search-result",
        "campaign_id": CAMPAIGN_ID,
        "family_id": contract["family_id"],
        "state": "DEVELOPMENT_EVALUATED",
        "search_path": _relative(search_path),
        "search_sha256": search["artifact_sha256"],
        "evaluation_binding": evaluation_binding,
        "evaluation_summary": _compact_development_evaluation(evaluation),
        "provider_telemetry": dict(telemetry),
        "elapsed_seconds": time.monotonic() - started,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
    }
    return _write_artifact(
        payload,
        root / str(contract["family_id"]) / "development",
        f"{contract['family_id']}-development",
    )


def inspect_development(
    result_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        require_committed(result_path)
    result = load_artifact(result_path, expected_kind="development-search-result")
    search_path = PROJECT_ROOT / str(result["search_path"])
    if enforce_commit:
        require_committed(search_path)
    search = load_artifact(search_path, expected_kind="frozen-development-search")
    if search["artifact_sha256"] != result["search_sha256"]:
        raise StrategyDiscoveryError("development result search binding drifted")
    contract = search["family_contract"]
    evaluation = _load_development_evaluation(result, root=root)
    development_manifest_path = Path(
        str(evaluation["dataset_manifest"])
    )
    if enforce_commit:
        require_committed(development_manifest_path)
    evidence_dates = _development_evidence_dates(contract)
    try:
        selection = select_development_winner(contract, evaluation)
    except LearningExperimentError as exc:
        raise StrategyDiscoveryError(str(exc)) from exc
    state = selection["status"]
    development_account_inspection: dict[str, Any] | None = None
    if state == "WINNER_SELECTED":
        selected_trial_id = selection["selected_trial_id"]
        selected_trials = [
            trial
            for trial in evaluation["trials"]
            if trial["trial_id"] == selected_trial_id
        ]
        if len(selected_trials) != 1:
            raise StrategyDiscoveryError(
                "selected development trial is absent or ambiguous"
            )
        selected_trial = selected_trials[0]
        account_scenarios = _maturity_rows_accounting(
            selected_trial.get("maturity_rows"),
            expected_dates=evidence_dates,
            phase="development",
        )
        development_account_inspection = inspect_confirmation_metrics(
            {
                "scenarios": account_scenarios,
                "maturity_rows": selected_trial["maturity_rows"],
                "rule_violations": [],
                "capture_complete": True,
            },
            required_signals=max(
                1,
                int(selection["required_total_signals"])
                - int(selection["required_confirmation_signals"]),
            ),
            expected_dates=evidence_dates,
            phase="development",
        )
        if not development_account_inspection["passed"]:
            state = "RETIRED_DEVELOPMENT_ACCOUNT_GATES"
    confirmation_inventory = len(contract["confirmation_dates"])
    maximum_total_signal_capacity = (
        int(selection.get("development_filled_signals", 0))
        + confirmation_inventory
    )
    if selection["status"] == "WINNER_SELECTED" and (
        confirmation_inventory < int(selection["required_confirmation_signals"])
        or maximum_total_signal_capacity < int(selection["required_total_signals"])
    ):
        state = "INSUFFICIENT_POWER_CAPACITY"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "development-search-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": contract["family_id"],
        "state": state,
        "result_path": _relative(result_path),
        "result_sha256": result["artifact_sha256"],
        "selection": _compact_selection(selection),
        "confirmation_inventory": confirmation_inventory,
        "maximum_total_signal_capacity": maximum_total_signal_capacity,
        "development_account_inspection": development_account_inspection,
        "evidence_counts_frozen_before_confirmation": state == "WINNER_SELECTED",
        "confirmation_access_permitted": state == "WINNER_SELECTED",
        "inspection": {
            "complete_trial_family_rebuilt": True,
            "trial_accounting_rebuilt": True,
            "winner_selection_rebuilt": True,
            "neighbor_stability_rebuilt": True,
            "multiple_testing_rebuilt": True,
            "power_target_rebuilt": True,
            "valid": True,
        },
        "broker_actions_permitted": False,
    }
    inspection_path, inspection_artifact = _write_artifact(
        payload,
        root / str(contract["family_id"]) / "development-inspection",
        f"{contract['family_id']}-development-inspection",
    )
    if enforce_commit and contract.get("development_scope") is not None:
        outcome_exposure.ensure_record(
            outcome_exposure.build_record(
                exposure_id=(
                    f"development-{contract['family_id']}-"
                    f"{result['artifact_sha256'][:16]}"
                ),
                campaign_id=CAMPAIGN_ID,
                lane="development",
                recorded_at=contract["created_at"],
                source_path=_relative(result_path),
                source_sha256=_file_hash(result_path),
                scope=contract["development_scope"],
            )
        )
    return inspection_path, inspection_artifact


def freeze_winner(
    inspection_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
    recorded_at: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        require_committed(inspection_path)
    inspection = load_artifact(
        inspection_path, expected_kind="development-search-inspection"
    )
    if inspection["state"] != "WINNER_SELECTED":
        raise StrategyDiscoveryError(f"development has no winner: {inspection['state']}")
    result_path = PROJECT_ROOT / str(inspection["result_path"])
    if enforce_commit:
        require_committed(result_path)
    result = load_artifact(
        result_path,
        expected_kind="development-search-result",
    )
    if result["artifact_sha256"] != inspection.get("result_sha256"):
        raise StrategyDiscoveryError("winner development result binding drifted")
    search_path = PROJECT_ROOT / str(result["search_path"])
    if enforce_commit:
        require_committed(search_path)
    search = load_artifact(
        search_path,
        expected_kind="frozen-development-search",
    )
    if search["artifact_sha256"] != result.get("search_sha256"):
        raise StrategyDiscoveryError("winner development search binding drifted")
    evaluation = _load_development_evaluation(result, root=root)
    development_manifest_path = Path(
        str(evaluation["dataset_manifest"])
    )
    if enforce_commit:
        require_committed(development_manifest_path)
    contract = search["family_contract"]
    selection = inspection["selection"]
    exact_rules = {
        "family_id": contract["family_id"],
        "mechanism_family": contract["mechanism_family"],
        "selected_trial_id": selection["selected_trial_id"],
        "parameters": selection["selected_parameters"],
        "entry_rule": contract["entry_rule"],
        "stop_rule": contract["stop_rule"],
        "exit_rule": contract["exit_rule"],
        "ranking_rule": contract["ranking_rule"],
        "selection_rule": contract["selection_rule"],
        "universe": contract["universe"],
        "execution_assumptions": contract["execution_assumptions"],
        "costs_bps_per_side": contract["costs_bps_per_side"],
    }
    rules_hash = _hash(exact_rules)
    version = f"{contract['family_id']}-{rules_hash[:12]}"
    winner_recorded_at = recorded_at or datetime.now(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    try:
        winner_timestamp = datetime.fromisoformat(
            winner_recorded_at.replace("Z", "+00:00")
        )
        contract_timestamp = datetime.fromisoformat(
            str(contract["created_at"]).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise StrategyDiscoveryError("winner freeze timestamp is invalid") from exc
    if (
        winner_timestamp.tzinfo is None
        or contract_timestamp.tzinfo is None
        or winner_timestamp <= contract_timestamp
    ):
        raise StrategyDiscoveryError(
            "winner freeze must follow family-contract creation"
        )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "frozen-strategy-winner",
        "campaign_id": CAMPAIGN_ID,
        "family_id": contract["family_id"],
        "mechanism_family": contract["mechanism_family"],
        "strategy_id": contract["strategy_id"],
        "strategy_version": version,
        "rules_hash": rules_hash,
        "state": "WINNER_FROZEN",
        "recorded_at": winner_recorded_at,
        "family_contract_created_at": contract["created_at"],
        "trial_count": len(contract["trial_family"]),
        "development_inspection_path": _relative(inspection_path),
        "development_inspection_sha256": inspection["artifact_sha256"],
        "exact_rules": exact_rules,
        "power_target": selection["power_target"],
        "required_total_signals": selection["required_total_signals"],
        "required_confirmation_signals": selection[
            "required_confirmation_signals"
        ],
        "development_dates": contract["development_dates"],
        "development_evidence_dates": _development_evidence_dates(contract),
        "rolling_origin_plan": contract["rolling_origin_plan"],
        "embargo_dates": contract["embargo_dates"],
        "confirmation_dates": contract["confirmation_dates"],
        "development_scope": contract.get("development_scope"),
        "confirmation_scope": contract.get("confirmation_scope"),
        "plugin": contract["plugin"],
        "implementation_hashes": contract["implementation_hashes"],
        "development_dataset_manifest": evaluation["dataset_manifest"],
        "confirmation_parameter_alternatives": 0,
        "confirmation_access_permitted": True,
        "broker_actions_permitted": False,
    }
    return _write_artifact(
        payload,
        root / str(contract["family_id"]) / "winner",
        f"{contract['family_id']}-winner",
    )


def evaluate_confirmation(
    winner_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        require_committed(winner_path)
    winner = load_artifact(winner_path, expected_kind="frozen-strategy-winner")
    if winner["state"] != "WINNER_FROZEN":
        raise StrategyDiscoveryError("winner is not frozen")
    _assert_implementation_current(winner, enforce_commit=enforce_commit)
    confirmation_scope = winner.get("confirmation_scope")
    if confirmation_scope is not None:
        try:
            outcome_exposure.assert_untouched(
                confirmation_scope,
                outcome_exposure.read_index(),
            )
        except outcome_exposure.OutcomeExposureError as exc:
            raise StrategyDiscoveryError(str(exc)) from exc
    function = _load_plugin({"plugin": winner["plugin"]}, "evaluate_confirmation")
    started = time.monotonic()
    result = function(winner)
    if not isinstance(result, Mapping):
        raise StrategyDiscoveryError("confirmation plugin must return an object")
    if result.get("rules_hash") != winner["rules_hash"]:
        raise StrategyDiscoveryError("confirmation rules hash drifted")
    if result.get("parameter_alternatives", 0) != 0:
        raise StrategyDiscoveryError("confirmation evaluated parameter alternatives")
    observed_dates = result.get("observed_dates")
    if observed_dates != winner["confirmation_dates"]:
        raise StrategyDiscoveryError("confirmation dates were substituted or omitted")
    if result.get("outcome_access_before_winner_freeze") is not False:
        raise StrategyDiscoveryError("confirmation outcome-access attestation is missing")
    confirmation_manifest = result.get("dataset_manifest")
    if not isinstance(confirmation_manifest, str) or not confirmation_manifest:
        raise StrategyDiscoveryError("confirmation dataset manifest is missing")
    confirmation_manifest_path = Path(confirmation_manifest)
    if enforce_commit:
        require_committed(confirmation_manifest_path)
    try:
        confirmation_dataset = load_frozen_dataset_contract(
            confirmation_manifest_path
        )
    except (LearningDataError, OSError) as exc:
        raise StrategyDiscoveryError(
            f"confirmation dataset manifest is invalid: {exc}"
        ) from exc
    confirmation_payload = confirmation_dataset["dataset_payload"]
    if not (
        confirmation_dataset["requested_dates"] == winner["confirmation_dates"]
        and confirmation_payload.get("lane") == "confirmation"
        and confirmation_payload.get("claim_scope")
        == "EXACT_PREREGISTERED_CONTRACT_ONLY"
        and confirmation_payload.get("preregistration_sha256")
        == winner["rules_hash"]
        and confirmation_payload.get("capture_after_preregistration_attested")
        is True
    ):
        raise StrategyDiscoveryError(
            "confirmation dataset is not exact, untouched, and winner-bound"
        )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "confirmation-result",
        "campaign_id": CAMPAIGN_ID,
        "family_id": winner["family_id"],
        "state": "CONFIRMATION_EVALUATED",
        "winner_path": _relative(winner_path),
        "winner_sha256": winner["artifact_sha256"],
        "result": dict(result),
        "elapsed_seconds": time.monotonic() - started,
        "broker_actions_permitted": False,
    }
    return _write_artifact(
        payload,
        root / str(winner["family_id"]) / "confirmation",
        f"{winner['family_id']}-confirmation",
    )


def _profit_factor(values: Sequence[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if losses == 0:
        return math.inf if gains > 0 else None
    return gains / losses


def _maturity_rows_accounting(
    rows: Any, *, expected_dates: Sequence[str], phase: str
) -> dict[str, dict[str, list[float]]]:
    if not isinstance(rows, list) or [
        row.get("date") if isinstance(row, Mapping) else None for row in rows
    ] != list(expected_dates):
        raise StrategyDiscoveryError(
            f"{phase} maturity rows must match every frozen date"
        )
    fields = {
        "primary_5bps": (
            "primary_account_return_fraction",
            "net_pnl_dollars",
        ),
        "stress_10bps": (
            "stress_10bps_account_return_fraction",
            "stress_10bps_net_pnl_dollars",
        ),
        "stress_20bps": (
            "stress_20bps_account_return_fraction",
            "stress_20bps_net_pnl_dollars",
        ),
    }
    rebuilt = {
        name: {
            "daily_account_returns": [],
            "filled_account_returns": [],
            "net_pnl_dollars": [],
        }
        for name in fields
    }

    def number(value: Any, label: str, *, account_return: bool = False) -> float:
        try:
            parsed = float(value)
        except (TypeError, ValueError) as exc:
            raise StrategyDiscoveryError(
                f"{phase} maturity {label} must be numeric"
            ) from exc
        if not math.isfinite(parsed) or (account_return and parsed <= -1):
            raise StrategyDiscoveryError(
                f"{phase} maturity {label} is outside its valid range"
            )
        return parsed

    for row in rows:
        outcome = row.get("session_outcome")
        raw_signals = row.get("signals")
        if raw_signals is None:
            signals = [row] if outcome == "filled" else []
        elif isinstance(raw_signals, list) and all(
            isinstance(signal, Mapping) for signal in raw_signals
        ):
            signals = raw_signals
        else:
            raise StrategyDiscoveryError(
                f"{phase} maturity-row signals are invalid"
            )
        for name, (return_field, dollar_field) in fields.items():
            rebuilt[name]["daily_account_returns"].append(
                number(
                    row.get(return_field),
                    f"row {return_field}",
                    account_return=True,
                )
            )
            for signal in signals:
                rebuilt[name]["filled_account_returns"].append(
                    number(
                        signal.get(return_field),
                        f"signal {return_field}",
                        account_return=True,
                    )
                )
                rebuilt[name]["net_pnl_dollars"].append(
                    number(signal.get(dollar_field), f"signal {dollar_field}")
                )
    return rebuilt


def inspect_confirmation_metrics(
    result: Mapping[str, Any],
    *,
    required_signals: int,
    expected_dates: Sequence[str],
    phase: str = "confirmation",
) -> dict[str, Any]:
    scenarios = result.get("scenarios")
    if not isinstance(scenarios, Mapping) or set(scenarios) != {
        "primary_5bps",
        "stress_10bps",
        "stress_20bps",
    }:
        raise StrategyDiscoveryError(f"{phase} needs exact 5/10/20 bps scenarios")
    maturity_accounting = _maturity_rows_accounting(
        result.get("maturity_rows"),
        expected_dates=expected_dates,
        phase=phase,
    )
    rebuilt: dict[str, Any] = {}
    gates: dict[str, bool] = {}
    for name, minimum_factor in (
        ("primary_5bps", 1.30),
        ("stress_10bps", 1.20),
        ("stress_20bps", 1.20),
    ):
        scenario = scenarios[name]
        if not isinstance(scenario, Mapping):
            raise StrategyDiscoveryError(f"{name} {phase} scenario is invalid")
        try:
            daily = [
                float(value) for value in scenario.get("daily_account_returns", [])
            ]
            filled = [
                float(value) for value in scenario.get("filled_account_returns", [])
            ]
            dollars = [float(value) for value in scenario.get("net_pnl_dollars", [])]
        except (TypeError, ValueError) as exc:
            raise StrategyDiscoveryError(
                f"{name} {phase} accounting must be numeric"
            ) from exc
        if (
            len(daily) != len(expected_dates)
            or len(filled) != len(dollars)
            or any(not math.isfinite(value) for value in [*daily, *filled, *dollars])
            or any(value <= -1 for value in [*daily, *filled])
        ):
            raise StrategyDiscoveryError(f"{name} {phase} accounting is incomplete")
        if any(
            values != maturity_accounting[name][field]
            for field, values in (
                ("daily_account_returns", daily),
                ("filled_account_returns", filled),
                ("net_pnl_dollars", dollars),
            )
        ):
            raise StrategyDiscoveryError(
                f"{name} {phase} accounting differs from maturity rows"
            )
        midpoint = len(daily) // 2
        without_best = sorted(filled, reverse=True)[5:]
        profit_factor = _profit_factor(dollars)
        infinite_profit_factor = profit_factor == math.inf
        metrics = {
            "signals": len(filled),
            "total_log_growth": sum(math.log1p(value) for value in daily),
            "compounded_return_fraction": math.prod(1 + value for value in daily) - 1,
            "profit_factor": None if infinite_profit_factor else profit_factor,
            "profit_factor_is_infinite": infinite_profit_factor,
            "maximum_drawdown_fraction": maximum_drawdown_fraction(daily),
            "first_half_log_growth": sum(
                math.log1p(value) for value in daily[:midpoint]
            )
            if midpoint
            else None,
            "second_half_log_growth": sum(
                math.log1p(value) for value in daily[midpoint:]
            )
            if midpoint
            else None,
            "without_five_best_log_growth": sum(
                math.log1p(value) for value in without_best
            )
            if without_best
            else None,
            "stationary_bootstrap": stationary_bootstrap_summary(
                filled, confidence=0.90, samples=5_000
            ),
        }
        rebuilt[name] = metrics
        prefix = name.replace("_", "-")
        gates[f"{prefix}-positive-log-growth"] = metrics["total_log_growth"] > 0
        gates[f"{prefix}-positive-compounding"] = (
            metrics["compounded_return_fraction"] > 0
        )
        gates[f"{prefix}-profit-factor"] = (
            metrics["profit_factor_is_infinite"]
            or (
                metrics["profit_factor"] is not None
                and metrics["profit_factor"] >= minimum_factor
            )
        )
        gates[f"{prefix}-drawdown"] = metrics["maximum_drawdown_fraction"] <= 0.03
        gates[f"{prefix}-chronological-halves"] = (
            metrics["first_half_log_growth"] is not None
            and metrics["first_half_log_growth"] > 0
            and metrics["second_half_log_growth"] is not None
            and metrics["second_half_log_growth"] > 0
        )
        gates[f"{prefix}-without-five-best"] = (
            metrics["without_five_best_log_growth"] is not None
            and metrics["without_five_best_log_growth"] > 0
        )
    primary = rebuilt["primary_5bps"]
    gates["required-confirmation-signals"] = primary["signals"] >= required_signals
    gates["primary-stationary-bootstrap"] = (
        primary["stationary_bootstrap"]["lower_one_sided"] > 0
    )
    gates["rule-completeness"] = result.get("rule_violations") == []
    gates["capture-completeness"] = result.get("capture_complete") is True
    return {
        "metrics": rebuilt,
        "gates": gates,
        "accounting": {
            "calendar_sessions": len(expected_dates),
            "closed_signals": primary["signals"],
            "maturity_rows_reconciled": True,
            "cost_scenarios_reconciled": True,
        },
        "passed": all(gates.values()),
    }


def _phase_maturity_records(
    rows: Any,
    *,
    phase: str,
    expected_dates: Sequence[str],
    winner: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or [item.get("date") for item in rows] != list(
        expected_dates
    ):
        raise StrategyDiscoveryError(
            f"{phase} maturity rows must match the frozen dates exactly"
        )
    records: list[dict[str, Any]] = []
    for row in rows:
        day = str(row["date"])
        outcome = str(row.get("session_outcome"))
        if outcome not in {
            "filled",
            "no_signal",
            "rejected",
            "missed_fill",
            "capital_blocked",
            "position_open",
            "exit",
            "mixed",
        }:
            raise StrategyDiscoveryError(f"{phase} maturity row outcome is invalid")
        common = {
            "schema_version": portfolio_maturity.SCHEMA_VERSION,
            "research_campaign_id": CAMPAIGN_ID,
            "recorded_at": winner["recorded_at"],
            "strategy_id": winner["strategy_id"],
            "strategy_version": winner["strategy_version"],
            "mechanism_family": winner.get(
                "mechanism_family", winner["family_id"]
            ),
            "rules_hash": winner["rules_hash"],
            "date": day,
            "sample_phase": phase,
            "mode": "historical",
            "session_capture_complete": True,
            "rule_violations": [],
        }
        records.append(
            {
                **common,
                "record_type": "session",
                "session_id": (
                    f"{day}-{winner['strategy_id']}-{phase}-session"
                ),
                "eligible_signal": bool(
                    row.get("eligible_signal", outcome == "filled")
                ),
                "session_outcome": outcome,
                "daily_account_return_fraction": row[
                    "primary_account_return_fraction"
                ],
                "stress_10bps_daily_account_return_fraction": row[
                    "stress_10bps_account_return_fraction"
                ],
                "stress_20bps_daily_account_return_fraction": row[
                    "stress_20bps_account_return_fraction"
                ],
            }
        )
        raw_signals = row.get("signals")
        if raw_signals is None:
            signals = [row] if outcome == "filled" else []
        elif isinstance(raw_signals, list):
            signals = raw_signals
        else:
            raise StrategyDiscoveryError(
                f"{phase} maturity row signals must be an array"
            )
        for index, signal in enumerate(signals):
            if not isinstance(signal, Mapping):
                raise StrategyDiscoveryError(
                    f"{phase} maturity row signals must contain objects"
                )
            signal_date = str(signal.get("date", day))
            if signal_date not in expected_dates:
                raise StrategyDiscoveryError(
                    f"{phase} closed signal date is outside the frozen dates"
                )
            closed_date = str(signal.get("closed_date", day))
            if closed_date not in expected_dates or closed_date < signal_date:
                raise StrategyDiscoveryError(
                    f"{phase} signal closure date is outside the frozen chronology"
                )
            signal_id = signal.get("signal_id")
            if not isinstance(signal_id, str) or not signal_id:
                signal_id = (
                    f"{signal_date}-{winner['strategy_id']}-{phase}-signal-{index}"
                )
            records.append({
                **common,
                "date": signal_date,
                "closed_date": closed_date,
                "record_type": "signal",
                "signal_id": signal_id,
                "closed": True,
                "eligible": True,
                "net_r": signal["net_r"],
                "stress_10bps_r": signal["stress_10bps_r"],
                "stress_20bps_r": signal["stress_20bps_r"],
                "net_account_return_fraction": signal[
                    "primary_account_return_fraction"
                ],
                "stress_10bps_account_return_fraction": signal[
                    "stress_10bps_account_return_fraction"
                ],
                "stress_20bps_account_return_fraction": signal[
                    "stress_20bps_account_return_fraction"
                ],
                "net_account_log_growth": math.log1p(
                    float(signal["primary_account_return_fraction"])
                ),
                "stress_10bps_account_log_growth": math.log1p(
                    float(signal["stress_10bps_account_return_fraction"])
                ),
                "stress_20bps_account_log_growth": math.log1p(
                    float(signal["stress_20bps_account_return_fraction"])
                ),
                "net_pnl_dollars": signal["net_pnl_dollars"],
                "stress_10bps_net_pnl_dollars": signal[
                    "stress_10bps_net_pnl_dollars"
                ],
                "stress_20bps_net_pnl_dollars": signal[
                    "stress_20bps_net_pnl_dollars"
                ],
                "stop_executed": signal["stop_executed"],
            })
    return records


def _write_historical_maturity_ledger(
    *,
    winner: Mapping[str, Any],
    confirmation_artifact: Mapping[str, Any],
    confirmation_inspection_path: Path,
    root: Path,
) -> tuple[Path, dict[str, Any]]:
    development_inspection = load_artifact(
        PROJECT_ROOT / str(winner["development_inspection_path"]),
        expected_kind="development-search-inspection",
    )
    development_result = load_artifact(
        PROJECT_ROOT / str(development_inspection["result_path"]),
        expected_kind="development-search-result",
    )
    development_evaluation = _load_development_evaluation(
        development_result,
        root=root,
    )
    selected_trial_id = development_inspection["selection"]["selected_trial_id"]
    matches = [
        item
        for item in development_evaluation["trials"]
        if item["trial_id"] == selected_trial_id
    ]
    if len(matches) != 1:
        raise StrategyDiscoveryError("selected development trial is absent or ambiguous")
    development_records = _phase_maturity_records(
        matches[0].get("maturity_rows"),
        phase="development",
        expected_dates=winner["development_evidence_dates"],
        winner=winner,
    )
    confirmation_records = _phase_maturity_records(
        confirmation_artifact["result"].get("maturity_rows"),
        phase="confirmation",
        expected_dates=winner["confirmation_dates"],
        winner=winner,
    )
    confirmation_relative = _relative(confirmation_inspection_path)
    evidence_hashes = {
        confirmation_relative: _file_hash(confirmation_inspection_path),
        str(winner["development_inspection_path"]): _file_hash(
            PROJECT_ROOT / str(winner["development_inspection_path"])
        ),
    }
    inspection_record = {
        "schema_version": portfolio_maturity.SCHEMA_VERSION,
        "research_campaign_id": CAMPAIGN_ID,
        "record_type": "inspection",
        "inspection_id": f"{winner['strategy_id']}-discovery-final-inspection",
        "recorded_at": winner["recorded_at"],
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "mechanism_family": winner.get("mechanism_family", winner["family_id"]),
        "rules_hash": winner["rules_hash"],
        "trial_count": winner["trial_count"],
        "selection_mode": "development_search",
        "power_target": winner["power_target"],
        "required_total_signals": winner["required_total_signals"],
        "required_confirmation_signals": winner[
            "required_confirmation_signals"
        ],
        "evidence_counts_frozen_before_confirmation": True,
        "trial_accounting_complete": True,
        "multiple_testing_clear": True,
        "execution_model_complete": True,
        "development_universe_representative": True,
        "confirmation_untouched": True,
        "confirmation_embargo_trading_days": len(winner["embargo_dates"]),
        "discovery_confirmation_inspection_path": confirmation_relative,
        "discovery_confirmation_inspection_sha256": load_artifact(
            confirmation_inspection_path, expected_kind="confirmation-inspection"
        )["artifact_sha256"],
        "evidence_hashes": evidence_hashes,
    }
    records = [inspection_record, *development_records, *confirmation_records]
    for record in records:
        portfolio_maturity.validate_record(record, root=PROJECT_ROOT)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "historical-maturity-ledger",
        "campaign_id": CAMPAIGN_ID,
        "family_id": winner["family_id"],
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "state": "HISTORICAL_EVIDENCE_INSPECTED",
        "confirmation_inspection_path": confirmation_relative,
        "records": records,
        "append_permitted": True,
        "broker_actions_permitted": False,
    }
    return _write_artifact(
        payload,
        root / str(winner["family_id"]) / "maturity-ledger",
        f"{winner['family_id']}-historical-maturity-ledger",
    )


def inspect_confirmation(
    result_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        require_committed(result_path)
    artifact = load_artifact(result_path, expected_kind="confirmation-result")
    winner_path = PROJECT_ROOT / str(artifact["winner_path"])
    if enforce_commit:
        require_committed(winner_path)
    winner = load_artifact(
        winner_path,
        expected_kind="frozen-strategy-winner",
    )
    if winner["artifact_sha256"] != artifact["winner_sha256"]:
        raise StrategyDiscoveryError("confirmation winner binding drifted")
    confirmation_manifest_path = Path(
        str(artifact["result"]["dataset_manifest"])
    )
    development_inspection_path = PROJECT_ROOT / str(
        winner["development_inspection_path"]
    )
    if enforce_commit:
        require_committed(confirmation_manifest_path)
        require_committed(development_inspection_path)
    development_inspection = load_artifact(
        development_inspection_path,
        expected_kind="development-search-inspection",
    )
    if (
        development_inspection["artifact_sha256"]
        != winner["development_inspection_sha256"]
    ):
        raise StrategyDiscoveryError(
            "confirmation development-inspection binding drifted"
        )
    inspection = inspect_confirmation_metrics(
        artifact["result"],
        required_signals=int(winner["required_confirmation_signals"]),
        expected_dates=winner["confirmation_dates"],
    )
    state = "CONFIRMATION_PASSED" if inspection["passed"] else "RETIRED_CONFIRMATION"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "confirmation-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": winner["family_id"],
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "state": state,
        "result_path": _relative(result_path),
        "result_sha256": artifact["artifact_sha256"],
        "winner_path": artifact["winner_path"],
        "winner_sha256": winner["artifact_sha256"],
        "inspection": inspection,
        "shadow_queue_permitted": state == "CONFIRMATION_PASSED",
        "broker_actions_permitted": False,
    }
    inspection_path, inspection_artifact = _write_artifact(
        payload,
        root / str(winner["family_id"]) / "confirmation-inspection",
        f"{winner['family_id']}-confirmation-inspection",
    )
    if state == "CONFIRMATION_PASSED":
        _write_historical_maturity_ledger(
            winner=winner,
            confirmation_artifact=artifact,
            confirmation_inspection_path=inspection_path,
            root=root,
        )
    if enforce_commit and winner.get("confirmation_scope") is not None:
        outcome_exposure.ensure_record(
            outcome_exposure.build_record(
                exposure_id=(
                    f"confirmation-{winner['family_id']}-"
                    f"{artifact['artifact_sha256'][:16]}"
                ),
                campaign_id=CAMPAIGN_ID,
                lane="confirmation",
                recorded_at=winner["recorded_at"],
                source_path=_relative(result_path),
                source_sha256=_file_hash(result_path),
                scope=winner["confirmation_scope"],
            )
        )
    return inspection_path, inspection_artifact


def _maturity_record_identity(record: Mapping[str, Any]) -> tuple[str, str]:
    identifier = (
        record.get("signal_id")
        or record.get("session_id")
        or record.get("inspection_id")
    )
    return str(record.get("record_type")), str(identifier)


def _verify_historical_admission(
    artifact: Mapping[str, Any], *, ledger_path: Path
) -> tuple[int, dict[str, Any]]:
    records = artifact.get("records")
    if not isinstance(records, list) or not records:
        raise StrategyDiscoveryError("historical maturity records are missing")
    if not isinstance(records[0], Mapping):
        raise StrategyDiscoveryError("historical maturity inspection record is invalid")
    admitted = {
        _maturity_record_identity(record): record
        for record in portfolio_maturity.read_records(ledger_path)
    }
    for record in records:
        if not isinstance(record, Mapping):
            raise StrategyDiscoveryError("historical maturity record is invalid")
        key = _maturity_record_identity(record)
        if admitted.get(key) != record:
            raise StrategyDiscoveryError(
                "historical maturity ledger has not been admitted unchanged"
            )
    assessment = portfolio_maturity.assess_strategy(
        records, portfolio_maturity.load_config()
    )
    return len(records), assessment


def admit_historical(
    artifact_path: Path,
    *,
    ledger_path: Path = portfolio_maturity.DEFAULT_LEDGER_PATH,
    enforce_commit: bool = True,
) -> dict[str, Any]:
    """Atomically admit one committed independently inspected historical ledger."""
    if enforce_commit:
        require_committed(artifact_path)
    artifact = load_artifact(
        artifact_path, expected_kind="historical-maturity-ledger"
    )
    if not (
        artifact.get("state") == "HISTORICAL_EVIDENCE_INSPECTED"
        and artifact.get("append_permitted") is True
        and artifact.get("broker_actions_permitted") is False
    ):
        raise StrategyDiscoveryError(
            "historical maturity ledger is not admission-ready"
        )
    records = artifact.get("records")
    if not isinstance(records, list) or not records:
        raise StrategyDiscoveryError("historical maturity records are missing")
    if not isinstance(records[0], Mapping):
        raise StrategyDiscoveryError("historical maturity inspection record is invalid")
    inspection_path = PROJECT_ROOT / str(
        artifact.get("confirmation_inspection_path")
    )
    if enforce_commit:
        require_committed(inspection_path)
    inspection = load_artifact(
        inspection_path, expected_kind="confirmation-inspection"
    )
    if not (
        inspection.get("state") == "CONFIRMATION_PASSED"
        and inspection.get("artifact_sha256")
        == records[0].get(
            "discovery_confirmation_inspection_sha256"
        )
        and inspection.get("strategy_id") == artifact.get("strategy_id")
        and inspection.get("strategy_version") == artifact.get("strategy_version")
        and inspection.get("rules_hash") == artifact.get("rules_hash")
    ):
        raise StrategyDiscoveryError(
            "historical maturity confirmation binding is invalid"
        )
    confirmation_result_path = PROJECT_ROOT / str(inspection["result_path"])
    winner_path = PROJECT_ROOT / str(inspection["winner_path"])
    if enforce_commit:
        require_committed(confirmation_result_path)
        require_committed(winner_path)
    confirmation_result = load_artifact(
        confirmation_result_path, expected_kind="confirmation-result"
    )
    winner = load_artifact(winner_path, expected_kind="frozen-strategy-winner")
    if (
        confirmation_result["artifact_sha256"] != inspection.get("result_sha256")
        or winner["artifact_sha256"] != inspection.get("winner_sha256")
        or confirmation_result.get("winner_sha256") != winner["artifact_sha256"]
    ):
        raise StrategyDiscoveryError(
            "historical maturity predecessor chain is invalid"
        )
    try:
        admission = portfolio_maturity.append_records(
            records,
            ledger_path,
            root=PROJECT_ROOT,
            idempotent=True,
        )
    except portfolio_maturity.PortfolioMaturityError as exc:
        raise StrategyDiscoveryError(str(exc)) from exc
    return {
        "state": "HISTORICAL_EVIDENCE_ADMITTED",
        "strategy_id": artifact["strategy_id"],
        "strategy_version": artifact["strategy_version"],
        "rules_hash": artifact["rules_hash"],
        "ledger_path": str(ledger_path),
        **admission,
        "broker_actions_performed": 0,
    }


def queue_shadow(
    winner_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    ledger_path: Path = portfolio_maturity.DEFAULT_LEDGER_PATH,
    enforce_commit: bool = True,
    queued_at: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        require_committed(winner_path)
    winner = load_artifact(winner_path, expected_kind="frozen-strategy-winner")
    inspection_path, inspection = _find_single(
        root / str(winner["family_id"]) / "confirmation-inspection",
        "*.json",
        kind="confirmation-inspection",
    )
    if enforce_commit:
        require_committed(inspection_path)
    if inspection["state"] != "CONFIRMATION_PASSED":
        raise StrategyDiscoveryError("confirmation has not passed unchanged")
    if inspection["winner_sha256"] != winner["artifact_sha256"]:
        raise StrategyDiscoveryError("shadow queue winner binding drifted")
    maturity_artifact_path, ledger = _find_single(
        root / str(winner["family_id"]) / "maturity-ledger",
        "*.json",
        kind="historical-maturity-ledger",
    )
    if enforce_commit:
        require_committed(maturity_artifact_path)
    if ledger["rules_hash"] != winner["rules_hash"]:
        raise StrategyDiscoveryError("historical maturity ledger binding drifted")
    if enforce_commit:
        require_committed(ledger_path)
    admitted_records, historical_assessment = _verify_historical_admission(
        ledger, ledger_path=ledger_path
    )
    if historical_assessment.get("validation_phase") != "SHADOW_QUALIFICATION":
        raise StrategyDiscoveryError(
            "historical maturity gates did not clear for shadow qualification: "
            + "; ".join(historical_assessment.get("current_phase_blockers", []))
        )
    activation = queued_at or datetime.now(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    try:
        activation_timestamp = datetime.fromisoformat(
            activation.replace("Z", "+00:00")
        )
        winner_timestamp = datetime.fromisoformat(
            str(winner["recorded_at"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError) as exc:
        raise StrategyDiscoveryError(
            "shadow queue and winner timestamps must be valid ISO timestamps"
        ) from exc
    if activation_timestamp.tzinfo is None or winner_timestamp.tzinfo is None:
        raise StrategyDiscoveryError(
            "shadow queue and winner timestamps must include timezones"
        )
    if activation_timestamp <= winner_timestamp:
        raise StrategyDiscoveryError(
            "shadow queue activation must follow winner preregistration"
        )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "prospective-shadow-queue",
        "campaign_id": CAMPAIGN_ID,
        "family_id": winner["family_id"],
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "state": "SHADOW_QUEUED",
        "queued_at": activation,
        "winner_path": _relative(winner_path),
        "winner_sha256": winner["artifact_sha256"],
        "required_clean_closed_shadows": 5,
        "completed_clean_closed_shadows": 0,
        "confirmation_inspection_path": _relative(inspection_path),
        "confirmation_inspection_sha256": inspection["artifact_sha256"],
        "historical_maturity_ledger_path": _relative(maturity_artifact_path),
        "historical_maturity_ledger_sha256": ledger["artifact_sha256"],
        "historical_records_admitted": admitted_records,
        "historical_admission_verified": True,
        "historical_validation_phase": historical_assessment["validation_phase"],
        "broker_actions_permitted": False,
    }
    return _write_artifact(
        payload,
        root / str(winner["family_id"]) / "shadow",
        f"{winner['family_id']}-shadow-queue",
    )


def build_status(*, root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    import continuous_strategy_discovery

    maturity = portfolio_maturity.build_report()
    assessments = {
        (
            str(item["strategy_id"]),
            str(item["strategy_version"]),
            str(item["rules_hash"]),
        ): item
        for item in maturity["strategies"]
    }
    stage_order = {
        "discovery-preflight-inspection": 1,
        "frozen-development-search": 2,
        "development-search-result": 3,
        "development-search-inspection": 4,
        "frozen-strategy-winner": 5,
        "confirmation-result": 6,
        "confirmation-inspection": 7,
        "historical-maturity-ledger": 8,
        "prospective-shadow-queue": 9,
    }
    families: list[dict[str, Any]] = []
    if root.exists():
        for family_root in sorted(path for path in root.iterdir() if path.is_dir()):
            artifacts: list[dict[str, Any]] = []
            values_by_kind: dict[str, dict[str, Any]] = {}
            for path in sorted(family_root.glob("**/*.json")):
                try:
                    value = load_artifact(path)
                except StrategyDiscoveryError:
                    continue
                kind = str(value.get("artifact_kind"))
                if kind in stage_order:
                    values_by_kind[kind] = value
                artifacts.append(
                    {
                        "kind": kind,
                        "state": value.get("state"),
                        "path": _relative(path),
                    }
                )
            ordered = sorted(
                (
                    (stage_order[kind], value)
                    for kind, value in values_by_kind.items()
                ),
                key=lambda item: item[0],
            )
            current = ordered[-1][1] if ordered else None
            search = values_by_kind.get("frozen-development-search", {})
            preflight = values_by_kind.get("discovery-preflight-inspection", {})
            development_inspection = values_by_kind.get(
                "development-search-inspection", {}
            )
            winner = values_by_kind.get("frozen-strategy-winner", {})
            confirmation_inspection = values_by_kind.get(
                "confirmation-inspection", {}
            )
            shadow_queue = values_by_kind.get("prospective-shadow-queue", {})
            selection = development_inspection.get("selection", {})
            if not isinstance(selection, Mapping):
                selection = {}
            contract = search.get("family_contract", {})
            if not isinstance(contract, Mapping):
                contract = {}
            assessment = assessments.get(
                (
                    str(winner.get("strategy_id")),
                    str(winner.get("strategy_version")),
                    str(winner.get("rules_hash")),
                )
            )
            if assessment is not None:
                blockers = list(assessment["current_phase_blockers"])
                current_state = str(assessment["validation_phase"])
                shadow_metrics = assessment["metrics"]
                completed_shadows = int(shadow_metrics["shadow_executions"])
                shadow_attempts = int(shadow_metrics["shadow_attempts"])
                shadow_resets = int(
                    shadow_metrics["shadow_qualification_resets"]
                )
            else:
                current_state = (
                    str(current.get("state")) if current is not None else "UNSTARTED"
                )
                completed_shadows = 0
                shadow_attempts = 0
                shadow_resets = 0
                blockers = []
                if preflight and preflight.get("state") != "CAPACITY_READY":
                    blockers.append(
                        f"preflight disposition: {preflight.get('state')}"
                    )
                elif development_inspection and development_inspection.get(
                    "state"
                ) != "WINNER_SELECTED":
                    blockers.append(
                        "development inspection disposition: "
                        f"{development_inspection.get('state')}"
                    )
                elif confirmation_inspection and confirmation_inspection.get(
                    "state"
                ) != "CONFIRMATION_PASSED":
                    blockers.append(
                        "confirmation inspection disposition: "
                        f"{confirmation_inspection.get('state')}"
                    )
                elif shadow_queue:
                    blockers.append(
                        "clean closed shadows "
                        f"{completed_shadows} is below required "
                        f"{shadow_queue.get('required_clean_closed_shadows', 5)}"
                    )
                elif current is not None:
                    blockers.append(f"next transition is required after {current_state}")
            families.append(
                {
                    "family_id": family_root.name,
                    "artifacts": artifacts,
                    "current_state": current_state,
                    "blockers": blockers,
                    "trial_count": (
                        search.get("trial_count")
                        or winner.get("trial_count")
                        or len(contract.get("trial_family", []))
                    ),
                    "power_target": (
                        winner.get("power_target") or selection.get("power_target")
                    ),
                    "required_total_signals": (
                        winner.get("required_total_signals")
                        or selection.get("required_total_signals")
                    ),
                    "development_filled_signals": selection.get(
                        "development_filled_signals"
                    ),
                    "maximum_total_signal_capacity": development_inspection.get(
                        "maximum_total_signal_capacity"
                    ),
                    "required_confirmation_signals": (
                        winner.get("required_confirmation_signals")
                        or selection.get("required_confirmation_signals")
                    ),
                    "confirmation_reserved_sessions": len(
                        winner.get(
                            "confirmation_dates",
                            contract.get("confirmation_dates", []),
                        )
                    ),
                    "confirmation_state": confirmation_inspection.get("state"),
                    "shadow_progress": {
                        "required_clean_closed": shadow_queue.get(
                            "required_clean_closed_shadows", 5
                        ),
                        "completed_clean_closed": completed_shadows,
                        "attempts": shadow_attempts,
                        "qualification_resets": shadow_resets,
                    },
                    "maturity": (
                        assessment.get("maturity") if assessment is not None else None
                    ),
                }
            )
    next_batch = next_week_discovery_batch.activation_status()
    continuous_lane = continuous_strategy_discovery.build_status()
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": CAMPAIGN_ID,
        "families": families,
        "active_family_count": len(families),
        "first_pilot_readiness": {
            "pilot_ready": maturity["pilot_ready_strategy_count"],
            "live_closed_reconciled": maturity[
                "pilot_ready_live_started_strategy_count"
            ],
            "milestone": maturity["first_pilot_milestone"],
            "blockers": maturity["first_pilot_milestone_blockers"],
        },
        "portfolio_readiness": {
            "target": maturity["portfolio_target"],
            "pilot_ready": maturity["pilot_ready_strategy_count"],
            "milestone": maturity["portfolio_milestone"],
            "blockers": maturity["portfolio_milestone_blockers"],
        },
        "continuous_existing_family_lane": continuous_lane,
        "next_family_batch": next_batch,
        "broker_actions_permitted": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument(
        "--ledger", type=Path, default=portfolio_maturity.DEFAULT_LEDGER_PATH
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status")
    for command, help_text in (
        ("preflight", "run and inspect outcome-blind capacity"),
        ("freeze-search", "freeze the complete development search"),
        ("evaluate-development", "evaluate every frozen development trial"),
        ("inspect-development", "rebuild selection and freeze power targets"),
        ("freeze-winner", "freeze one exact selected strategy version"),
        ("evaluate-confirmation", "evaluate only the frozen winner"),
        ("inspect-confirmation", "rebuild untouched confirmation gates"),
        ("admit-historical", "atomically admit inspected historical evidence"),
        ("queue-shadow", "queue five prospective zero-broker shadows"),
    ):
        child = subparsers.add_parser(command, help=help_text)
        child.add_argument("artifact", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            result: Any = build_status(root=args.root)
        elif args.command == "admit-historical":
            result = admit_historical(args.artifact, ledger_path=args.ledger)
        else:
            function = {
                "preflight": run_preflight,
                "freeze-search": freeze_search,
                "evaluate-development": evaluate_development,
                "inspect-development": inspect_development,
                "freeze-winner": freeze_winner,
                "evaluate-confirmation": evaluate_confirmation,
                "inspect-confirmation": inspect_confirmation,
                "queue-shadow": queue_shadow,
            }[args.command]
            if args.command == "queue-shadow":
                path, artifact = function(
                    args.artifact, root=args.root, ledger_path=args.ledger
                )
            else:
                path, artifact = function(args.artifact, root=args.root)
            result = {
                "written": _relative(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "broker_actions_permitted": False,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (StrategyDiscoveryError, LearningExperimentError, OSError, ValueError) as exc:
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
