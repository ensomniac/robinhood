"""Validate the unchanged Schedule 13D strategy after Stage 0 survival."""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

import portfolio_maturity as maturity
import schedule13d_capacity as capacity
import schedule13d_semantic_capacity as semantic
import schedule13d_stage0 as stage0
import schedule13d_symbol_documents as symbols
import sector_etf_rotation_stage0 as daily
from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
)
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
PORTFOLIO_CONFIG = PROJECT_ROOT / "portfolio_config.toml"
STAGE0_CONTRACT_SHA256 = (
    "a43b9f9039ca6c260be4174855cdf65491983c635e3c3a74e062baa74c9d7526"
)
STAGE0_RESULT_SHA256 = (
    "fafd9ab4b61395efb256c449f316580b1e2ee622df3f601562e9d426f460bd6a"
)
STAGE0_INSPECTION_SHA256 = (
    "35070c7871d5a765407c97e193614241e13847bfcc61f91e5941cf9741451d76"
)
STAGE0_CONTRACT_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/stage0/contracts/"
    f"{capacity.CANDIDATE_ID}-{STAGE0_CONTRACT_SHA256}.json"
)
STAGE0_RESULT_PATH = (
    PROJECT_ROOT
    / "research_results/"
    f"2026-07-22-{capacity.CANDIDATE_ID}-stage0-{STAGE0_RESULT_SHA256}.json"
)
STAGE0_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/stage0/inspections/"
    f"{capacity.CANDIDATE_ID}-result-{STAGE0_INSPECTION_SHA256}.json"
)
ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/validation"
CONTRACT_ROOT = ROOT / "contracts"
ACTIVATION_ROOT = ROOT / "activations"
INSPECTION_ROOT = ROOT / "inspections"
RESULT_ROOT = PROJECT_ROOT / "research_results"
PRIVATE_ROOT = (
    DATA_ROOT
    / "_derived/schedule13d_validation/"
    "dataset-schedule-13d-activist-continuation-validation-2026-07-22-v1"
)
SCHEMA_VERSION = 1
STRATEGY_ID = "schedule-13d-activist-continuation"
STRATEGY_VERSION = capacity.STRATEGY_VERSION
VARIANT_ID = capacity.CANDIDATE_ID
MECHANISM_FAMILY = capacity.THEME_ID
PHASES = ("development", "confirmation")


class Schedule13dValidationError(RuntimeError):
    """The representative or confirmation validation evidence is inconsistent."""


class DailyBarProvider(Protocol):
    def fetch_bars(
        self,
        symbol: str,
        start: str | Any,
        end: str | Any,
        *,
        bar_size: str = "1 day",
        what: str = "TRADES",
        use_rth: bool = True,
    ) -> list[dict[str, Any]]: ...


def _read_json(path: Path) -> dict[str, Any]:
    return symbols._read_object(path)


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    symbols._write_json(value, path)


def _published(path: Path) -> None:
    relative = path.resolve().relative_to(PROJECT_ROOT)
    completed = subprocess.run(
        ("git", "status", "--porcelain=v1", "--", str(relative)),
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    if completed.stdout.strip():
        raise Schedule13dValidationError(f"required artifact is not published: {relative}")


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dValidationError(f"expected one {description}; found {len(matches)}")
    return matches[0]


def _stage0_lineage(*, require_published: bool) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    contract = _read_json(STAGE0_CONTRACT_PATH)
    result = _read_json(STAGE0_RESULT_PATH)
    inspection = _read_json(STAGE0_INSPECTION_PATH)
    if not (
        contract.get("contract_sha256") == STAGE0_CONTRACT_SHA256
        and contract.get("contract_sha256")
        == capacity.successor._self_hash(contract, "contract_sha256")
        and result.get("result_sha256") == STAGE0_RESULT_SHA256
        and result.get("result_sha256")
        == capacity.successor._self_hash(result, "result_sha256")
        and result.get("stage0_survived") is True
        and result.get("rules_hash") == contract.get("rules_hash")
        and inspection.get("inspection_sha256") == STAGE0_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == capacity.successor._self_hash(inspection, "inspection_sha256")
        and inspection.get("result_sha256") == result["result_sha256"]
        and inspection.get("stage0_survived") is True
        and inspection.get("valid") is True
    ):
        raise Schedule13dValidationError("inspected Stage 0 survivor lineage is invalid")
    if require_published:
        for path in (STAGE0_CONTRACT_PATH, STAGE0_RESULT_PATH, STAGE0_INSPECTION_PATH):
            _published(path)
    return contract, result, inspection


def build_contract(*, require_published: bool = True) -> dict[str, Any]:
    stage0_contract, stage0_result, stage0_inspection = _stage0_lineage(
        require_published=require_published
    )
    sessions = stage0._xnys_sessions()
    phases = {
        phase: [
            stage0._request_contract(row, sessions)
            for row in stage0_contract["frozen_partitions"][phase]
        ]
        for phase in PHASES
    }
    if len(phases["development"]) < 30 or len(phases["confirmation"]) != 20:
        raise Schedule13dValidationError("validation partition denominator differs")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "representative-validation-zero-result-contract",
        "campaign_id": capacity.CAMPAIGN_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "variant_id": VARIANT_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "rules_hash": stage0_contract["rules_hash"],
        "stage0_contract_sha256": stage0_contract["contract_sha256"],
        "stage0_result_sha256": stage0_result["result_sha256"],
        "stage0_result_inspection_sha256": stage0_inspection["inspection_sha256"],
        "partition_sha256": stage0_contract["partition_sha256"],
        "confirmation_embargo_trading_days": stage0.CONFIRMATION_EMBARGO_SESSIONS,
        "phase_denominator": {phase: len(rows) for phase, rows in phases.items()},
        "phase_input_requests": phases,
        "unchanged_execution_contract": stage0_contract["execution_contract"],
        "unchanged_exit_contract": stage0_contract["exit_contract"],
        "unchanged_cost_contract": stage0_contract["cost_contract"],
        "robustness_gate": maturity.load_config(PORTFOLIO_CONFIG).raw["pilot_ready"],
        "access_contract": {
            "development_input_access_after_contract_inspection_permitted": True,
            "development_evaluation_before_input_inspection_permitted": False,
            "confirmation_input_access_before_inspected_development_pass_permitted": False,
            "confirmation_evaluation_before_input_inspection_permitted": False,
            "broker_actions_permitted": False,
        },
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "stage0_implementation_sha256": sha256_file(PROJECT_ROOT / "schedule13d_stage0.py"),
        "market_outcomes_accessed": False,
        "returns_computed": 0,
        "claim_limit": "Development and confirmation validation only; no live or shadow claim.",
    }
    value["contract_sha256"] = capacity.successor._self_hash(value, "contract_sha256")
    return value


def contract_path(value: Mapping[str, Any]) -> Path:
    return CONTRACT_ROOT / f"{VARIANT_ID}-{value['contract_sha256']}.json"


def inspect_contract(path: Path) -> dict[str, Any]:
    recorded = _read_json(path)
    if recorded.get("contract_sha256") != capacity.successor._self_hash(
        recorded, "contract_sha256"
    ):
        raise Schedule13dValidationError("validation contract hash is invalid")
    if recorded != build_contract(require_published=False):
        raise Schedule13dValidationError("validation contract does not rebuild")
    if not (
        recorded["phase_denominator"]["development"] >= 30
        and recorded["phase_denominator"]["confirmation"] == 20
        and recorded["confirmation_embargo_trading_days"] >= 5
        and recorded["market_outcomes_accessed"] is False
        and recorded["returns_computed"] == 0
    ):
        raise Schedule13dValidationError("validation anti-tuning boundary differs")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "representative-validation-zero-result-contract-inspection",
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "rules_hash": recorded["rules_hash"],
        "contract_sha256": recorded["contract_sha256"],
        "contract_file_sha256": sha256_file(path),
        "phase_denominator": recorded["phase_denominator"],
        "confirmation_embargo_trading_days": recorded["confirmation_embargo_trading_days"],
        "development_input_access_permitted": True,
        "development_evaluation_permitted": False,
        "confirmation_input_access_permitted": False,
        "broker_actions_permitted": False,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "valid": True,
    }
    value["inspection_sha256"] = capacity.successor._self_hash(value, "inspection_sha256")
    return value


def _contract_and_inspection() -> tuple[Path, Path, dict[str, Any], dict[str, Any]]:
    contract_path_value = _one(
        "strategy_tournament/v2/schedule13d/validation/contracts/"
        "schedule-13d-activist-continuation-v1-*.json",
        "validation contract",
    )
    inspection_path = _one(
        "strategy_tournament/v2/schedule13d/validation/inspections/"
        "schedule-13d-activist-continuation-v1-contract-*.json",
        "validation contract inspection",
    )
    contract = _read_json(contract_path_value)
    inspection = _read_json(inspection_path)
    if inspection != inspect_contract(contract_path_value):
        raise Schedule13dValidationError("validation contract inspection differs")
    return contract_path_value, inspection_path, contract, inspection


def _private_input_path(phase: str) -> Path:
    return PRIVATE_ROOT / f"{phase}-inputs.json.gz"


def _development_passed() -> tuple[Path, Path, dict[str, Any], dict[str, Any]]:
    result_path = _one(
        "research_results/2026-07-22-schedule-13d-activist-continuation-v1-"
        "development-*.json",
        "development result",
    )
    inspection_path = _one(
        "strategy_tournament/v2/schedule13d/validation/inspections/"
        "schedule-13d-activist-continuation-v1-development-result-*.json",
        "development result inspection",
    )
    result = _read_json(result_path)
    inspection = _read_json(inspection_path)
    if not (
        result.get("sample_phase") == "development"
        and result.get("phase_passed") is True
        and inspection.get("sample_phase") == "development"
        and inspection.get("result_sha256") == result.get("result_sha256")
        and inspection.get("phase_passed") is True
        and inspection.get("valid") is True
    ):
        raise Schedule13dValidationError("confirmation requires inspected development pass")
    for path in (result_path, inspection_path):
        _published(path)
    return result_path, inspection_path, result, inspection


def _phase_allowed(phase: str, inspection: Mapping[str, Any]) -> dict[str, Any] | None:
    if phase == "development":
        if inspection.get("development_input_access_permitted") is not True:
            raise Schedule13dValidationError("development input access is closed")
        return None
    _result_path, _inspection_path, result, _audit = _development_passed()
    return result


def _activation_from_private(
    contract: Mapping[str, Any],
    inspection: Mapping[str, Any],
    phase: str,
    graph: Mapping[str, Any],
    development_result: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not (
        graph.get("input_sha256") == capacity.successor._self_hash(graph, "input_sha256")
        and graph.get("contract_sha256") == contract["contract_sha256"]
        and graph.get("request_count") == len(contract["phase_input_requests"][phase])
        and graph.get("returns_computed") == 0
    ):
        raise Schedule13dValidationError(f"private {phase} input graph is invalid")
    path = _private_input_path(phase)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "representative-validation-input-activation",
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "sample_phase": phase,
        "rules_hash": contract["rules_hash"],
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "development_result_sha256": (
            development_result["result_sha256"] if development_result else None
        ),
        "input_sha256": graph["input_sha256"],
        "private_input_file_sha256": sha256_file(path),
        "frozen_signals": graph["request_count"],
        "complete_signals": graph["complete_signal_count"],
        "incomplete_signals": graph["incomplete_signal_count"],
        "daily_rows_frozen": graph["market_outcome_rows_frozen"],
        "provider_requests": graph["request_count"],
        "return_evaluation_before_input_inspection_permitted": False,
        "broker_actions_permitted": False,
        "returns_computed": 0,
        "maturity_effect": "NONE",
    }
    value["activation_sha256"] = capacity.successor._self_hash(value, "activation_sha256")
    return value


def build_activation(
    phase: str,
    provider: DailyBarProvider | None = None,
    *,
    write_private: bool = False,
) -> dict[str, Any]:
    if phase not in PHASES:
        raise Schedule13dValidationError("invalid validation phase")
    contract_path_value, inspection_path, contract, inspection = _contract_and_inspection()
    daily._require_published((contract_path_value, inspection_path))
    development_result = _phase_allowed(phase, inspection)
    path = _private_input_path(phase)
    if write_private:
        config = AlpacaConfig.optional_from_env(PROJECT_ROOT / ".env")
        if provider is None and config is None:
            raise Schedule13dValidationError("Alpaca configuration is unavailable")
        owned = None
        try:
            if provider is None:
                owned = AlpacaHistoricalClient(replace(config, adjustment="all", feed="sip"))
                provider = owned
            phase_contract = {
                "contract_sha256": contract["contract_sha256"],
                "rules_hash": contract["rules_hash"],
                "stage0_input_requests": contract["phase_input_requests"][phase],
            }
            graph = stage0._collect_inputs(phase_contract, provider)
            graph["sample_phase"] = phase
            graph["input_sha256"] = capacity.successor._self_hash(graph, "input_sha256")
            semantic._write_gzip_json(graph, path)
        finally:
            if owned is not None:
                owned.close()
    if not path.is_file():
        raise Schedule13dValidationError(f"private {phase} inputs are missing")
    graph = semantic._read_gzip_object(path)
    return _activation_from_private(
        contract, inspection, phase, graph, development_result
    )


def activation_path(value: Mapping[str, Any]) -> Path:
    return ACTIVATION_ROOT / (
        f"{VARIANT_ID}-{value['sample_phase']}-{value['activation_sha256']}.json"
    )


def inspect_inputs(path: Path, phase: str) -> dict[str, Any]:
    recorded = _read_json(path)
    if recorded.get("activation_sha256") != capacity.successor._self_hash(
        recorded, "activation_sha256"
    ):
        raise Schedule13dValidationError("validation activation hash is invalid")
    if recorded != build_activation(phase, write_private=False):
        raise Schedule13dValidationError("validation activation does not rebuild")
    graph = semantic._read_gzip_object(_private_input_path(phase))
    _contract_path, _inspection_path, contract, _inspection = _contract_and_inspection()
    expected = {
        row["request_sha256"] for row in contract["phase_input_requests"][phase]
    }
    observed = {row["request"]["request_sha256"] for row in graph["records"]}
    if expected != observed or len(observed) != len(expected):
        raise Schedule13dValidationError(f"{phase} input denominator differs")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "representative-validation-input-inspection",
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "sample_phase": phase,
        "rules_hash": recorded["rules_hash"],
        "contract_sha256": recorded["contract_sha256"],
        "activation_sha256": recorded["activation_sha256"],
        "activation_file_sha256": sha256_file(path),
        "input_sha256": graph["input_sha256"],
        "private_input_file_sha256": sha256_file(_private_input_path(phase)),
        "frozen_signals": graph["request_count"],
        "complete_signals": graph["complete_signal_count"],
        "incomplete_signals": graph["incomplete_signal_count"],
        "daily_rows_inspected": graph["market_outcome_rows_frozen"],
        "complete_denominator_verified": True,
        "return_evaluation_permitted": True,
        "broker_actions_permitted": False,
        "returns_computed": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    value["inspection_sha256"] = capacity.successor._self_hash(value, "inspection_sha256")
    return value


def _serializable_metrics(metrics: maturity.RobustnessMetrics) -> dict[str, Any]:
    result = asdict(metrics)
    for field in ("profit_factor", "stress_10_profit_factor", "stress_20_profit_factor"):
        value = result[field]
        result[f"{field}_infinite"] = isinstance(value, float) and math.isinf(value)
        if result[f"{field}_infinite"]:
            result[field] = None
    return result


def _phase_blockers(metrics: maturity.RobustnessMetrics, phase: str) -> list[str]:
    config = maturity.load_config(PORTFOLIO_CONFIG)
    gate = config.raw["pilot_ready"]
    minimum = (
        int(gate["minimum_closed_historical_signals"])
        - int(gate["minimum_confirmation_signals"])
        if phase == "development"
        else int(gate["minimum_confirmation_signals"])
    )
    threshold = float(
        gate[
            "minimum_expectancy_r"
            if phase == "development"
            else "minimum_confirmation_expectancy_r"
        ]
    )
    return maturity._robustness_blockers(
        phase,
        metrics,
        minimum_signals=minimum,
        expectancy_threshold=threshold,
        gate=gate,
    )


def build_result(
    activation_path_value: Path,
    inspection_path: Path,
    phase: str,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    activation = _read_json(activation_path_value)
    inspection = _read_json(inspection_path)
    if inspection != inspect_inputs(activation_path_value, phase):
        raise Schedule13dValidationError(f"{phase} input inspection differs")
    if inspection.get("return_evaluation_permitted") is not True:
        raise Schedule13dValidationError(f"{phase} evaluation is closed")
    if require_published:
        daily._require_published((activation_path_value, inspection_path))
    graph = semantic._read_gzip_object(_private_input_path(phase))
    result_records = []
    ledger_records = []
    missed = []
    for row in graph["records"]:
        request = row["request"]
        if row.get("complete") is not True:
            missed.append(
                {
                    "event_ordinal": request["event_ordinal"],
                    "event_accession": request["event_accession"],
                    "symbol": request["symbol"],
                    "entry_session": request["entry_session"],
                    "missing_sessions": row["missing_sessions"],
                    "reason": "INCOMPLETE_FROZEN_PROVIDER_INPUT",
                }
            )
            continue
        outcomes = {
            str(cost): stage0._outcome(row["atr_rows"], row["outcome_rows"], cost)
            for cost in (stage0.PRIMARY_COST_BPS, *stage0.STRESS_COST_BPS)
        }
        primary = outcomes[str(stage0.PRIMARY_COST_BPS)]
        record = {
            "event_ordinal": request["event_ordinal"],
            "event_accession": request["event_accession"],
            "date": request["entry_session"],
            "symbol": request["symbol"],
            "exit_date": primary["exit_date"],
            "exit_reason": primary["exit_reason"],
            "stop_executed": primary["stop_executed"],
            "net_r": primary["net_r"],
            "stress_10bps_r": outcomes["10"]["net_r"],
            "stress_20bps_r": outcomes["20"]["net_r"],
        }
        result_records.append(record)
        ledger_records.append(
            {
                "schema_version": SCHEMA_VERSION,
                "record_type": "signal",
                "recorded_at": "2026-07-22T00:00:00-04:00",
                "strategy_id": STRATEGY_ID,
                "strategy_version": STRATEGY_VERSION,
                "mechanism_family": MECHANISM_FAMILY,
                "rules_hash": activation["rules_hash"],
                "date": request["entry_session"],
                "sample_phase": phase,
                "mode": "historical",
                "signal_id": (
                    f"{request['entry_session']}-{STRATEGY_ID}-{phase}-"
                    f"{request['event_ordinal']}"
                ),
                "closed": True,
                "eligible": True,
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
                "stop_executed": primary["stop_executed"],
                "session_capture_complete": True,
                "rule_violations": [],
            }
        )
    config = maturity.load_config(PORTFOLIO_CONFIG)
    metrics = maturity._robustness_metrics(
        ledger_records, float(config.raw["pilot_ready"]["minimum_bootstrap_confidence"])
    )
    blockers = _phase_blockers(metrics, phase)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "representative-validation-result",
        "campaign_id": capacity.CAMPAIGN_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "variant_id": VARIANT_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "sample_phase": phase,
        "rules_hash": activation["rules_hash"],
        "contract_sha256": activation["contract_sha256"],
        "activation_sha256": activation["activation_sha256"],
        "input_inspection_sha256": inspection["inspection_sha256"],
        "denominator": {
            "frozen_signals": graph["request_count"],
            "closed_signals": len(result_records),
            "missed_entries": len(missed),
            "rule_violations": 0,
        },
        "robustness": _serializable_metrics(metrics),
        "phase_passed": not blockers,
        "phase_blockers": blockers,
        "next_action": (
            "collect untouched confirmation under the same rules"
            if phase == "development" and not blockers
            else (
                "begin five prospective shadows"
                if phase == "confirmation" and not blockers
                else "retire this exact strategy version without parameter repair"
            )
        ),
        "missed_entry_records": missed,
        "records": result_records,
        "ledger_records": ledger_records,
        "broker_actions": 0,
        "maturity_effect": "ELIGIBLE_AFTER_INDEPENDENT_RESULT_INSPECTION",
    }
    value["result_sha256"] = capacity.successor._self_hash(value, "result_sha256")
    return value


def inspect_result(
    activation_path_value: Path,
    input_inspection_path: Path,
    result_path: Path,
    phase: str,
) -> dict[str, Any]:
    recorded = _read_json(result_path)
    if recorded.get("result_sha256") != capacity.successor._self_hash(
        recorded, "result_sha256"
    ):
        raise Schedule13dValidationError(f"{phase} result hash is invalid")
    rebuilt = build_result(
        activation_path_value,
        input_inspection_path,
        phase,
        require_published=False,
    )
    if recorded != rebuilt:
        raise Schedule13dValidationError(f"{phase} result does not rebuild")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "representative-validation-result-inspection",
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "sample_phase": phase,
        "rules_hash": recorded["rules_hash"],
        "contract_sha256": recorded["contract_sha256"],
        "activation_sha256": recorded["activation_sha256"],
        "input_inspection_sha256": recorded["input_inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "denominator": recorded["denominator"],
        "robustness": recorded["robustness"],
        "phase_passed": recorded["phase_passed"],
        "phase_blockers": recorded["phase_blockers"],
        "ledger_records_rebuilt": len(recorded["ledger_records"]),
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = capacity.successor._self_hash(value, "inspection_sha256")
    return value


def _activation_file(phase: str) -> Path:
    return _one(
        "strategy_tournament/v2/schedule13d/validation/activations/"
        f"schedule-13d-activist-continuation-v1-{phase}-*.json",
        f"{phase} activation",
    )


def _input_inspection_file(phase: str) -> Path:
    return _one(
        "strategy_tournament/v2/schedule13d/validation/inspections/"
        f"schedule-13d-activist-continuation-v1-{phase}-input-*.json",
        f"{phase} input inspection",
    )


def _result_file(phase: str) -> Path:
    return _one(
        f"research_results/2026-07-22-{VARIANT_ID}-{phase}-*.json",
        f"{phase} result",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "freeze-contract",
            "inspect-contract",
            "collect",
            "inspect-inputs",
            "evaluate",
            "inspect-result",
        ),
    )
    parser.add_argument("--phase", choices=PHASES)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-contract":
            _published(Path(__file__).resolve())
            value = build_contract()
            path = contract_path(value)
            _write_json(value, path)
            result: dict[str, Any] = {
                "contract_sha256": value["contract_sha256"],
                "rules_hash": value["rules_hash"],
                "phase_denominator": value["phase_denominator"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "market_outcome_access_permitted": False,
            }
        elif args.command == "inspect-contract":
            source = _one(
                "strategy_tournament/v2/schedule13d/validation/contracts/"
                "schedule-13d-activist-continuation-v1-*.json",
                "validation contract",
            )
            value = inspect_contract(source)
            path = INSPECTION_ROOT / f"{VARIANT_ID}-contract-{value['inspection_sha256']}.json"
            _write_json(value, path)
            result = {
                "inspection_sha256": value["inspection_sha256"],
                "development_input_access_permitted": True,
                "confirmation_input_access_permitted": False,
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        else:
            if args.phase not in PHASES:
                raise Schedule13dValidationError("--phase is required")
            phase = args.phase
            if args.command == "collect":
                value = build_activation(phase, write_private=True)
                path = activation_path(value)
                _write_json(value, path)
                result = {
                    "activation_sha256": value["activation_sha256"],
                    "sample_phase": phase,
                    "frozen_signals": value["frozen_signals"],
                    "complete_signals": value["complete_signals"],
                    "incomplete_signals": value["incomplete_signals"],
                    "written": str(path.relative_to(PROJECT_ROOT)),
                    "return_evaluation_permitted": False,
                }
            elif args.command == "inspect-inputs":
                activation = _activation_file(phase)
                value = inspect_inputs(activation, phase)
                path = INSPECTION_ROOT / (
                    f"{VARIANT_ID}-{phase}-input-{value['inspection_sha256']}.json"
                )
                _write_json(value, path)
                result = {
                    "inspection_sha256": value["inspection_sha256"],
                    "sample_phase": phase,
                    "complete_signals": value["complete_signals"],
                    "incomplete_signals": value["incomplete_signals"],
                    "written": str(path.relative_to(PROJECT_ROOT)),
                    "return_evaluation_permitted": True,
                }
            elif args.command == "evaluate":
                activation = _activation_file(phase)
                inspection = _input_inspection_file(phase)
                value = build_result(activation, inspection, phase)
                path = RESULT_ROOT / (
                    f"2026-07-22-{VARIANT_ID}-{phase}-{value['result_sha256']}.json"
                )
                _write_json(value, path)
                result = {
                    "result_sha256": value["result_sha256"],
                    "sample_phase": phase,
                    "closed_signals": value["denominator"]["closed_signals"],
                    "phase_passed": value["phase_passed"],
                    "phase_blockers": value["phase_blockers"],
                    "written": str(path.relative_to(PROJECT_ROOT)),
                }
            else:
                activation = _activation_file(phase)
                inspection = _input_inspection_file(phase)
                result_path = _result_file(phase)
                value = inspect_result(activation, inspection, result_path, phase)
                path = INSPECTION_ROOT / (
                    f"{VARIANT_ID}-{phase}-result-{value['inspection_sha256']}.json"
                )
                _write_json(value, path)
                result = {
                    "inspection_sha256": value["inspection_sha256"],
                    "sample_phase": phase,
                    "phase_passed": value["phase_passed"],
                    "phase_blockers": value["phase_blockers"],
                    "written": str(path.relative_to(PROJECT_ROOT)),
                }
    except (
        Schedule13dValidationError,
        HistoricalProviderError,
        OSError,
        ValueError,
        subprocess.CalledProcessError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
