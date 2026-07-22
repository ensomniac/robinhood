"""Build and audit a non-activating successor portfolio-campaign proposal."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import portfolio_funnel
import portfolio_maturity
import strategy_ledger
from learning_registry import current_entities


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
PROPOSAL_ID = "multi-strategy-portfolio-validation-v2-proposal"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "strategy_tournament/successor_proposal"
QUALIFICATION_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-22-challenger-orb-retest-entry-qualification.json"
)
QUALIFICATION_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "retest-qualification-status.json"
)
BOUND_EVIDENCE = (
    "PORTFOLIO_SIGNALS.jsonl",
    "SIGNALS.jsonl",
    "learning/EXPERIMENTS.jsonl",
    "learning/STRATEGIES.jsonl",
    "research_results/2026-07-22-challenger-orb-retest-entry-qualification.json",
    "historical_batches/challenger_orb_retest_v1_tranche3/"
    "retest-qualification-status.json",
)


class PortfolioSuccessorProposalError(RuntimeError):
    """The non-activating successor proposal or its exhaustion evidence differs."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise PortfolioSuccessorProposalError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioSuccessorProposalError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PortfolioSuccessorProposalError(f"{path} must contain an object")
    return value


def _self_hash(value: Mapping[str, Any]) -> str:
    return _hash_json(
        {key: item for key, item in value.items() if key != "proposal_sha256"}
    )


def _legacy_maturity() -> dict[str, Any]:
    records = strategy_ledger.read_records(strategy_ledger.DEFAULT_LEDGER_PATH)
    return strategy_ledger.build_report(records)["maturity"]


def _assert_exhaustion() -> dict[str, Any]:
    maturity = portfolio_maturity.build_report()
    funnel = portfolio_funnel.build_funnel_status(maturity)
    result = _read_json(QUALIFICATION_RESULT)
    status = _read_json(QUALIFICATION_STATUS)
    experiments = current_entities("experiments")
    strategies = current_entities("strategies")
    experiment = experiments.get("experiment-catalyst-orb-retest-v1", {})
    challenger = strategies.get("strategy-catalyst-orb-retest-v1", {})
    legacy = _legacy_maturity()
    lanes = funnel.get("lanes")
    if not isinstance(lanes, Mapping):
        raise PortfolioSuccessorProposalError("portfolio lanes are missing")
    if not (
        funnel["first_wave"]["complete"] is True
        and funnel["first_wave"]["disposed_count"] == 10
        and funnel["first_wave"]["survivor_count"] == 1
        and funnel["validation_candidates"][0]["status"]
        == "RETIRED_DEVELOPMENT"
        and funnel["second_wave"]["complete"] is True
        and funnel["second_wave"]["disposed_count"] == 6
        and funnel["second_wave"]["survivor_count"] == 0
        and all(value is None for value in lanes.values())
        and maturity["earned_milestone"] == "RESEARCH"
        and maturity["pilot_ready_strategy_count"] == 0
        and maturity["live_started_strategy_count"] == 0
        and result == status
        and result.get("status") == "INSUFFICIENT_CAPACITY"
        and result.get("inspected") is True
        and result.get("eligible_signals") == 2
        and result.get("minimum_eligible_signals") == 50
        and result.get("outcome_contract_permitted") is False
        and result.get("post_entry_data_access_allowed") is False
        and result.get("target_outcomes_observed_or_derived") is False
        and experiment.get("event_type") == "retired"
        and experiment.get("payload", {}).get("status") == "FAILED"
        and challenger.get("event_type") == "retired"
        and challenger.get("payload", {}).get("alpha_state") == "RETIRED"
        and legacy["earned_maturity"] == "UNVALIDATED"
        and legacy["metrics"]["closed_signals"] == 0
    ):
        raise PortfolioSuccessorProposalError(
            "authorized campaign exhaustion does not rebuild"
        )
    return {
        "first_wave_dispositions": 10,
        "first_wave_stage0_survivors": 1,
        "retired_development_survivors": 1,
        "second_wave_dispositions": 6,
        "second_wave_survivors": 0,
        "preserved_challenger_eligible_signals": 2,
        "preserved_challenger_minimum_signals": 50,
        "active_stage0_lanes": 0,
        "active_development_lanes": 0,
        "active_confirmation_or_shadow_lanes": 0,
        "pilot_ready_strategies": 0,
        "live_started_strategies": 0,
        "legacy_orb_closed_signals": 0,
    }


def _candidate_themes() -> list[dict[str, Any]]:
    return [
        {
            "priority": 1,
            "theme_id": "multi-asset-etf-time-series-momentum",
            "mechanism": (
                "own-price persistence across liquid unlevered ETFs representing "
                "distinct conventional asset classes"
            ),
            "why_distinct": (
                "time-series rather than cross-sectional ranking, sector rotation, "
                "opening-range breakout, or pullback entry"
            ),
            "primary_or_research_basis": [
                "https://doi.org/10.1093/rof/rfw048"
            ],
            "known_transfer_risk": (
                "published evidence uses longer horizons and broader instruments; "
                "a long-only five-session implementation may have no edge"
            ),
            "data_feasibility": (
                "daily OHLCV and corporate-action-safe ETF histories already fit "
                "the repository's cheapest deterministic data path"
            ),
            "state": "THEME_ONLY_NOT_PREREGISTERED",
        },
        {
            "priority": 2,
            "theme_id": "schedule-13d-activist-continuation",
            "mechanism": (
                "delayed price discovery after a newly public control-intent "
                "beneficial-ownership filing"
            ),
            "why_distinct": (
                "ownership and control-intent event rather than earnings, generic "
                "news, gap structure, or insider Form 4 activity"
            ),
            "primary_or_research_basis": [
                "https://www.sec.gov/rules-regulations/2023/10/33-11180",
                "https://www.nber.org/papers/w23522",
            ],
            "known_transfer_risk": (
                "filing latency and pre-filing information leakage may leave no "
                "post-publication five-session edge after costs"
            ),
            "data_feasibility": (
                "SEC requires structured machine-readable Schedule 13D filings; "
                "historical point-in-time parsing and purpose classification remain "
                "to be capacity-tested"
            ),
            "state": "THEME_ONLY_NOT_PREREGISTERED",
        },
        {
            "priority": 3,
            "theme_id": "accelerated-share-repurchase-continuation",
            "mechanism": (
                "short-horizon continuation after a higher-commitment accelerated "
                "repurchase announcement"
            ),
            "why_distinct": (
                "issuer capital-allocation commitment rather than price-only trend, "
                "earnings drift, or ordinary open-market repurchase announcements"
            ),
            "primary_or_research_basis": [
                "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1107217"
            ],
            "known_transfer_risk": (
                "announcement response may be immediate, event capacity may be low, "
                "and the five-session residual edge is unproven"
            ),
            "data_feasibility": (
                "issuer releases and SEC filings can provide causal event timestamps, "
                "but exact ASR classification must be independently inspected"
            ),
            "state": "THEME_ONLY_NOT_PREREGISTERED",
        },
    ]


def build_proposal() -> dict[str, Any]:
    exhaustion = _assert_exhaustion()
    evidence = {
        path: _hash_file(PROJECT_ROOT / path) for path in BOUND_EVIDENCE
    }
    proposal: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "successor-campaign-proposal",
        "proposal_id": PROPOSAL_ID,
        "proposal_date": "2026-07-22",
        "objective": "FIRST_PILOT_READY_LIVE_STARTED",
        "authorization_state": "REQUIRED_NOT_GRANTED",
        "campaign_activation_permitted": False,
        "candidate_preregistration_permitted": False,
        "provider_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "current_campaign_exhaustion": exhaustion,
        "requested_superseding_authority": {
            "maximum_new_mechanism_families_per_iso_week": 3,
            "long_common_equities_and_etfs_only": True,
            "maximum_concurrent_positions": 3,
            "maximum_new_entries_per_day": 5,
            "maximum_holding_sessions": 5,
            "may_create_new_exact_versions": True,
            "may_repair_failed_variants_on_their_evaluation_corpora": False,
            "may_relabel_prior_stage0_or_failed_evidence": False,
            "may_weaken_promotion_or_risk_gates": False,
            "end_goal_unchanged": "FIRST_PILOT_READY_LIVE_STARTED",
        },
        "required_authorization_text": (
            "Authorize superseding portfolio research campaign v2 with up to three "
            "new mechanism families per ISO week, preserving all existing evidence, "
            "anti-tuning, privacy, broker-safety, promotion, and risk gates, until "
            "FIRST_PILOT_READY_LIVE_STARTED is machine-earned."
        ),
        "admission_gates_before_any_outcome_access": [
            "MECHANISM_IS_NOT_A_PARAMETER_REPAIR_OR_RELABELING_OF_A_FAILED_VARIANT",
            "POINT_IN_TIME_INPUTS_AND_CAUSAL_TIMESTAMPS_ARE_RECONSTRUCTABLE",
            "CAPACITY_SUPPORTS_AT_LEAST_30_STAGE0_AND_50_TOTAL_HISTORICAL_SIGNALS",
            "EXACT_RULES_UNIVERSE_DATES_FILLS_EXITS_COSTS_AND_FALSIFIERS_ARE_FROZEN",
            "INDEPENDENT_ZERO_RESULT_INSPECTION_PASSES",
            "PUBLIC_IDENTITIES_REMAIN_AGGREGATE_OR_HASH_ONLY",
        ],
        "candidate_themes": _candidate_themes(),
        "sequencing_after_authorization": [
            "CHEAP_OUTCOME_BLIND_CAPACITY_PREFLIGHT",
            "FREEZE_ONE_EXACT_STAGE0_VARIANT",
            "INDEPENDENT_ZERO_RESULT_INSPECTION",
            "STAGE0_EVALUATION_AND_INDEPENDENT_DISPOSITION",
            "REPRESENTATIVE_DEVELOPMENT_IF_SURVIVED",
            "UNTOUCHED_CONFIRMATION_AFTER_EMBARGO",
            "FIVE_PROSPECTIVE_SHADOWS",
            "PORTFOLIO_MATURITY_AWARD_OF_PILOT_READY",
            "ONE_CONTROLLED_LIVE_EXECUTION_AND_FLAT_RECONCILIATION",
            "FIRST_PILOT_READY_LIVE_STARTED_AUDIT",
        ],
        "implementation_sha256": _hash_file(Path(__file__)),
        "evidence_hashes": dict(sorted(evidence.items())),
        "claim_limit": (
            "This artifact is a proposal only. It freezes no strategy rule, date, "
            "universe, signal, outcome, or campaign activation and grants no authority."
        ),
    }
    proposal["proposal_sha256"] = _self_hash(proposal)
    return proposal


def write_proposal(output_root: Path = DEFAULT_OUTPUT_ROOT) -> tuple[Path, dict[str, Any]]:
    proposal = build_proposal()
    path = output_root / f"{PROPOSAL_ID}-{proposal['proposal_sha256']}.json"
    if path.exists() and _read_json(path) != proposal:
        raise PortfolioSuccessorProposalError(
            "content-addressed successor proposal has other content"
        )
    output_root.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(proposal, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path, proposal


def audit_proposal(path: Path) -> dict[str, Any]:
    observed = _read_json(path)
    expected = build_proposal()
    if observed != expected:
        raise PortfolioSuccessorProposalError(
            "successor proposal does not rebuild from current exhaustion evidence"
        )
    if not (
        observed.get("proposal_sha256") == _self_hash(observed)
        and path.name
        == f"{PROPOSAL_ID}-{observed['proposal_sha256']}.json"
        and observed.get("authorization_state") == "REQUIRED_NOT_GRANTED"
        and observed.get("campaign_activation_permitted") is False
        and observed.get("candidate_preregistration_permitted") is False
        and observed.get("provider_access_permitted") is False
        and observed.get("outcome_access_permitted") is False
        and observed.get("broker_actions_permitted") is False
        and all(
            item.get("state") == "THEME_ONLY_NOT_PREREGISTERED"
            for item in observed.get("candidate_themes", [])
        )
    ):
        raise PortfolioSuccessorProposalError(
            "successor proposal activation boundary is invalid"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "proposal_id": PROPOSAL_ID,
        "proposal_sha256": observed["proposal_sha256"],
        "authorization_state": observed["authorization_state"],
        "candidate_themes": len(observed["candidate_themes"]),
        "evidence_files_verified": len(observed["evidence_hashes"]),
        "campaign_activation_permitted": False,
        "provider_requests": 0,
        "outcomes_accessed": 0,
        "broker_actions": 0,
        "valid": True,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("build")
    audit = subparsers.add_parser("audit")
    audit.add_argument("proposal", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "build":
            path, proposal = write_proposal()
            result: dict[str, Any] = {
                "path": str(path.relative_to(PROJECT_ROOT)),
                **proposal,
            }
        else:
            result = audit_proposal(args.proposal)
    except (PortfolioSuccessorProposalError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
