"""Persist and audit the multi-strategy portfolio validation campaign.

The controller composes repository truth and emits one bounded handoff. It does
not contact providers or brokers, change strategy rules, or submit orders.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from learning_data import audit_learning_data
from learning_registry import audit_registries
from learning_strategy import audit_strategy_evidence
from portfolio_funnel import audit_funnel, build_funnel_status
from portfolio_maturity import (
    FIRST_PILOT_MILESTONE,
    audit_ledger as audit_portfolio_ledger,
    build_report as build_portfolio_report,
    load_config as load_portfolio_config,
    read_records as read_portfolio_records,
)
from progress_history import load_history
from strategy_engine import load_config as load_orb_config
from strategy_ledger import audit_ledger as audit_orb_ledger
from strategy_validation import (
    _git_state,
    _load_events as _load_legacy_events,
    _privacy_audit,
    _store_capacity,
)
from trade_lifecycle import audit_lifecycle


PROJECT_ROOT = Path(__file__).resolve().parent
RUN_ROOT = PROJECT_ROOT / "learning_runs" / "portfolio_validation"
PLAN_PATH = PROJECT_ROOT / "PORTFOLIO_VALIDATION.md"
CONFIG_PATH = PROJECT_ROOT / "portfolio_config.toml"
LEDGER_PATH = PROJECT_ROOT / "PORTFOLIO_SIGNALS.jsonl"
SCHEMA_VERSION = 1
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v1"
TERMINAL_PHASE = "THREE_PILOT_READY_LIVE_STARTED"
CAMPAIGN_GOAL = "campaign"
FIRST_PILOT_GOAL = "first-pilot-live-started"
AUDIT_GOALS = {CAMPAIGN_GOAL, FIRST_PILOT_GOAL}
SAFETY_SNAPSHOT_MAX_AGE_SECONDS = 15 * 60

PHASES = (
    "SUPERSESSION",
    "DATA_INVENTORY",
    "TOURNAMENT",
    "DEVELOPMENT",
    "CONFIRMATION",
    "SHADOW_QUALIFICATION",
    "LIVE_PILOT",
    "PORTFOLIO_AUDIT",
    TERMINAL_PHASE,
)
STATUSES = {
    "READY",
    "WAITING_DATA",
    "WAITING_MARKET",
    "WAITING_PROVIDER",
    "WAITING_SUBSCRIPTION",
    "WAITING_NEW_SESSIONS",
    "WAITING_USER_CONFIRMATION",
    "PAUSED_SAFETY",
}
WAITING_STATUSES = STATUSES - {"READY"}
CORE_ARTIFACTS = (
    "AGENTS.md",
    "PORTFOLIO_VALIDATION.md",
    "portfolio_config.toml",
    "portfolio_data_inventory.py",
    "portfolio_funnel.py",
    "second_wave_slate.py",
    "sector_etf_rotation_stage0.py",
    "broad_etf_trend_pullback_stage0.py",
    "close_to_open_etf_momentum_stage0.py",
    "portfolio_guard.py",
    "etf_or_momentum_stage0.py",
    "etf_vwap_mean_reversion_stage0.py",
    "equity_gap_continuation_stage0.py",
    "equity_gap_recovery_stage0.py",
    "volatility_compression_breakout_stage0.py",
    "short_horizon_oversold_reversal_stage0.py",
    "cross_sectional_momentum_stage0.py",
    "post_earnings_drift_stage0.py",
    "relative_strength_continuation_stage0.py",
    "catalyst_orb_retest_stage0.py",
    "portfolio_maturity.py",
    "portfolio_tournament.py",
    "portfolio_validation.py",
    "research_results/2026-07-21-portfolio-data-inventory.json",
    "research_results/2026-07-21-etf-or-momentum-stage0-fe9853712ae52f92b4a88403bdb9270f3f3a8add382a393a2b183529d9cc05d2.json",
    "research_results/2026-07-21-etf-vwap-mean-reversion-stage0-6b3cb4374d858c293176a622e3dc23300d1e09e9a7207a2f487bae700281a5bd.json",
    "research_results/2026-07-21-equity-gap-continuation-stage0-1f447fb4e066b041463e62e26d7e752a12e1b90da7c13b6881ea42e10dda9e94.json",
    "research_results/2026-07-21-equity-gap-recovery-stage0-f0bb51acd548819e91292210b5b59029fa7b4307a37770b084707e1530b5c1bc.json",
    "research_results/2026-07-21-volatility-compression-breakout-stage0-5959e922a6cdaf117521a2a70877984497077f3c79acd5aedf2ac87eea0bbdfe.json",
    "research_results/2026-07-21-short-horizon-oversold-reversal-stage0-bb2152f3f18aef92bac5c7d5dcffc15bdbe2967525e63fa6678c6c553aeb75c6.json",
    "research_results/2026-07-21-cross-sectional-momentum-stage0-9a165ae149b2f9220a2d78c2251a34617ad63ed2a468ded8749fb5b0cc0a8869.json",
    "research_results/2026-07-21-post-earnings-drift-stage0-08f475e3800530d3044ca09948612f61bb6e3fc9f3da8a84aa8c9d30d6f70357.json",
    "research_results/2026-07-21-relative-strength-continuation-stage0-cfe76e6678078f2a5857c8365322215274134a807851014b6babafc90896e0d8.json",
    "research_results/2026-07-21-catalyst-orb-retest-stage0-60d3dd20a62dbff480148ac19aaaa047e036829385dbbe75cba0d044fc558be9.json",
    "research_results/2026-07-21-sector-etf-rotation-stage0-29ea58ac2b950c25ee7f0d17246567daf3eac84a11cddcd9f0f873da08d1a646.json",
    "research_results/2026-07-21-broad-etf-trend-pullback-stage0-2aa31af9eb16c6a268b19df57b5bfc3b9d6950b69fa00cddcbfcb12386a41209.json",
    "strategy_tournament/second_wave/inspections/sector-etf-rotation-v1-result-563155de66061d57761fa18262893d435bb5d6d038bb9760791f92e127d43c20.json",
    "strategy_tournament/second_wave/inspections/broad-etf-trend-pullback-v1-result-384c8372624f14ecc6b92c2a4406830db7061a27474e643f11777800675db8e3.json",
    "strategy_tournament/manifests/portfolio-stage0-slate-1c4f1cd20c490ea56e36fc0b81c30f1c1eb588d9a9dfb310501028de95b3c50f.json",
    "strategy_tournament/second_wave/first-wave-failure-taxonomy-73d54349f24f52f62125fc9fcc7105ffa9b8a38e0771bf1fb994decadfa90292.json",
    "strategy_tournament/second_wave/inspections/first-wave-failure-taxonomy-6f2619090a6312763d16ab9f3e01f63047c59b7fae1907803aea78b0ffa74f58.json",
    "strategy_tournament/second_wave/manifests/portfolio-stage0-second-wave-slate-4c513f9de6417a48dccae5bf5ce1e5c59d68a1d2d0b6e65bba41a3f4a5cac822.json",
    "strategy_tournament/second_wave/inspections/portfolio-stage0-second-wave-slate-3c12ba6f0ea4a49db5888904015a73d42f54cf1da2a43447ece8b741d1e2c6c0.json",
    "strategy_tournament/second_wave/sector_etf_rotation/collection-status.json",
    "strategy_tournament/second_wave/activations/sector-etf-rotation-v1-5360bfd6c12065169975ec828a813f7e6f5042d87979131542b39659ca5df632.json",
    "strategy_tournament/second_wave/inspections/sector-etf-rotation-v1-input-ae9133091afe877306589961ee1e6ad2383d698119a244b8e9e875add1805366.json",
    "strategy_tournament/second_wave/broad_etf_trend_pullback/collection-status.json",
    "strategy_tournament/second_wave/activations/broad-etf-trend-pullback-v1-a4f2348809b00bf70994d3c7868a1bce34b8cec578a5270dc32d80afa1f39860.json",
    "strategy_tournament/second_wave/inspections/broad-etf-trend-pullback-v1-input-83d156b0ea6f6b01bfdf41072fdd4c43c710bb0a734c102ed9cdda22fd83426f.json",
    "strategy_tournament/second_wave/close_to_open_etf_momentum/collection-status.json",
    "strategy_tournament/second_wave/activations/close-to-open-etf-momentum-v1-1e7934f023b26213e085277cb18bb343ddc6f4655aa9cdc8a9bc0096cc95a9c6.json",
    "strategy_tournament/second_wave/inspections/close-to-open-etf-momentum-v1-input-650887a1b75eb05a9588928452c9a1e687d384c66089184ff2273eca60b83b07.json",
    "strategy_tournament/activations/etf-or-momentum-v1-fbea206058e0530a38f89b4b19ccdb71949329960fc6ebbef63ae56966961b03.json",
    "strategy_tournament/activations/etf-vwap-mean-reversion-v1-ecaff3785f2dc9fe3f14332be9e40e056e08aacf55f86487cb2f25df62688a47.json",
    "strategy_tournament/activations/equity-gap-continuation-v1-3bc6f70c2a331e70b00a1dda076b258ce46f1175d062ecee0a641e4c4ab06f3f.json",
    "strategy_tournament/activations/equity-gap-recovery-v1-550a812bdf0e39f272b9c055c1111bef9cffecf13ad7e69c8f5feb1ecca36bfb.json",
    "strategy_tournament/activations/volatility-compression-breakout-v1-3376ff9d2b6af10247e9eee3704f4e3bf612283a42c37a2c0f1b8f680a76d4c0.json",
    "strategy_tournament/activations/short-horizon-oversold-reversal-v1-b65565475be90a9ad981e4e5dd1c598edbd21d5ee21ba9cad3333ca7da8d9d51.json",
    "strategy_tournament/activations/cross-sectional-momentum-v1-a8970ad2bfb0045f54404da985178686ec7cd0068c3a6a23f59165c7e415520a.json",
    "strategy_tournament/activations/post-earnings-drift-v1-d41dd9d9db8531ed90e8cfde0f40382b250ca67177b16f01146e2eab1594f6cf.json",
    "strategy_tournament/activations/post-earnings-drift-v1-1302ec21b91847106830f51dd8e90ee2309290c93d3032f2696a84a55314d9a5.json",
    "strategy_tournament/activations/post-earnings-drift-v1-dbedd0065e72bedbb2bb26f88dd9da5d7c0c42de57bbb8b0fab55dfa2a1697d4.json",
    "strategy_tournament/activations/post-earnings-drift-v1-04dcf6470f6347ede51b6bed481c8ddd389744b7d649be14fbee646d235994ca.json",
    "strategy_tournament/activations/relative-strength-continuation-v1-a22173e0b7ee21a9a5f37e4b7bc84c8863e9733d6c6d670313662a6e2b0a55cb.json",
    "strategy_tournament/activations/relative-strength-continuation-v1-269dcb726208e7f364709a65ed51158c05ccefd02ecc005def0ffb460cc17c8a.json",
    "strategy_tournament/activations/catalyst-orb-retest-v1-c1c7004952ae77ee579b3f33496239783905caccf4aa29637a64528f7805d630.json",
    "strategy_tournament/activations/catalyst-orb-retest-v1-1c95b9338d42023682bd84c31b5168097b6c228425663f91f6affac8772aa873.json",
    "strategy_tournament/cross_sectional_momentum/collection-status.json",
    "strategy_tournament/post_earnings_drift/earnings-status.json",
    "strategy_tournament/post_earnings_drift/market-status.json",
    "strategy_tournament/relative_strength_continuation/prefix-status.json",
    "strategy_tournament/relative_strength_continuation/benchmark-prior-close-status.json",
    "strategy_tournament/relative_strength_continuation/outcome-status.json",
    "strategy_tournament/inspections/equity-gap-continuation-v1-input-fe842aebc174196ceaa34a529d6c3a8eb75ab4df5bc6db110221f7f3dea4c881.json",
    "strategy_tournament/inspections/equity-gap-continuation-v1-result-66ede02688e090973e482ecc9c491349238d14e2af63fd02c336b99a8564c7be.json",
    "strategy_tournament/inspections/equity-gap-recovery-v1-input-6109b4a84007118f27bd6a5e474655b827e8d95515e709f23c5cfb0b95d33b91.json",
    "strategy_tournament/inspections/equity-gap-recovery-v1-result-c2deb307da4528c2f3b6b48d7330c5df8c965a2ca6f04763a5ca2d83fc9a6a12.json",
    "strategy_tournament/inspections/volatility-compression-breakout-v1-input-600ae694412202299505870eb48a14a9819a00f6c88867c3370a27e89c7d8fce.json",
    "strategy_tournament/inspections/volatility-compression-breakout-v1-result-67792983095a62d5811046d8d15a095faa75da7566d7957492d6c52213824f53.json",
    "strategy_tournament/inspections/short-horizon-oversold-reversal-v1-input-0e8796722e06b0643aeb9f6e3001e9e9320730d617619b50884293c006dd46dd.json",
    "strategy_tournament/inspections/short-horizon-oversold-reversal-v1-result-be56bcf7bf8fe2fd16e223f510a8236135fbc39f1a27e7531af9a4649a706e25.json",
    "strategy_tournament/inspections/cross-sectional-momentum-v1-activation-a0f893cb02e31fb445febfacd4e5535a6e5ec602f18d630c06827efbdc84c31f.json",
    "strategy_tournament/inspections/cross-sectional-momentum-v1-input-198957534e2d6ec63519d9e6c96b598ff96f8490f2eece45af282ffc5b9358de.json",
    "strategy_tournament/inspections/cross-sectional-momentum-v1-result-fa04429130158e31d221151b1d00d4dd3432ec43a8c88595dd0f8bc4a3c0421b.json",
    "strategy_tournament/inspections/post-earnings-drift-v1-activation-fad0b77a77d8ae2d2d68197e00a1c25dd7bc189969c3b8db05a514784cd573e5.json",
    "strategy_tournament/inspections/post-earnings-drift-v1-activation-1f0bc5f5a0416ffe09d832be2f1dac2f72d03236ab775e85c4152dd13ebae58e.json",
    "strategy_tournament/inspections/post-earnings-drift-v1-activation-e728d442e0e32c86ccb4447595f4245a195276fd2b9084c85b2e488fe24b3c0e.json",
    "strategy_tournament/inspections/post-earnings-drift-v1-activation-0159e738fe1e50c3248ea1496d560b264419f80077fc906b91d4cb6373c5d5f4.json",
    "strategy_tournament/inspections/post-earnings-drift-v1-input-dfbf41c267099c34fc2e97f88c801b9d6f2b4731c9598cba107c7faef5cbf9cf.json",
    "strategy_tournament/inspections/post-earnings-drift-v1-result-a9efc2d2e2e9454673dd61b03f3037ff429613bde835ee5dd5d920b048d06d7a.json",
    "strategy_tournament/inspections/relative-strength-continuation-v1-activation-3d386ed05e5b616f014169f6a6dfb0088ba146c82579140c66b71463a55586d2.json",
    "strategy_tournament/inspections/relative-strength-continuation-v1-activation-7bfe5fddcbaaba26be35eba3cf35dea00d617a6654464998cc499f62f161f992.json",
    "strategy_tournament/inspections/relative-strength-continuation-v1-prefix-554a3e4678175f32140642ad98ea9959a5bd87ab65aca5636ba92119a8fe6e9e.json",
    "strategy_tournament/inspections/relative-strength-continuation-v1-input-91fa69d7dea4555c579c1d0d71beb8a3a3176d684d2e5d5e0552098792a36f43.json",
    "strategy_tournament/inspections/relative-strength-continuation-v1-result-342e84c3668952a6d1e1dc5ea9b4d391648efebf6030ccb2674a7060077f0d35.json",
    "strategy_tournament/inspections/catalyst-orb-retest-v1-input-126b7dbb0a67202b1f5e6e8f888ff42e5ab7fa087e683ff5549c8fd6da06e6dd.json",
    "strategy_tournament/inspections/catalyst-orb-retest-v1-input-039752b9f53336360151dda965bb931e41ab6e1ef1636107ba49faf7e4e1832e.json",
    "strategy_tournament/inspections/catalyst-orb-retest-v1-result-caefe1a76c8b17c94bc412dd6cbd24f3f5906d9d4be29cb7e5e3b34596a8e278.json",
    "strategy_tournament/inspections/etf-vwap-mean-reversion-v1-input-deae004f10d53c1753435155385fc0557b6c39249f43896725bf1237dd32eaa0.json",
    "strategy_tournament/inspections/etf-vwap-mean-reversion-v1-result-faef3c6c57e36f8604179e596c959bd3f7c982848552ad224ef371da7c963176.json",
    "strategy_tournament/inspections/etf-or-momentum-v1-input-6736b37133fd8ba9d46c86e76ef3d7e21a2a09541f1ad631d4ec8b94ff78b1c8.json",
    "strategy_tournament/inspections/etf-or-momentum-v1-result-9db03d7d7e497917e6a4ee663ffee68506bd72a0050a873a903906ff4715fa6b.json",
    "PRODUCTION_STRATEGY_VALIDATION.md",
    "strategy_validation.py",
    "strategy_config.toml",
    "SIGNALS.jsonl",
    "learning/DATASETS.jsonl",
    "learning/EXPERIMENTS.jsonl",
    "learning/STRATEGIES.jsonl",
)
ALLOWED_TRANSITIONS = {
    "SUPERSESSION": {"SUPERSESSION", "DATA_INVENTORY"},
    "DATA_INVENTORY": {"DATA_INVENTORY", "TOURNAMENT"},
    "TOURNAMENT": {"TOURNAMENT", "DATA_INVENTORY", "DEVELOPMENT"},
    "DEVELOPMENT": {
        "DEVELOPMENT",
        "DATA_INVENTORY",
        "TOURNAMENT",
        "CONFIRMATION",
        "LIVE_PILOT",
    },
    "CONFIRMATION": {
        "CONFIRMATION",
        "TOURNAMENT",
        "DEVELOPMENT",
        "SHADOW_QUALIFICATION",
        "LIVE_PILOT",
    },
    "SHADOW_QUALIFICATION": {
        "SHADOW_QUALIFICATION",
        "TOURNAMENT",
        "DEVELOPMENT",
        "CONFIRMATION",
        "LIVE_PILOT",
    },
    "LIVE_PILOT": {
        "LIVE_PILOT",
        "TOURNAMENT",
        "DEVELOPMENT",
        "CONFIRMATION",
        "SHADOW_QUALIFICATION",
        "PORTFOLIO_AUDIT",
    },
    "PORTFOLIO_AUDIT": {
        "PORTFOLIO_AUDIT",
        "TOURNAMENT",
        "DEVELOPMENT",
        "CONFIRMATION",
        "SHADOW_QUALIFICATION",
        "LIVE_PILOT",
    },
    TERMINAL_PHASE: {TERMINAL_PHASE},
}
PHASE_HANDOFFS: dict[str, dict[str, str]] = {
    "SUPERSESSION": {
        "objective": "bind-supersession-without-rewriting-legacy-evidence",
        "action": "Inspect the preserved production campaign and record that it is SUPERSEDED_PAUSED only as the sole production path.",
        "command": "python3 strategy_validation.py status",
    },
    "DATA_INVENTORY": {
        "objective": "map-representative-point-in-time-data-capacity",
        "action": "Inspect one bounded provider/cache slice and record its universe, timestamps, corporate actions, outcome locks, fidelity, and disk reserve without reading unfrozen outcomes.",
        "command": "python3 historical_data_cli.py check",
    },
    "TOURNAMENT": {
        "objective": "falsify-one-preregistered-mechanism-variant",
        "action": "Freeze and evaluate one still-untried Stage 0 variant, retain its trial identity and failures, and do not tune it on confirmation data.",
        "command": "python3 learning_experiment.py audit",
    },
    "DEVELOPMENT": {
        "objective": "build-one-representative-development-slice",
        "action": "For one surviving version, freeze or resume one representative development cohort and inspect executable cost-stressed outcomes with all denominators retained.",
        "command": "python3 portfolio_maturity.py audit",
    },
    "CONFIRMATION": {
        "objective": "evaluate-one-untouched-confirmation-slice",
        "action": "Freeze or resume one chronologically separated confirmation slice after the five-session embargo and apply the exact unchanged strategy contract.",
        "command": "python3 portfolio_maturity.py audit",
    },
    "SHADOW_QUALIFICATION": {
        "objective": "collect-one-complete-prospective-shadow-session",
        "action": "During an eligible session, run one frozen candidate's complete scanner, decision, risk, order-construction, protection, monitoring, and journal path without an order.",
        "command": "python3 session_mode.py --mode shadow",
    },
    "LIVE_PILOT": {
        "objective": "run-one-compliant-pilot-ready-live-handoff",
        "action": "During an eligible session, select only an independently PILOT_READY strategy and follow every account, risk, review, confirmation, protection, monitoring, and journal gate in AGENTS.md.",
        "command": "python3 session_mode.py --mode live",
    },
    "PORTFOLIO_AUDIT": {
        "objective": "recompute-three-strategy-portfolio-milestone",
        "action": "Refresh privacy-safe broker safety state, reconcile evidence and lifecycle audits, commit and push public artifacts, then rerun the controller audit.",
        "command": "python3 portfolio_validation.py audit",
    },
    TERMINAL_PHASE: {
        "objective": "campaign-milestone-earned",
        "action": "Continue controlled live pilots and degradation monitoring; LIVE_VALIDATED remains strategy-specific and evidence-earned.",
        "command": "python3 portfolio_validation.py status",
    },
}


class PortfolioValidationError(RuntimeError):
    """The campaign state, transition, or terminal evidence is unsafe."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise PortfolioValidationError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioValidationError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PortfolioValidationError(f"{path} must contain an object")
    return value


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise PortfolioValidationError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PortfolioValidationError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise PortfolioValidationError(f"{field} must include a timezone")
    return parsed


def _nonempty(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PortfolioValidationError(f"{field} must be non-empty")
    return value.strip()


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise PortfolioValidationError(f"{field} must be a nonnegative number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioValidationError(f"{field} must be a nonnegative number") from exc
    if result < 0 or result == float("inf") or result != result:
        raise PortfolioValidationError(f"{field} must be a nonnegative number")
    return result


def _artifact_hashes(root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in CORE_ARTIFACTS:
        path = root / relative
        if not path.is_file():
            raise PortfolioValidationError(f"required artifact is missing: {relative}")
        result[relative] = _sha256_file(path)
    return result


def _optional_file_hash(path: Path) -> str:
    return _sha256_file(path) if path.is_file() else hashlib.sha256(b"").hexdigest()


def _legacy_campaign_binding(root: Path) -> dict[str, Any]:
    events_path = root / "learning_runs" / "production_validation" / "events.jsonl"
    events = _load_legacy_events(events_path)
    if not events:
        raise PortfolioValidationError("preserved legacy campaign event log is missing")
    current = events[-1]
    return {
        "campaign_id": str(current["campaign_id"]),
        "sequence": int(current["sequence"]),
        "phase": str(current["phase"]),
        "status": str(current["status"]),
        "event_sha256": str(current["event_sha256"]),
        "events_file_sha256": _sha256_file(events_path),
    }


def _strategy_bindings(report: Mapping[str, Any]) -> list[dict[str, str]]:
    return [
        {
            "strategy_id": str(item["strategy_id"]),
            "strategy_version": str(item["strategy_version"]),
            "mechanism_family": str(item["mechanism_family"]),
            "rules_hash": str(item["rules_hash"]),
            "maturity": str(item["maturity"]),
        }
        for item in sorted(
            report["strategies"],
            key=lambda value: (value["strategy_id"], value["strategy_version"]),
        )
    ]


def _lineage(root: Path) -> dict[str, Any]:
    config = load_portfolio_config(root / "portfolio_config.toml")
    records = read_portfolio_records(root / "PORTFOLIO_SIGNALS.jsonl", root=root)
    report = build_portfolio_report(records, config)
    return {
        "plan_sha256": _sha256_file(root / "PORTFOLIO_VALIDATION.md"),
        "portfolio_config_sha256": config.sha256,
        "portfolio_ledger_sha256": _optional_file_hash(
            root / "PORTFOLIO_SIGNALS.jsonl"
        ),
        "upstream_artifact_hashes": _artifact_hashes(root),
        "strategy_bindings": _strategy_bindings(report),
        "legacy_campaign_binding": _legacy_campaign_binding(root),
    }


def _validate_safety_snapshot(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PortfolioValidationError("safety snapshot must be an object")
    snapshot = dict(value)
    allowed = {
        "schema_version",
        "observed_at",
        "broker_state",
        "account_reconciled",
        "orders_reconciled",
        "positions_count",
        "protected_positions_count",
        "open_orders_count",
        "unknown_orders_count",
        "unprotected_positions_count",
        "new_entries_today",
        "gross_notional_fraction",
        "aggregate_planned_open_loss_fraction",
        "daily_loss_fraction",
        "weekly_loss_fraction",
        "peak_to_trough_drawdown_fraction",
        "source",
    }
    extras = sorted(set(snapshot) - allowed)
    if extras:
        raise PortfolioValidationError(
            f"safety snapshot contains forbidden fields: {extras}"
        )
    if snapshot.get("schema_version") != 1:
        raise PortfolioValidationError("safety snapshot schema_version must be 1")
    _timestamp(snapshot.get("observed_at"), "safety_snapshot.observed_at")
    if snapshot.get("broker_state") not in {
        "FLAT_RECONCILED",
        "PROTECTED_EXPOSURE_RECONCILED",
        "UNPROTECTED_EXPOSURE",
        "UNKNOWN",
    }:
        raise PortfolioValidationError("invalid safety snapshot broker_state")
    for field in ("account_reconciled", "orders_reconciled"):
        if not isinstance(snapshot.get(field), bool):
            raise PortfolioValidationError(f"safety snapshot {field} must be boolean")
    for field in (
        "positions_count",
        "protected_positions_count",
        "open_orders_count",
        "unknown_orders_count",
        "unprotected_positions_count",
        "new_entries_today",
    ):
        current = snapshot.get(field)
        if isinstance(current, bool) or not isinstance(current, int) or current < 0:
            raise PortfolioValidationError(f"safety snapshot {field} must be nonnegative")
    for field in (
        "gross_notional_fraction",
        "aggregate_planned_open_loss_fraction",
        "daily_loss_fraction",
        "weekly_loss_fraction",
        "peak_to_trough_drawdown_fraction",
    ):
        snapshot[field] = _number(snapshot.get(field), f"safety_snapshot.{field}")
    snapshot["source"] = _nonempty(snapshot.get("source"), "safety_snapshot.source")
    return snapshot


def _evidence_hashes(paths: Sequence[Path], root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for supplied in paths:
        path = supplied if supplied.is_absolute() else root / supplied
        try:
            relative = str(path.resolve().relative_to(root.resolve()))
        except ValueError as exc:
            raise PortfolioValidationError(
                f"campaign evidence must be inside the repository: {path}"
            ) from exc
        if not path.is_file():
            raise PortfolioValidationError(f"campaign evidence is not a file: {relative}")
        result[relative] = _sha256_file(path)
    return dict(sorted(result.items()))


def validate_phase_transition(previous: str, current: str) -> None:
    if previous not in ALLOWED_TRANSITIONS or current not in ALLOWED_TRANSITIONS[previous]:
        raise PortfolioValidationError(
            f"unsafe campaign phase transition {previous} -> {current}"
        )


def _event_hash(event: Mapping[str, Any]) -> str:
    payload = {key: value for key, value in event.items() if key != "event_sha256"}
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _validate_event(
    value: Any, *, previous_hash: str, expected_sequence: int
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PortfolioValidationError("campaign event must be an object")
    event = dict(value)
    if event.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioValidationError("campaign event schema_version must be 1")
    if event.get("campaign_id") != CAMPAIGN_ID:
        raise PortfolioValidationError("campaign event has the wrong campaign_id")
    if event.get("sequence") != expected_sequence:
        raise PortfolioValidationError("campaign event sequence is not contiguous")
    _timestamp(event.get("recorded_at"), "event.recorded_at")
    if event.get("previous_event_sha256") != previous_hash:
        raise PortfolioValidationError("campaign event hash chain is broken")
    if event.get("event_sha256") != _event_hash(event):
        raise PortfolioValidationError("campaign event content hash is invalid")
    transition_kind = event.get("transition_kind")
    milestone = event.get("milestone")
    if transition_kind == "MILESTONE":
        if milestone != FIRST_PILOT_MILESTONE:
            raise PortfolioValidationError("campaign milestone event is invalid")
    elif milestone is not None:
        raise PortfolioValidationError("only milestone events may name a milestone")
    phase = event.get("phase")
    if phase not in PHASES:
        raise PortfolioValidationError(f"invalid campaign phase {phase!r}")
    status = event.get("status")
    if status not in STATUSES:
        raise PortfolioValidationError(f"invalid campaign status {status!r}")
    _nonempty(event.get("active_objective"), "event.active_objective")
    if phase != TERMINAL_PHASE:
        _nonempty(event.get("next_action"), "event.next_action")
    if status in WAITING_STATUSES:
        _nonempty(event.get("blocker"), "event.blocker")
    for field in (
        "plan_sha256",
        "portfolio_config_sha256",
        "portfolio_ledger_sha256",
    ):
        _nonempty(event.get(field), f"event.{field}")
    if event.get("superseded_campaign_status") != "SUPERSEDED_PAUSED":
        raise PortfolioValidationError("legacy campaign supersession status drifted")
    if not isinstance(event.get("upstream_artifact_hashes"), Mapping):
        raise PortfolioValidationError("event lacks upstream artifact hashes")
    if not isinstance(event.get("strategy_bindings"), list):
        raise PortfolioValidationError("event strategy_bindings must be a list")
    if not isinstance(event.get("legacy_campaign_binding"), Mapping):
        raise PortfolioValidationError("event lacks legacy campaign binding")
    if not isinstance(event.get("evidence_hashes"), Mapping):
        raise PortfolioValidationError("event evidence_hashes must be an object")
    if event.get("safety_snapshot") is not None:
        event["safety_snapshot"] = _validate_safety_snapshot(event["safety_snapshot"])
    return event


def _load_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    previous_hash = ""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise PortfolioValidationError(f"cannot read campaign events: {exc}") from exc
    for index, line in enumerate(lines, 1):
        if not line.strip():
            raise PortfolioValidationError(f"campaign event line {index} is blank")
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PortfolioValidationError(
                f"campaign event line {index} is invalid JSON: {exc.msg}"
            ) from exc
        event = _validate_event(value, previous_hash=previous_hash, expected_sequence=index)
        if events and event.get("transition_kind") != "TERMINAL":
            validate_phase_transition(str(events[-1]["phase"]), str(event["phase"]))
        events.append(event)
        previous_hash = str(event["event_sha256"])
    return events


def _atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as target:
            target.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _project_state(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": CAMPAIGN_ID,
        "current_event_sha256": event["event_sha256"],
        "sequence": event["sequence"],
        "phase": event["phase"],
        "status": event["status"],
        "active_objective": event["active_objective"],
        "blocker": event["blocker"],
        "next_action": event["next_action"],
        "plan_sha256": event["plan_sha256"],
        "portfolio_config_sha256": event["portfolio_config_sha256"],
        "portfolio_ledger_sha256": event["portfolio_ledger_sha256"],
        "legacy_campaign_event_sha256": event["legacy_campaign_binding"][
            "event_sha256"
        ],
        "superseded_campaign_status": event["superseded_campaign_status"],
        "updated_at": event["recorded_at"],
    }


def _load_current(run_root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events = _load_events(run_root / "events.jsonl")
    if not events:
        raise PortfolioValidationError(
            "campaign is not initialized; run portfolio_validation.py init"
        )
    state = _read_json(run_root / "state.json")
    projected = _project_state(events[-1])
    if state != projected:
        raise PortfolioValidationError("campaign state projection does not match event log")
    return events, events[-1]


def _append_event(
    *,
    root: Path,
    run_root: Path,
    phase: str,
    status: str,
    objective: str,
    blocker: str,
    next_action: str,
    transition_kind: str,
    evidence_paths: Sequence[Path] = (),
    safety_snapshot: Mapping[str, Any] | None = None,
    milestone: str | None = None,
) -> dict[str, Any]:
    if phase not in PHASES or status not in STATUSES:
        raise PortfolioValidationError("invalid campaign phase or status")
    objective = _nonempty(objective, "active objective")
    if phase != TERMINAL_PHASE:
        next_action = _nonempty(next_action, "next action")
    if status in WAITING_STATUSES:
        blocker = _nonempty(blocker, "blocker")
    lineage = _lineage(root)
    run_root.mkdir(parents=True, exist_ok=True)
    with (run_root / ".lock").open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        events_path = run_root / "events.jsonl"
        events = _load_events(events_path)
        if events and transition_kind != "TERMINAL":
            validate_phase_transition(str(events[-1]["phase"]), phase)
        previous_hash = str(events[-1]["event_sha256"]) if events else ""
        if safety_snapshot is None and events:
            safety_snapshot = events[-1].get("safety_snapshot")
        event: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "sequence": len(events) + 1,
            "recorded_at": datetime.now(UTC).isoformat(),
            "transition_kind": transition_kind,
            **({"milestone": milestone} if milestone is not None else {}),
            "phase": phase,
            "status": status,
            "active_objective": objective,
            "blocker": blocker.strip(),
            "next_action": next_action.strip(),
            "superseded_campaign_status": "SUPERSEDED_PAUSED",
            **lineage,
            "evidence_hashes": _evidence_hashes(evidence_paths, root),
            "safety_snapshot": (
                _validate_safety_snapshot(safety_snapshot)
                if safety_snapshot is not None
                else None
            ),
            "previous_event_sha256": previous_hash,
        }
        event["event_sha256"] = _event_hash(event)
        _validate_event(
            event,
            previous_hash=previous_hash,
            expected_sequence=len(events) + 1,
        )
        with events_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        _atomic_write_json(run_root / "state.json", _project_state(event))
    return event


def initialize_campaign(
    *, root: Path = PROJECT_ROOT, run_root: Path = RUN_ROOT
) -> dict[str, Any]:
    if (run_root / "events.jsonl").exists():
        _, current = _load_current(run_root)
        return {"initialized": True, "idempotent": True, "state": _project_state(current)}
    event = _append_event(
        root=root,
        run_root=run_root,
        phase="SUPERSESSION",
        status="READY",
        objective=PHASE_HANDOFFS["SUPERSESSION"]["objective"],
        blocker="",
        next_action=PHASE_HANDOFFS["SUPERSESSION"]["action"],
        transition_kind="INITIALIZED",
        evidence_paths=(
            root / "PRODUCTION_STRATEGY_VALIDATION.md",
            root / "PORTFOLIO_VALIDATION.md",
        ),
    )
    return {"initialized": True, "idempotent": False, "state": _project_state(event)}


def _authoritative_snapshot(root: Path) -> dict[str, Any]:
    portfolio_config = load_portfolio_config(root / "portfolio_config.toml")
    portfolio_records = read_portfolio_records(root / "PORTFOLIO_SIGNALS.jsonl", root=root)
    portfolio_audit = audit_portfolio_ledger(root / "PORTFOLIO_SIGNALS.jsonl", root=root)
    portfolio_report = build_portfolio_report(portfolio_records, portfolio_config)
    funnel = build_funnel_status(portfolio_report, root=root)
    registry_root = root / "learning"
    orb_config = load_orb_config(root / "strategy_config.toml")
    orb_ledger = audit_orb_ledger(root / "SIGNALS.jsonl", orb_config)
    lifecycle = audit_lifecycle(
        active_root=root / "trades" / "active",
        archive_root=root / "trades" / "archived",
        config=orb_config,
    )
    return {
        "lineage": _lineage(root),
        "portfolio_config": portfolio_config.raw,
        "portfolio_ledger_audit": portfolio_audit,
        "portfolio_report": portfolio_report,
        "funnel": funnel,
        "funnel_audit": audit_funnel(portfolio_report, root=root),
        "registry_audit": audit_registries(registry_root),
        "strategy_audit": audit_strategy_evidence(registry_root),
        "learning_data_audit": audit_learning_data(
            registry_root=registry_root,
            security_path=registry_root / "SECURITY_MASTER.jsonl",
        ),
        "orb_ledger_audit": {**asdict(orb_ledger), "valid": orb_ledger.valid},
        "lifecycle_audit": {**asdict(lifecycle), "valid": lifecycle.valid},
        "privacy_audit": _privacy_audit(root),
        "progress_audit": {
            "valid": True,
            "entries": len(load_history(root / "progress" / "HISTORY.jsonl")),
        },
        "store_capacity": _store_capacity(root),
        "git": _git_state(root),
    }


def _safety_snapshot_blockers(
    snapshot: Mapping[str, Any] | None,
    config: Mapping[str, Any],
    *,
    now: datetime | None = None,
    require_flat: bool = False,
) -> list[str]:
    if snapshot is None:
        return ["fresh privacy-safe broker safety snapshot is missing"]
    value = _validate_safety_snapshot(snapshot)
    blockers: list[str] = []
    age = (
        (now or datetime.now(UTC))
        - _timestamp(value["observed_at"], "safety_snapshot.observed_at").astimezone(UTC)
    ).total_seconds()
    if age < 0 or age > SAFETY_SNAPSHOT_MAX_AGE_SECONDS:
        blockers.append("broker safety snapshot is stale")
    if value["broker_state"] not in {
        "FLAT_RECONCILED",
        "PROTECTED_EXPOSURE_RECONCILED",
    }:
        blockers.append("broker state is not safely reconciled")
    if require_flat and value["broker_state"] != "FLAT_RECONCILED":
        blockers.append("broker state is not flat and reconciled")
    if require_flat and (
        value["positions_count"] != 0 or value["open_orders_count"] != 0
    ):
        blockers.append("broker snapshot retains positions or open orders")
    if value["account_reconciled"] is not True:
        blockers.append("broker account is not reconciled")
    if value["orders_reconciled"] is not True:
        blockers.append("broker orders are not reconciled")
    if value["unknown_orders_count"]:
        blockers.append("broker safety snapshot has unknown orders")
    if value["unprotected_positions_count"]:
        blockers.append("broker safety snapshot has unprotected positions")
    if value["protected_positions_count"] != value["positions_count"]:
        blockers.append("not every open position is counted as protected")
    portfolio = config["portfolio"]
    risk = config["pilot_risk"]
    limits = {
        "positions_count": portfolio["maximum_concurrent_positions"],
        "new_entries_today": portfolio["maximum_new_entries_per_day"],
        "gross_notional_fraction": risk["maximum_gross_notional_fraction"],
        "aggregate_planned_open_loss_fraction": risk[
            "maximum_aggregate_planned_open_loss_fraction"
        ],
        "daily_loss_fraction": risk["maximum_daily_loss_fraction"],
        "weekly_loss_fraction": risk["maximum_weekly_loss_fraction"],
        "peak_to_trough_drawdown_fraction": risk[
            "maximum_peak_to_trough_drawdown_fraction"
        ],
    }
    for field, maximum in limits.items():
        if value[field] > maximum:
            blockers.append(f"broker safety snapshot {field} exceeds {maximum}")
    return blockers


def _lineage_blockers(current: Mapping[str, Any], lineage: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    for field in (
        "plan_sha256",
        "portfolio_config_sha256",
        "portfolio_ledger_sha256",
        "upstream_artifact_hashes",
        "strategy_bindings",
        "legacy_campaign_binding",
    ):
        if current.get(field) != lineage.get(field):
            blockers.append(f"campaign {field} binding is stale")
    return blockers


def _finalization_blockers(
    current: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> list[str]:
    blockers: list[str] = []
    report = snapshot["portfolio_report"]
    if report["earned_milestone"] != TERMINAL_PHASE:
        blockers.extend(str(value) for value in report["milestone_blockers"])
    for field in (
        "portfolio_ledger_audit",
        "funnel_audit",
        "registry_audit",
        "strategy_audit",
        "learning_data_audit",
        "orb_ledger_audit",
        "lifecycle_audit",
        "privacy_audit",
        "progress_audit",
    ):
        if snapshot[field].get("valid") is not True:
            blockers.append(f"{field} is not valid")
    if snapshot["store_capacity"].get("capacity_ready") is not True:
        blockers.append("historical store capacity reserve is not ready")
    blockers.extend(_lineage_blockers(current, snapshot["lineage"]))
    blockers.extend(
        _safety_snapshot_blockers(
            current.get("safety_snapshot"),
            snapshot["portfolio_config"],
        )
    )
    git = snapshot["git"]
    if git.get("valid") is not True:
        blockers.append("Git state is unavailable")
    else:
        if git.get("clean") is not True:
            blockers.append("Git worktree is not clean")
        if git.get("head_equals_upstream") is not True:
            blockers.append("HEAD does not equal its configured upstream")
    return list(dict.fromkeys(blockers))


def _interim_goal_blockers(
    current: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> list[str]:
    blockers: list[str] = []
    report = snapshot["portfolio_report"]
    if FIRST_PILOT_MILESTONE not in report.get("earned_interim_milestones", []):
        blockers.extend(
            str(value) for value in report.get("interim_milestone_blockers", [])
        )
    for field in (
        "portfolio_ledger_audit",
        "funnel_audit",
        "registry_audit",
        "strategy_audit",
        "learning_data_audit",
        "orb_ledger_audit",
        "lifecycle_audit",
        "privacy_audit",
        "progress_audit",
    ):
        if snapshot[field].get("valid") is not True:
            blockers.append(f"{field} is not valid")
    if snapshot["store_capacity"].get("capacity_ready") is not True:
        blockers.append("historical store capacity reserve is not ready")
    blockers.extend(_lineage_blockers(current, snapshot["lineage"]))
    blockers.extend(
        _safety_snapshot_blockers(
            current.get("safety_snapshot"),
            snapshot["portfolio_config"],
            require_flat=True,
        )
    )
    git = snapshot["git"]
    if git.get("valid") is not True:
        blockers.append("Git state is unavailable")
    else:
        if git.get("clean") is not True:
            blockers.append("Git worktree is not clean")
        if git.get("head_equals_upstream") is not True:
            blockers.append("HEAD does not equal its configured upstream")
    return list(dict.fromkeys(blockers))


def _recorded_interim_milestones(
    events: Sequence[Mapping[str, Any]],
) -> list[str]:
    return list(
        dict.fromkeys(
            str(event["milestone"])
            for event in events
            if event.get("transition_kind") == "MILESTONE"
            and event.get("milestone") == FIRST_PILOT_MILESTONE
        )
    )


def campaign_status(
    *, root: Path = PROJECT_ROOT, run_root: Path = RUN_ROOT
) -> dict[str, Any]:
    events, current = _load_current(run_root)
    authoritative = _authoritative_snapshot(root)
    recorded_interim = _recorded_interim_milestones(events)
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": CAMPAIGN_ID,
        "terminal": current["phase"] == TERMINAL_PHASE,
        "events": len(events),
        "state": _project_state(current),
        "lineage_blockers": _lineage_blockers(current, authoritative["lineage"]),
        "portfolio_report": authoritative["portfolio_report"],
        "funnel": authoritative["funnel"],
        "store_capacity": authoritative["store_capacity"],
        "safety_snapshot": current.get("safety_snapshot"),
        "earned_interim_milestones": recorded_interim,
        "first_pilot_goal_complete": (
            FIRST_PILOT_MILESTONE in recorded_interim
        ),
        "first_pilot_goal_blockers": (
            []
            if FIRST_PILOT_MILESTONE in recorded_interim
            else _interim_goal_blockers(current, authoritative)
        ),
        "finalization_blockers": _finalization_blockers(current, authoritative),
    }


def record_transition(
    *,
    phase: str,
    status: str,
    objective: str,
    blocker: str,
    next_action: str,
    evidence_paths: Sequence[Path],
    safety_path: Path | None,
    root: Path = PROJECT_ROOT,
    run_root: Path = RUN_ROOT,
) -> dict[str, Any]:
    if phase == TERMINAL_PHASE:
        raise PortfolioValidationError(
            f"{TERMINAL_PHASE} cannot be recorded manually; run portfolio_validation.py audit"
        )
    _, current = _load_current(run_root)
    validate_phase_transition(str(current["phase"]), phase)
    snapshot = _read_json(safety_path) if safety_path is not None else None
    event = _append_event(
        root=root,
        run_root=run_root,
        phase=phase,
        status=status,
        objective=objective,
        blocker=blocker,
        next_action=next_action,
        transition_kind="RECORDED",
        evidence_paths=evidence_paths,
        safety_snapshot=snapshot,
    )
    return {"recorded": True, "state": _project_state(event)}


def next_handoff(
    *, root: Path = PROJECT_ROOT, run_root: Path = RUN_ROOT
) -> dict[str, Any]:
    _, current = _load_current(run_root)
    lineage = _lineage(root)
    drift = _lineage_blockers(current, lineage)
    phase = str(current["phase"])
    handoff = PHASE_HANDOFFS[phase]
    action = str(current["next_action"] or handoff["action"])
    if drift:
        action = (
            "Inspect and record the authorized plan, implementation, or strategy-evidence "
            "binding change before resuming the current phase: " + "; ".join(drift)
        )
    return {
        "campaign_id": CAMPAIGN_ID,
        "phase": phase,
        "status": current["status"],
        "active_objective": current["active_objective"],
        "blocker": current["blocker"],
        "bounded_handoff": {
            **handoff,
            "action": action,
            "perform_by_controller": False,
            "waiting_state_must_be_resolved_first": current["status"] in WAITING_STATUSES,
        },
    }


def audit_campaign(
    *,
    goal: str = CAMPAIGN_GOAL,
    root: Path = PROJECT_ROOT,
    run_root: Path = RUN_ROOT,
) -> dict[str, Any]:
    if goal not in AUDIT_GOALS:
        raise PortfolioValidationError(f"unsupported audit goal {goal!r}")
    events, current = _load_current(run_root)
    authoritative = _authoritative_snapshot(root)
    recorded_interim = _recorded_interim_milestones(events)
    interim_already_earned = FIRST_PILOT_MILESTONE in recorded_interim
    blockers = (
        _finalization_blockers(current, authoritative)
        if goal == CAMPAIGN_GOAL
        else (
            []
            if interim_already_earned
            else _interim_goal_blockers(current, authoritative)
        )
    )
    controller_valid = not (current["phase"] == TERMINAL_PHASE and blockers)
    finalized = False
    milestone_recorded_now = False
    if (
        goal == CAMPAIGN_GOAL
        and not blockers
        and current["phase"] != TERMINAL_PHASE
    ):
        event = _append_event(
            root=root,
            run_root=run_root,
            phase=TERMINAL_PHASE,
            status="READY",
            objective=PHASE_HANDOFFS[TERMINAL_PHASE]["objective"],
            blocker="",
            next_action="",
            transition_kind="TERMINAL",
            safety_snapshot=current.get("safety_snapshot"),
        )
        current = event
        events.append(event)
        finalized = True
    elif goal == FIRST_PILOT_GOAL and not blockers and not interim_already_earned:
        event = _append_event(
            root=root,
            run_root=run_root,
            phase=str(current["phase"]),
            status="READY",
            objective="first-pilot-ready-live-started-earned",
            blocker="",
            next_action=(
                "Continue the controlled portfolio campaign toward "
                "THREE_PILOT_READY_LIVE_STARTED and strategy-specific LIVE_VALIDATED."
            ),
            transition_kind="MILESTONE",
            safety_snapshot=current.get("safety_snapshot"),
            milestone=FIRST_PILOT_MILESTONE,
        )
        current = event
        events.append(event)
        recorded_interim.append(FIRST_PILOT_MILESTONE)
        milestone_recorded_now = True
    return {
        "valid": controller_valid,
        "campaign_id": CAMPAIGN_ID,
        "events": len(events),
        "state": _project_state(current),
        "terminal": current["phase"] == TERMINAL_PHASE,
        "finalized_now": finalized,
        "goal": goal,
        "goal_complete": (
            current["phase"] == TERMINAL_PHASE
            if goal == CAMPAIGN_GOAL
            else FIRST_PILOT_MILESTONE in recorded_interim
        ),
        "earned_interim_milestones": recorded_interim,
        "milestone_recorded_now": milestone_recorded_now,
        "finalization_blockers": blockers,
        "integrity": {
            key: authoritative[key]
            for key in (
                "portfolio_ledger_audit",
                "funnel_audit",
                "registry_audit",
                "strategy_audit",
                "learning_data_audit",
                "orb_ledger_audit",
                "lifecycle_audit",
                "privacy_audit",
                "progress_audit",
                "store_capacity",
                "git",
            )
        },
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="initialize or resume the persistent campaign")
    subparsers.add_parser("status", help="compose authoritative campaign status")
    subparsers.add_parser("next", help="emit one bounded handoff")
    record = subparsers.add_parser("record", help="append one evidence-bound transition")
    record.add_argument("--phase", choices=PHASES[:-1], required=True)
    record.add_argument("--status", choices=sorted(STATUSES), required=True)
    record.add_argument("--objective", required=True)
    record.add_argument("--blocker", default="")
    record.add_argument("--next-action", required=True)
    record.add_argument("--evidence", action="append", type=Path, default=[])
    record.add_argument("--safety-snapshot", type=Path)
    audit = subparsers.add_parser(
        "audit", help="audit and machine-finalize only if earned"
    )
    audit.add_argument(
        "--goal",
        choices=sorted(AUDIT_GOALS),
        default=CAMPAIGN_GOAL,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "init":
            result = initialize_campaign()
        elif args.command == "status":
            result = campaign_status()
        elif args.command == "next":
            result = next_handoff()
        elif args.command == "record":
            result = record_transition(
                phase=args.phase,
                status=args.status,
                objective=args.objective,
                blocker=args.blocker,
                next_action=args.next_action,
                evidence_paths=args.evidence,
                safety_path=args.safety_snapshot,
            )
        else:
            result = audit_campaign(goal=args.goal)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("valid", True) else 1
    except Exception as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
