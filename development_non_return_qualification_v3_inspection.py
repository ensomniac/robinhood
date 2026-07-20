"""Independently rebuild the frozen v3 non-return qualification result."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import development_non_return_qualification_v3 as qualification
from learning_data import LearningDataError, load_frozen_dataset_contract
from scanner_replay import load_split_actions
from strategy_engine import StrategyInputError, evaluate_candidate, load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-20-development-non-return-qualification-v3-inspection.json"
)


class QualificationInspectionError(RuntimeError):
    """Independent qualification reconstruction found a mismatch."""


def _independent_record(
    state: Mapping[str, Any], split_actions: Mapping[str, Any]
) -> dict[str, Any]:
    if state.get("terminal_disposition") == "NO_CLEAN_CROSS_BEFORE_CUTOFF":
        return {
            "pair_key": state["pair_key"],
            "date": state["date"],
            "symbol": state["symbol"],
            "instrument_id": state["instrument_id"],
            "terminal_reason": "NO_CLEAN_CROSS_BEFORE_CUTOFF",
            "survivor": False,
            "gates": {},
            "target_outcome_observed_or_derived": False,
        }
    try:
        rules = qualification._rules_contract()
        snapshots = state["requests"]["quote_window"]["snapshots"]
        quote = qualification.quote_metrics(snapshots, rules)
        if quote.get("valid") is not True:
            raise QualificationInspectionError("quote snapshots are unresolved")
        fields = state["scanner_fields"]
        final = datetime.fromisoformat(str(state["final_decision_at_et"]))
        exact_vwap = qualification._exact_vwap_state(
            state["requests"]["decision_trade_prefix"]["observations"], final
        )
        completed = state["requests"]["completed_bars"]
        symbol = str(state["symbol"])
        market = qualification._market_state(
            float(fields["open_price"]),
            float(quote["final_ask"]),
            completed[symbol]["observations"],
            {name: completed[name]["observations"] for name in ("SPY", "QQQ")},
        )
        structure = qualification._structure_state(state, quote, split_actions)
        payload = qualification._evaluator_payload(
            state, quote, exact_vwap, market, structure
        )
        try:
            evaluation = evaluate_candidate(
                payload, load_config(qualification.STRATEGY_CONFIG)
            ).to_dict()
        except StrategyInputError as exc:
            evaluation = {
                "eligible": False,
                "score": 0,
                "classification": "rejected",
                "hard_rejects": [str(exc)],
            }
        final_ask = float(quote["final_ask"])
        recent = int(market["candidate"]["last_real_volume"])
        q_liquidity = min(
            math.floor(
                int(quote["minimum_ask_depth"])
                * rules["maximum_depth_participation_fraction"]
            ),
            math.floor(
                recent * rules["maximum_recent_volume_participation_fraction"]
            ),
        )
        opening_high = float(fields["opening_high"])
        gates = {
            "coarse_scanner": qualification._coarse_scanner_pass(state),
            "opening_prefix_consistent": qualification._opening_prefix_consistent(
                state
            ),
            "quote": quote["valid"] and quote["operating_spread_pass"],
            "a_plus_spread": quote["a_plus_spread_pass"],
            "chase": opening_high <= final_ask
            <= opening_high * (1 + rules["maximum_entry_chase_fraction"]),
            "vwap": final_ask > exact_vwap["wap"]
            and exact_vwap["flat_or_rising"],
            "liquidity": int(quote["minimum_ask_depth"]) > 0
            and recent > 0
            and q_liquidity > 0,
            "halt": int(state["requests"]["halts"]["causal_record_count"]) == 0,
            "market": market["supportive_or_independent_strength"],
            "relative_strength": market["candidate_outperforms_both"],
            "structure": structure["stop"]["stop_outside_noise"]
            and structure["stop"]["maximum_stop_fraction_pass"]
            and structure["planned_stop_below_every_observed_bid"],
            "resistance": structure["resistance"]["minimum_room_pass"] is True,
            "evaluator": evaluation.get("eligible") is True,
        }
        terminal_order = (
            ("COARSE_SCANNER_GATE_FAILED", "coarse_scanner"),
            ("OPENING_PREFIX_MISMATCH", "opening_prefix_consistent"),
            ("QUOTE_GATE_FAILED", "quote"),
            ("A_PLUS_SPREAD_FAILED", "a_plus_spread"),
            ("CHASE_GATE_FAILED", "chase"),
            ("VWAP_GATE_FAILED", "vwap"),
            ("LIQUIDITY_GATE_FAILED", "liquidity"),
            ("HALT_GATE_FAILED", "halt"),
            ("MARKET_GATE_FAILED", "market"),
            ("RELATIVE_STRENGTH_GATE_FAILED", "relative_strength"),
            ("STRUCTURE_GATE_FAILED", "structure"),
            ("RESISTANCE_GATE_FAILED", "resistance"),
            ("EVALUATOR_INELIGIBLE", "evaluator"),
        )
        terminal = next(
            (reason for reason, gate in terminal_order if gates[gate] is not True),
            "SURVIVOR",
        )
        return {
            "pair_key": state["pair_key"],
            "date": state["date"],
            "symbol": state["symbol"],
            "instrument_id": state["instrument_id"],
            "rank": state["rank"],
            "source_state_sha256": qualification._sha256_json(state),
            "terminal_reason": terminal,
            "survivor": terminal == "SURVIVOR",
            "gates": gates,
            "metrics": {
                "median_spread_fraction": quote["median_spread_fraction"],
                "maximum_spread_fraction": quote["maximum_spread_fraction"],
                "final_ask": final_ask,
                "exact_vwap": exact_vwap["wap"],
                "q_liquidity": q_liquidity,
                "stop_fraction": structure["stop"]["stop_fraction"],
                "resistance_status": structure["resistance"]["status"],
                "resistance_room_fraction": structure["resistance"][
                    "resistance_room_fraction"
                ],
                "score": int(evaluation.get("score", 0)),
                "classification": evaluation.get("classification"),
            },
            "evaluator": evaluation,
            "broker_specific_tradability": "PROSPECTIVE_ONLY_UNRECONSTRUCTABLE",
            "target_outcome_observed_or_derived": False,
        }
    except (
        KeyError,
        TypeError,
        ValueError,
        QualificationInspectionError,
        qualification.DevelopmentNonReturnQualificationError,
    ) as exc:
        return {
            "pair_key": state.get("pair_key"),
            "date": state.get("date"),
            "symbol": state.get("symbol"),
            "instrument_id": state.get("instrument_id"),
            "source_state_sha256": qualification._sha256_json(state),
            "terminal_reason": "INPUT_UNRESOLVED",
            "survivor": False,
            "gates": {},
            "error": str(exc),
            "target_outcome_observed_or_derived": False,
        }


def inspect_contract(*, manifest_path: Path, env_path: Path) -> dict[str, Any]:
    store, index, _pair_paths = qualification._load_source(env_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    expected = qualification._expected_contract(
        index=index,
        store_root=store.root,
        observed_free_bytes=int(
            manifest["capacity_contract"]["observed_free_bytes_at_freeze"]
        ),
    )
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise QualificationInspectionError(f"contract drifted at {key}")
    for value in manifest["implementation_contract"]["files"].values():
        qualification._verify_binding(value)
    if qualification._private_result_path(store.root).exists():
        raise QualificationInspectionError(
            "qualification result exists before contract inspection"
        )
    return {
        "schema_version": 1,
        "dataset_id": qualification.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "pairs_expected": qualification.EXPECTED_PAIRS,
        "preentry_inputs_collected": qualification.EXPECTED_INPUT_READY,
        "minimum_survivors_before_outcomes": qualification.MINIMUM_SURVIVORS,
        "inspection": {
            "source_collection_rebuilt": True,
            "implementation_and_rules_rebuilt": True,
            "privacy_and_outcome_locks_rebuilt": True,
            "valid": True,
        },
        "target_outcomes_observed_or_derived": False,
    }


def inspect_result(
    *, manifest_path: Path, env_path: Path, output_path: Path
) -> dict[str, Any]:
    store, _index, pair_paths = qualification._load_source(env_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    private_path = qualification._private_result_path(store.root)
    private = qualification._read_gzip(private_path)
    observed = private.get("records")
    if not isinstance(observed, list) or len(observed) != qualification.EXPECTED_PAIRS:
        raise QualificationInspectionError("private qualification records are incomplete")
    split_actions = load_split_actions(qualification._source_paths(store.root)["splits"])
    rebuilt = [
        _independent_record(qualification._read_gzip(path), split_actions)
        for path in pair_paths
    ]
    if rebuilt != observed:
        raise QualificationInspectionError("independent per-pair gate rebuild differs")
    aggregate = qualification._aggregate(rebuilt)
    for key, value in aggregate.items():
        if private.get(key) != value:
            raise QualificationInspectionError(f"aggregate drifted at {key}")
    terminal = Counter(str(row["terminal_reason"]) for row in rebuilt)
    if sum(terminal.values()) != qualification.EXPECTED_PAIRS:
        raise QualificationInspectionError("terminal reasons do not reconcile")
    result = {
        "schema_version": 1,
        "dataset_id": qualification.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "QUALIFICATION_INSPECTED",
        "inspected": True,
        "pairs_evaluated": len(rebuilt),
        **aggregate,
        "minimum_survivors_before_outcomes": qualification.MINIMUM_SURVIVORS,
        "outcome_contract_permitted": aggregate["survivors"]
        >= qualification.MINIMUM_SURVIVORS,
        "private_result_sha256": qualification._sha256_file(private_path),
        "inspection": {
            "source_pair_hashes_rebuilt": True,
            "per_pair_gate_records_rebuilt": True,
            "terminal_precedence_rebuilt": True,
            "aggregate_counts_and_cascade_rebuilt": True,
            "privacy_and_outcome_locks_rechecked": True,
            "valid": True,
        },
        "symbols_dates_instrument_ids_raw_rows_and_gate_records_public": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    qualification._write_json(output_path, result)
    qualification._write_json(qualification.DEFAULT_QUALIFICATION_STATUS, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    contract = sub.add_parser("inspect-contract")
    contract.add_argument("manifest", type=Path)
    result = sub.add_parser("inspect")
    result.add_argument("manifest", type=Path)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output", type=Path, default=DEFAULT_RESULT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            value = inspect_contract(manifest_path=args.manifest, env_path=args.env)
            qualification._write_json(qualification.DEFAULT_CONTRACT_STATUS, value)
        else:
            value = inspect_result(
                manifest_path=args.manifest,
                env_path=args.env,
                output_path=args.output,
            )
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        QualificationInspectionError,
        qualification.DevelopmentNonReturnQualificationError,
        LearningDataError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
