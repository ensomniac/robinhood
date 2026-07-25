"""Inspect the combined ASR tradable-capacity disposition."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_combined_capacity as combined


class AsrCombinedCapacityInspectionError(RuntimeError):
    """The combined ASR disposition does not independently rebuild."""


def inspect(
    path: Path,
    *,
    output_root: Path = combined.DEFAULT_ROOT / "inspections",
) -> tuple[Path, dict[str, Any]]:
    recorded = combined._read(path)
    rebuilt = combined.build_result()
    if not (
        recorded == rebuilt
        and recorded.get("result_sha256") == combined._hash(recorded, "result_sha256")
        and recorded.get("verified_agreement_count") == 185
        and recorded.get("independent_disclosure_signal_count") == 89
        and recorded.get("capacity_disposition")
        == "PRESERVED_LATER_SINGLE_RULE_RESEARCH"
        and recorded.get("development_search_contract_freeze_permitted") is False
        and recorded.get("market_price_access_permitted") is False
        and recorded.get("market_outcomes_accessed") is False
    ):
        raise AsrCombinedCapacityInspectionError(
            "combined ASR capacity does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-combined-capacity-inspection",
        "campaign_id": recorded["campaign_id"],
        "candidate_id": recorded["candidate_id"],
        "result_sha256": recorded["result_sha256"],
        "source_tier_lineage_rebuilt": True,
        "accession_disjointness_rebuilt": True,
        "agreement_count_rebuilt": True,
        "independent_signal_count_rebuilt": True,
        "capacity_disposition_rebuilt": True,
        "verified_agreement_count": 185,
        "independent_disclosure_signal_count": 89,
        "capacity_disposition": "PRESERVED_LATER_SINGLE_RULE_RESEARCH",
        "development_search_contract_freeze_permitted": False,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = combined._hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / f"asr-combined-{result['inspection_sha256']}.json"
    output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return output, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect",))
    parser.add_argument("path", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        output, result = inspect(args.path)
    except (AsrCombinedCapacityInspectionError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(
        json.dumps(
            {**result, "written": str(output.relative_to(combined.PROJECT_ROOT))},
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
