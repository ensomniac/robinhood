"""Independent entrypoint for the v2 ETF trend Stage 0 inspections."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

import multi_asset_etf_tsmom_stage0 as stage0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    contract = subparsers.add_parser("contract")
    contract.add_argument("contract", type=Path)
    inputs = subparsers.add_parser("inputs")
    inputs.add_argument("activation", type=Path)
    result = subparsers.add_parser("result")
    result.add_argument("activation", type=Path)
    result.add_argument("inspection", type=Path)
    result.add_argument("result", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "contract":
            value = stage0.inspect_contract(args.contract)
        elif args.command == "inputs":
            value = stage0.inspect_inputs(args.activation)
        else:
            value = stage0.inspect_result(
                args.activation, args.inspection, args.result
            )
    except (stage0.MultiAssetEtfTsmomStage0Error, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
