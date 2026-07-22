"""Independent entrypoint for ETF trend capacity input and result inspection."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

import multi_asset_etf_tsmom_capacity_run as capacity_run


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
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
        if args.command == "inputs":
            value = capacity_run.inspect_inputs(args.activation)
        else:
            value = capacity_run.inspect_capacity_result(
                args.activation, args.inspection, args.result
            )
    except (
        capacity_run.MultiAssetEtfTsmomCapacityRunError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
