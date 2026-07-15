"""Select an explicit operating mode for a new agentic trading session.

Mode selection is declarative.  It does not fetch broker data, place orders,
start a replay, or apply a strategy proposal.  The selected workflow must still
run every mode-specific safety and confirmation check in ``AGENTS.md``.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from typing import Sequence


class ModeSelectionError(ValueError):
    """Raised when a session mode is missing or invalid."""


@dataclass(frozen=True)
class SessionMode:
    key: str
    label: str
    description: str
    broker_actions_allowed: bool
    next_step: str


MODES = (
    SessionMode(
        key="live",
        label="Live trading",
        description="Current-day research and authorized real-money equity execution.",
        broker_actions_allowed=True,
        next_step="Run encryption, lifecycle, ledger, account, order, maturity, and session-guard checks before discovery.",
    ),
    SessionMode(
        key="shadow",
        label="Shadow trading",
        description="Current-day market research and simulated decisions with no live orders.",
        broker_actions_allowed=False,
        next_step="Create today's shadow session context and run the normal discovery/evaluation loop without order tools.",
    ),
    SessionMode(
        key="historical",
        label="Historical learning",
        description="Random unarchived-day point-in-time replay with no broker actions.",
        broker_actions_allowed=False,
        next_step="Ask how many days to simulate, collect and validate replay bundles, then run historical_learning.py.",
    ),
    SessionMode(
        key="review",
        label="Strategy review",
        description="Read-only evidence report and cadence-gated change proposal.",
        broker_actions_allowed=False,
        next_step="Run strategy_learning.py report; write a proposal only when cadence gates pass.",
    ),
)
MODE_BY_KEY = {mode.key: mode for mode in MODES}


def select_mode(value: str | int) -> SessionMode:
    """Resolve a numeric menu choice or stable mode key."""
    if isinstance(value, int) or (isinstance(value, str) and value.strip().isdigit()):
        number = int(value)
        if 1 <= number <= len(MODES):
            return MODES[number - 1]
        raise ModeSelectionError(f"mode number must be between 1 and {len(MODES)}")
    if not isinstance(value, str):
        raise ModeSelectionError("mode must be a menu number or mode key")
    key = value.strip().lower()
    try:
        return MODE_BY_KEY[key]
    except KeyError as exc:
        raise ModeSelectionError(
            f"unknown mode {value!r}; choose one of {tuple(MODE_BY_KEY)}"
        ) from exc


def prompt_for_mode() -> SessionMode:
    print("Select the agentic session mode:")
    for index, mode in enumerate(MODES, 1):
        print(f"  {index}. {mode.label} — {mode.description}")
    while True:
        try:
            choice = input("Mode: ")
        except EOFError as exc:
            raise ModeSelectionError("mode selection requires input") from exc
        try:
            return select_mode(choice)
        except ModeSelectionError as exc:
            print(f"Invalid selection: {exc}", file=sys.stderr)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=tuple(MODE_BY_KEY),
        help="select non-interactively; omit to show the numbered picker",
    )
    parser.add_argument(
        "--list", action="store_true", help="print all modes without selecting one"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.list:
            result = {"modes": [asdict(mode) for mode in MODES]}
        else:
            selected = select_mode(args.mode) if args.mode else prompt_for_mode()
            result = {
                "selected": asdict(selected),
                "selection_only": True,
                "safety_checks_still_required": True,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except ModeSelectionError as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
