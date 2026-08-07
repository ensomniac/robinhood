"""Select a clean-slate operating mode; every mode forbids broker mutations."""

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
        key="data",
        label="Historical data",
        description="Inspect, fetch, refresh, or migrate canonical historical data.",
        broker_actions_allowed=False,
        next_step=(
            "Select an exact data command, verify LOCAL_HISTORICAL_DATA_ROOT, "
            "and preserve provider provenance."
        ),
    ),
    SessionMode(
        key="account",
        label="Read-only account",
        description="Inspect sanitized account state without orders or mutations.",
        broker_actions_allowed=False,
        next_step=(
            "Discover accounts at runtime, require one unambiguous "
            "agentic_allowed account, and persist no identifier."
        ),
    ),
    SessionMode(
        key="history",
        label="Preserved history",
        description="Audit compact ledgers or inspect the archived repository tree.",
        broker_actions_allowed=False,
        next_step=(
            "Run outcome_exposure.py audit and use the archive tag in a separate "
            "read-only worktree when detailed legacy evidence is needed."
        ),
    ),
    SessionMode(
        key="learning",
        label="Neutral learning",
        description="Bounded engineering and data-quality work with no strategy.",
        broker_actions_allowed=False,
        next_step=(
            "Run learning_loop.py inspect, validate a bounded prompt, and verify "
            "the result with explicit acceptance checks."
        ),
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
    print("Select the clean-slate session mode:")
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
            result = {
                "modes": [asdict(mode) for mode in MODES],
                "live_trading_enabled": False,
            }
        else:
            selected = select_mode(args.mode) if args.mode else prompt_for_mode()
            result = {
                "selected": asdict(selected),
                "selection_only": True,
                "live_trading_enabled": False,
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
