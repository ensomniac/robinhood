"""Versioned declarative strategy and operation contracts."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from .hashing import canonical_sha256


IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$")
ALLOWED_FEATURES = {
    "return_1d",
    "return_5d",
    "return_20d",
    "gap_1d",
    "range_position_14",
    "volume_ratio_20",
    "atr_pct_14",
    "close_sma_20_ratio",
    "close_sma_50_ratio",
    "market_return_5d",
    "opening_return_30m",
    "opening_range_pct_30m",
}
BOOLEAN_OPERATORS = {"all", "any"}
COMPARISON_OPERATORS = {"gt", "gte", "lt", "lte"}
ALLOWED_RANK_DIRECTIONS = {"asc", "desc"}
ALLOWED_ENTRIES = {"next_open", "next_15m_open"}
ALLOWED_HORIZONS = {"daily", "intraday"}


class ContractError(ValueError):
    """Raised when a public Strategy Lab contract is unsafe or ambiguous."""


class CandidateState(StrEnum):
    DISCOVERED = "DISCOVERED"
    DEVELOPMENT_PASS = "DEVELOPMENT_PASS"
    HISTORICALLY_VALIDATED = "HISTORICALLY_VALIDATED"
    PAPER_ACTIVE = "PAPER_ACTIVE"
    PILOT_READY = "PILOT_READY"
    LIVE_EVALUATING = "LIVE_EVALUATING"
    LIVE_VALIDATED = "LIVE_VALIDATED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class StrategyIdea:
    idea_id: str
    family_id: str
    template: str
    causal_thesis: str
    falsifier: str
    horizon: str
    parameter_bias: str = "balanced"
    source: str = "deterministic"

    def __post_init__(self) -> None:
        for field_name in ("idea_id", "family_id"):
            value = getattr(self, field_name)
            if not IDENTIFIER.fullmatch(value):
                raise ContractError(
                    f"{field_name} must be a stable lowercase identifier"
                )
        if self.horizon not in ALLOWED_HORIZONS:
            raise ContractError(f"unsupported horizon {self.horizon!r}")
        if not self.causal_thesis.strip() or not self.falsifier.strip():
            raise ContractError("ideas require a causal thesis and falsifier")


def _validate_operand(value: Any, *, tunables: list[float]) -> None:
    if not isinstance(value, dict) or len(value) != 1:
        raise ContractError("signal operands must be one-key objects")
    if "feature" in value:
        if value["feature"] not in ALLOWED_FEATURES:
            raise ContractError(f"unsupported feature {value['feature']!r}")
        return
    if "value" in value:
        number = value["value"]
        if isinstance(number, bool) or not isinstance(number, (int, float)):
            raise ContractError("signal constants must be numeric")
        tunables.append(float(number))
        return
    raise ContractError("signal operands must contain feature or value")


def validate_signal(
    node: Any,
    *,
    maximum_depth: int = 4,
    maximum_nodes: int = 12,
    maximum_tunables: int = 4,
) -> None:
    count = 0
    tunables: list[float] = []

    def walk(value: Any, depth: int) -> None:
        nonlocal count
        count += 1
        if count > maximum_nodes:
            raise ContractError("signal exceeds maximum AST nodes")
        if depth > maximum_depth:
            raise ContractError("signal exceeds maximum AST depth")
        if not isinstance(value, dict):
            raise ContractError("signal nodes must be objects")
        op = value.get("op")
        if op in BOOLEAN_OPERATORS:
            args = value.get("args")
            if not isinstance(args, list) or not 2 <= len(args) <= 4:
                raise ContractError(f"{op} requires two to four child nodes")
            for child in args:
                walk(child, depth + 1)
            return
        if op in COMPARISON_OPERATORS:
            _validate_operand(value.get("left"), tunables=tunables)
            _validate_operand(value.get("right"), tunables=tunables)
            return
        raise ContractError(f"unsupported signal operator {op!r}")

    walk(node, 1)
    if len(tunables) > maximum_tunables:
        raise ContractError("signal exceeds maximum tunable values")


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    family_id: str
    idea_id: str
    horizon: str
    signal: dict[str, Any]
    rank_by: str
    rank_direction: str
    entry: str
    stop_loss_pct: float
    target_pct: float
    maximum_hold_sessions: int
    round_trip_bps: int
    causal_thesis: str
    falsifier: str
    parameters: dict[str, float] = field(default_factory=dict)
    provenance: dict[str, Any] = field(default_factory=dict)
    schema_version: int = 1

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ContractError("StrategySpec schema_version must be 1")
        for name in ("strategy_id", "family_id", "idea_id"):
            if not IDENTIFIER.fullmatch(getattr(self, name)):
                raise ContractError(f"{name} must be a stable lowercase identifier")
        if self.horizon not in ALLOWED_HORIZONS:
            raise ContractError(f"unsupported horizon {self.horizon!r}")
        validate_signal(self.signal)
        if self.rank_by not in ALLOWED_FEATURES:
            raise ContractError(f"unsupported rank feature {self.rank_by!r}")
        if self.rank_direction not in ALLOWED_RANK_DIRECTIONS:
            raise ContractError("rank_direction must be asc or desc")
        if self.entry not in ALLOWED_ENTRIES:
            raise ContractError(f"unsupported entry {self.entry!r}")
        if self.horizon == "daily" and self.entry != "next_open":
            raise ContractError("daily strategies must enter at next_open")
        if self.horizon == "intraday" and self.entry != "next_15m_open":
            raise ContractError("intraday strategies must enter at next_15m_open")
        if not 0.001 <= float(self.stop_loss_pct) <= 0.10:
            raise ContractError("stop_loss_pct must be between 0.1% and 10%")
        if not 0.001 <= float(self.target_pct) <= 0.25:
            raise ContractError("target_pct must be between 0.1% and 25%")
        if not 1 <= int(self.maximum_hold_sessions) <= 5:
            raise ContractError("maximum_hold_sessions must be between one and five")
        if not 0 <= int(self.round_trip_bps) <= 100:
            raise ContractError("round_trip_bps must be between zero and 100")
        if not self.causal_thesis.strip() or not self.falsifier.strip():
            raise ContractError("strategy specs require thesis and falsifier")
        if len(self.parameters) > 4:
            raise ContractError("strategy specs may expose at most four parameters")
        if any(
            isinstance(value, bool) or not isinstance(value, (int, float))
            for value in self.parameters.values()
        ):
            raise ContractError("strategy parameters must be numeric")

    @property
    def executable_contract(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "horizon": self.horizon,
            "signal": self.signal,
            "rank_by": self.rank_by,
            "rank_direction": self.rank_direction,
            "entry": self.entry,
            "stop_loss_pct": float(self.stop_loss_pct),
            "target_pct": float(self.target_pct),
            "maximum_hold_sessions": int(self.maximum_hold_sessions),
            "round_trip_bps": int(self.round_trip_bps),
        }

    @property
    def rules_sha256(self) -> str:
        return canonical_sha256(self.executable_contract)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "strategy_id": self.strategy_id,
            "family_id": self.family_id,
            "idea_id": self.idea_id,
            "horizon": self.horizon,
            "signal": self.signal,
            "rank_by": self.rank_by,
            "rank_direction": self.rank_direction,
            "entry": self.entry,
            "stop_loss_pct": float(self.stop_loss_pct),
            "target_pct": float(self.target_pct),
            "maximum_hold_sessions": int(self.maximum_hold_sessions),
            "round_trip_bps": int(self.round_trip_bps),
            "causal_thesis": self.causal_thesis,
            "falsifier": self.falsifier,
            "parameters": dict(sorted(self.parameters.items())),
            "provenance": self.provenance,
            "rules_sha256": self.rules_sha256,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "StrategySpec":
        permitted = {
            "schema_version",
            "strategy_id",
            "family_id",
            "idea_id",
            "horizon",
            "signal",
            "rank_by",
            "rank_direction",
            "entry",
            "stop_loss_pct",
            "target_pct",
            "maximum_hold_sessions",
            "round_trip_bps",
            "causal_thesis",
            "falsifier",
            "parameters",
            "provenance",
            "rules_sha256",
        }
        unknown = sorted(set(value).difference(permitted))
        if unknown:
            raise ContractError(f"unknown StrategySpec fields: {unknown}")
        prepared = dict(value)
        claimed = prepared.pop("rules_sha256", None)
        result = cls(**prepared)
        if claimed is not None and claimed != result.rules_sha256:
            raise ContractError("StrategySpec rules_sha256 does not match its contract")
        return result
