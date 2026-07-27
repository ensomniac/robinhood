"""Capped autonomous idea generation and deterministic unique-spec expansion."""

from __future__ import annotations

import itertools
import json
import os
from datetime import date
from typing import Any, Iterable, Iterator
from uuid import uuid4

from .config import LabConfig
from .contracts import ContractError, StrategyIdea, StrategySpec
from .database import LabDatabase, utc_now
from .hashing import canonical_json, canonical_sha256


class GenerationError(RuntimeError):
    """Raised when the daily specification floor cannot be produced safely."""


TEMPLATES = {
    "short_horizon_reversal",
    "gap_reversal",
    "trend_pullback",
    "medium_term_momentum",
    "range_breakout",
    "volatility_contraction_breakout",
    "market_regime_momentum",
    "relative_strength_continuation",
    "opening_reversal",
    "opening_momentum",
}


def fallback_ideas() -> list[StrategyIdea]:
    rows = [
        (
            "short-horizon-reversal",
            "short_horizon_reversal",
            "Short-term liquidity pressure overshoots and partially mean reverts.",
            "Net expectancy is non-positive after 20 bps or wins depend on the best five trades.",
            "daily",
        ),
        (
            "gap-reversal",
            "gap_reversal",
            "Large overnight gaps without sustained demand partially retrace.",
            "Gap cohorts do not retain positive selection-adjusted expectancy.",
            "daily",
        ),
        (
            "trend-pullback",
            "trend_pullback",
            "Liquid leaders resume established trends after bounded one-session pullbacks.",
            "Pullback entries underperform unconditional trend continuation after costs.",
            "daily",
        ),
        (
            "medium-term-momentum",
            "medium_term_momentum",
            "Persistent information diffusion creates short medium-term continuation.",
            "Continuation disappears under chronological folds or market-regime controls.",
            "daily",
        ),
        (
            "range-breakout",
            "range_breakout",
            "High-volume closes near a rolling range extreme predict follow-through.",
            "Breakouts fail to clear stressed profit-factor and drawdown gates.",
            "daily",
        ),
        (
            "volatility-contraction-breakout",
            "volatility_contraction_breakout",
            "Compressed volatility followed by range pressure precedes directional expansion.",
            "Expansion does not persist after next-open execution and costs.",
            "daily",
        ),
        (
            "market-regime-momentum",
            "market_regime_momentum",
            "Individual momentum is more durable during a supportive broad-market regime.",
            "Conditioning on market trend does not improve stable net account growth.",
            "daily",
        ),
        (
            "relative-strength-continuation",
            "relative_strength_continuation",
            "Cross-sectional leaders continue outperforming after liquidity filtering.",
            "Relative-strength ranks are unstable or rely on concentrated winners.",
            "daily",
        ),
        (
            "opening-reversal",
            "opening_reversal",
            "Early regular-session dislocations retrace after the first two completed bars.",
            "The post-10:00 path does not overcome stop-first execution and costs.",
            "intraday",
        ),
        (
            "opening-momentum",
            "opening_momentum",
            "Demand persisting through two completed opening bars continues intraday.",
            "Opening strength is exhausted before a causal post-10:00 entry.",
            "intraday",
        ),
    ]
    return [
        StrategyIdea(
            idea_id=f"idea-{slug}",
            family_id=f"family-{slug}-v1",
            template=template,
            causal_thesis=thesis,
            falsifier=falsifier,
            horizon=horizon,
        )
        for slug, template, thesis, falsifier, horizon in rows
    ]


IDEA_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["ideas"],
    "properties": {
        "ideas": {
            "type": "array",
            "minItems": 8,
            "maxItems": 20,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "family_slug",
                    "template",
                    "causal_thesis",
                    "falsifier",
                    "parameter_bias",
                ],
                "properties": {
                    "family_slug": {
                        "type": "string",
                        "pattern": "^[a-z0-9][a-z0-9-]{2,48}$",
                    },
                    "template": {"type": "string", "enum": sorted(TEMPLATES)},
                    "causal_thesis": {
                        "type": "string",
                        "minLength": 20,
                        "maxLength": 400,
                    },
                    "falsifier": {"type": "string", "minLength": 20, "maxLength": 400},
                    "parameter_bias": {
                        "type": "string",
                        "enum": ["conservative", "balanced", "aggressive"],
                    },
                },
            },
        }
    },
}


class IdeaGenerator:
    def __init__(self, config: LabConfig, database: LabDatabase) -> None:
        self.config = config
        self.database = database

    def _spent_today(self, research_date: date) -> float:
        row = self.database.connection.execute(
            "SELECT coalesce(sum(estimated_cost_usd), 0) FROM idea_usage WHERE research_date = ?",
            [research_date],
        ).fetchone()
        return float(row[0])

    def generate(
        self, *, research_date: date
    ) -> tuple[list[StrategyIdea], dict[str, Any]]:
        settings = self.config.section("idea_generation")
        if not bool(settings["enabled"]) or not os.getenv("OPENAI_API_KEY"):
            return fallback_ideas(), {
                "source": "deterministic_fallback",
                "reason": "OpenAI generation disabled or OPENAI_API_KEY unavailable",
                "estimated_cost_usd": 0.0,
            }
        spent = self._spent_today(research_date)
        budget = float(settings["daily_budget_usd"])
        if spent >= budget:
            return fallback_ideas(), {
                "source": "deterministic_fallback",
                "reason": "daily OpenAI budget exhausted",
                "estimated_cost_usd": 0.0,
            }

        from openai import OpenAI

        prompt = canonical_json(
            {
                "objective": (
                    "Propose distinct causal long-only U.S. common-stock or ETF strategy "
                    "mechanisms for daily or post-10:00 intraday testing. Favor net geometric "
                    "growth after costs and mechanisms capable of winning more often than losing."
                ),
                "constraints": {
                    "templates": sorted(TEMPLATES),
                    "maximum_hold_sessions": 5,
                    "no_holdout_outcomes": True,
                    "no_broker_or_account_data": True,
                    "no_generated_code": True,
                    "ideas": "8 to 20",
                },
                "adverse_history": (
                    "Many prior momentum, reversal, event, and forced-flow variants failed "
                    "selection-aware gates. Ideas must name a falsifier and may not claim success."
                ),
            }
        )
        response = OpenAI().responses.create(
            model=str(settings["model"]),
            input=prompt,
            reasoning={"effort": str(settings["reasoning_effort"])},
            max_output_tokens=int(settings["maximum_output_tokens"]),
            store=False,
            text={
                "format": {
                    "type": "json_schema",
                    "name": "strategy_lab_ideas",
                    "schema": IDEA_SCHEMA,
                    "strict": True,
                }
            },
        )
        try:
            payload = json.loads(response.output_text)
        except (TypeError, json.JSONDecodeError) as exc:
            raise GenerationError(
                "OpenAI returned invalid structured idea output"
            ) from exc
        raw_ideas = payload.get("ideas")
        if not isinstance(raw_ideas, list):
            raise GenerationError("structured idea output has no ideas array")
        ideas: list[StrategyIdea] = []
        seen_templates: set[str] = set()
        for index, raw in enumerate(raw_ideas, 1):
            if not isinstance(raw, dict) or raw.get("template") not in TEMPLATES:
                continue
            template = str(raw["template"])
            if template in seen_templates:
                continue
            seen_templates.add(template)
            horizon = "intraday" if template.startswith("opening_") else "daily"
            try:
                ideas.append(
                    StrategyIdea(
                        idea_id=f"idea-ai-{research_date.isoformat()}-{index:02d}",
                        family_id=f"family-{template.replace('_', '-')}-v1",
                        template=template,
                        causal_thesis=str(raw["causal_thesis"]),
                        falsifier=str(raw["falsifier"]),
                        horizon=horizon,
                        parameter_bias=str(raw["parameter_bias"]),
                        source="openai",
                    )
                )
            except ContractError:
                continue
        minimum = int(self.config.section("daily_run")["mechanism_ideas_minimum"])
        if len(ideas) < minimum:
            ideas.extend(
                item
                for item in fallback_ideas()
                if item.template not in {idea.template for idea in ideas}
            )
        ideas = ideas[
            : int(self.config.section("daily_run")["mechanism_ideas_maximum"])
        ]

        usage = response.usage
        input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
        cost = (
            input_tokens * float(settings["input_cost_per_million_tokens_usd"])
            + output_tokens * float(settings["output_cost_per_million_tokens_usd"])
        ) / 1_000_000
        if spent + cost > budget:
            raise GenerationError("OpenAI response would exceed the hard daily budget")
        usage_id = f"usage-{uuid4()}"
        self.database.connection.execute(
            "INSERT INTO idea_usage VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                usage_id,
                research_date,
                str(settings["model"]),
                input_tokens,
                output_tokens,
                cost,
                canonical_sha256(prompt),
                canonical_sha256(payload),
                utc_now(),
            ],
        )
        return ideas, {
            "source": "openai",
            "model": str(settings["model"]),
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "estimated_cost_usd": cost,
        }


def _comparison(op: str, feature: str, value: float) -> dict[str, Any]:
    return {
        "op": op,
        "left": {"feature": feature},
        "right": {"value": value},
    }


def _all(*nodes: dict[str, Any]) -> dict[str, Any]:
    return {"op": "all", "args": list(nodes)}


def _grid(
    idea: StrategyIdea,
) -> Iterator[tuple[dict[str, Any], str, str, float, float, int, dict[str, float]]]:
    template = idea.template
    stops = [0.005, 0.0075, 0.01, 0.0125, 0.015, 0.0175, 0.02, 0.025, 0.03]
    targets = [0.01, 0.015, 0.02, 0.025, 0.03, 0.04, 0.05, 0.06]
    holds = [1, 2, 3, 4, 5]
    if idea.parameter_bias == "conservative":
        stops, targets = stops[:6], targets[:5]
    elif idea.parameter_bias == "aggressive":
        stops, targets = stops[3:], targets[3:]

    if template == "short_horizon_reversal":
        for threshold, position, stop, target, hold in itertools.product(
            [-0.01 * value for value in range(1, 11)],
            [0.05 * value for value in range(1, 11)],
            stops,
            targets,
            holds,
        ):
            yield (
                _all(
                    _comparison("lte", "return_5d", threshold),
                    _comparison("lte", "range_position_14", position),
                ),
                "return_5d",
                "asc",
                stop,
                target,
                hold,
                {
                    "return_5d": threshold,
                    "range_position": position,
                    "stop": stop,
                    "target": target,
                },
            )
    elif template == "gap_reversal":
        for gap, volume, stop, target, hold in itertools.product(
            [-0.01 * value for value in range(1, 11)],
            [0.4 + 0.2 * value for value in range(10)],
            stops,
            targets,
            holds,
        ):
            yield (
                _all(
                    _comparison("lte", "gap_1d", gap),
                    _comparison("gte", "volume_ratio_20", volume),
                ),
                "gap_1d",
                "asc",
                stop,
                target,
                hold,
                {"gap": gap, "volume": volume, "stop": stop, "target": target},
            )
    elif template == "trend_pullback":
        for trend, pullback, stop, target, hold in itertools.product(
            [0.02 * value for value in range(1, 11)],
            [-0.005 * value for value in range(1, 11)],
            stops,
            targets,
            holds,
        ):
            yield (
                _all(
                    _comparison("gte", "return_20d", trend),
                    _comparison("lte", "return_1d", pullback),
                ),
                "return_20d",
                "desc",
                stop,
                target,
                hold,
                {"trend": trend, "pullback": pullback, "stop": stop, "target": target},
            )
    elif template == "medium_term_momentum":
        for momentum, volume, stop, target, hold in itertools.product(
            [0.02 * value for value in range(1, 11)],
            [0.4 + 0.2 * value for value in range(10)],
            stops,
            targets,
            holds,
        ):
            yield (
                _all(
                    _comparison("gte", "return_20d", momentum),
                    _comparison("gte", "volume_ratio_20", volume),
                ),
                "return_20d",
                "desc",
                stop,
                target,
                hold,
                {
                    "momentum": momentum,
                    "volume": volume,
                    "stop": stop,
                    "target": target,
                },
            )
    elif template == "range_breakout":
        for position, volume, stop, target, hold in itertools.product(
            [0.50 + 0.05 * value for value in range(10)],
            [0.5 + 0.25 * value for value in range(10)],
            stops,
            targets,
            holds,
        ):
            yield (
                _all(
                    _comparison("gte", "range_position_14", position),
                    _comparison("gte", "volume_ratio_20", volume),
                ),
                "volume_ratio_20",
                "desc",
                stop,
                target,
                hold,
                {
                    "position": position,
                    "volume": volume,
                    "stop": stop,
                    "target": target,
                },
            )
    elif template == "volatility_contraction_breakout":
        for atr, position, stop, target, hold in itertools.product(
            [0.005 * value for value in range(1, 11)],
            [0.50 + 0.05 * value for value in range(10)],
            stops,
            targets,
            holds,
        ):
            yield (
                _all(
                    _comparison("lte", "atr_pct_14", atr),
                    _comparison("gte", "range_position_14", position),
                ),
                "range_position_14",
                "desc",
                stop,
                target,
                hold,
                {"atr": atr, "position": position, "stop": stop, "target": target},
            )
    elif template == "market_regime_momentum":
        for market, momentum, stop, target, hold in itertools.product(
            [-0.02 + 0.005 * value for value in range(10)],
            [0.01 * value for value in range(1, 11)],
            stops,
            targets,
            holds,
        ):
            yield (
                _all(
                    _comparison("gte", "market_return_5d", market),
                    _comparison("gte", "return_5d", momentum),
                ),
                "return_5d",
                "desc",
                stop,
                target,
                hold,
                {
                    "market": market,
                    "momentum": momentum,
                    "stop": stop,
                    "target": target,
                },
            )
    elif template == "relative_strength_continuation":
        for strength, position, stop, target, hold in itertools.product(
            [0.02 * value for value in range(1, 11)],
            [0.50 + 0.05 * value for value in range(10)],
            stops,
            targets,
            holds,
        ):
            yield (
                _all(
                    _comparison("gte", "return_20d", strength),
                    _comparison("gte", "range_position_14", position),
                ),
                "return_20d",
                "desc",
                stop,
                target,
                hold,
                {
                    "strength": strength,
                    "position": position,
                    "stop": stop,
                    "target": target,
                },
            )
    elif template == "opening_reversal":
        for opening, range_pct, stop, target in itertools.product(
            [-0.0025 * value for value in range(1, 11)],
            [0.0025 * value for value in range(1, 11)],
            stops,
            targets,
        ):
            yield (
                _all(
                    _comparison("lte", "opening_return_30m", opening),
                    _comparison("gte", "opening_range_pct_30m", range_pct),
                ),
                "opening_return_30m",
                "asc",
                stop,
                target,
                1,
                {
                    "opening": opening,
                    "range": range_pct,
                    "stop": stop,
                    "target": target,
                },
            )
    elif template == "opening_momentum":
        for opening, range_pct, stop, target in itertools.product(
            [0.0025 * value for value in range(1, 11)],
            [0.0025 * value for value in range(1, 11)],
            stops,
            targets,
        ):
            yield (
                _all(
                    _comparison("gte", "opening_return_30m", opening),
                    _comparison("gte", "opening_range_pct_30m", range_pct),
                ),
                "opening_return_30m",
                "desc",
                stop,
                target,
                1,
                {
                    "opening": opening,
                    "range": range_pct,
                    "stop": stop,
                    "target": target,
                },
            )
    else:
        raise GenerationError(f"unsupported idea template {template!r}")


def expand_specs(
    ideas: Iterable[StrategyIdea],
    *,
    target: int,
    round_trip_bps: int,
    provenance: dict[str, Any],
    existing_rules: set[str] | None = None,
) -> list[StrategySpec]:
    if not 50 <= target <= 500:
        raise GenerationError("daily target must be between 50 and 500")
    idea_list = list(ideas)
    if not idea_list:
        raise GenerationError("at least one strategy idea is required")
    iterators = {idea.idea_id: iter(_grid(idea)) for idea in idea_list}
    exhausted: set[str] = set()
    known = set(existing_rules or ())
    result: list[StrategySpec] = []
    while len(result) < target and len(exhausted) < len(idea_list):
        for idea in idea_list:
            if idea.idea_id in exhausted or len(result) >= target:
                continue
            try:
                signal, rank, direction, stop, target_pct, hold, parameters = next(
                    iterators[idea.idea_id]
                )
            except StopIteration:
                exhausted.add(idea.idea_id)
                continue
            temporary = StrategySpec(
                strategy_id="strategy-pending",
                family_id=idea.family_id,
                idea_id=idea.idea_id,
                horizon=idea.horizon,
                signal=signal,
                rank_by=rank,
                rank_direction=direction,
                entry="next_15m_open" if idea.horizon == "intraday" else "next_open",
                stop_loss_pct=stop,
                target_pct=target_pct,
                maximum_hold_sessions=hold,
                round_trip_bps=round_trip_bps,
                causal_thesis=idea.causal_thesis,
                falsifier=idea.falsifier,
                parameters=parameters,
                provenance=provenance,
            )
            if temporary.rules_sha256 in known:
                continue
            payload = temporary.to_dict()
            payload.pop("rules_sha256")
            payload["strategy_id"] = f"strategy-{temporary.rules_sha256[:20]}"
            spec = StrategySpec(**payload)
            known.add(spec.rules_sha256)
            result.append(spec)
    if len(result) < target:
        raise GenerationError(
            f"only {len(result)} semantically unique configurations were available for target {target}"
        )
    return result
