"""Daily run, holdout, paper, and candidate lifecycle orchestration."""

from __future__ import annotations

import json
import shutil
import subprocess
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from time import monotonic
from typing import Any, Callable, Sequence
from uuid import uuid4

from .config import LabConfig
from .contracts import CandidateState, StrategyIdea
from .database import LabDatabase, utc_now
from .engine import BacktestEngine, BacktestEvaluation
from .generator import IdeaGenerator, expand_specs
from .hashing import canonical_json
from .statistics import (
    FamilyAdjustment,
    deflated_sharpe_probability,
    family_adjustment,
)
from .validation import GateDecision, decide, persist_evaluation


class RunnerError(RuntimeError):
    """Raised when an operator transition cannot proceed safely."""


def code_commit(project_root: Path) -> tuple[str, bool]:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = bool(
        subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=project_root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    )
    return commit, dirty


class StrategyLabRunner:
    def __init__(self, config: LabConfig, database: LabDatabase) -> None:
        self.config = config
        self.database = database
        self.engine = BacktestEngine(config, database)

    def _check_resources(self) -> None:
        minimum = int(self.config.section("daily_run")["minimum_free_disk_gb"])
        free = shutil.disk_usage(self.config.state_root).free / (1024**3)
        if free < minimum:
            raise RunnerError(
                f"free disk {free:.1f}GB is below the {minimum}GB reserve"
            )

    def _register_ideas(self, ideas: Sequence[StrategyIdea]) -> None:
        for idea in ideas:
            payload = {
                "idea_id": idea.idea_id,
                "family_id": idea.family_id,
                "template": idea.template,
                "causal_thesis": idea.causal_thesis,
                "falsifier": idea.falsifier,
                "horizon": idea.horizon,
                "parameter_bias": idea.parameter_bias,
                "source": idea.source,
            }
            self.database.connection.execute(
                """
                INSERT INTO ideas VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(idea_id) DO NOTHING
                """,
                [
                    idea.idea_id,
                    idea.family_id,
                    idea.source,
                    utc_now(),
                    canonical_json(payload),
                ],
            )

    def _family_selection_rows(
        self,
        family_id: str,
        *,
        excluding: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        excluded = excluding or set()
        rows = self.database.connection.execute(
            """
            SELECT strategy_id, metrics_json
            FROM experiment_results
            WHERE family_id=? AND phase='development'
            QUALIFY row_number() OVER (
                PARTITION BY strategy_id ORDER BY created_at DESC
            ) = 1
            ORDER BY strategy_id
            """,
            [family_id],
        ).fetchall()
        result = []
        for strategy_id, metrics_json in rows:
            if str(strategy_id) in excluded:
                continue
            metrics = json.loads(str(metrics_json))
            result.append(
                {
                    "strategy_id": str(strategy_id),
                    "pvalue": float(metrics["pvalue"]),
                    "stressed_log_growth": float(metrics["stressed_total_log_growth"]),
                    "fold_log_growth": list(metrics["fold_log_growth"]),
                    "parameters": dict(metrics.get("parameters") or {}),
                }
            )
        return result

    def _refresh_prior_dsr(
        self,
        evaluation: BacktestEvaluation,
        *,
        cumulative_trial_count: int,
    ) -> BacktestEvaluation:
        metrics = dict(evaluation.metrics)
        costs = self.config.section("execution")
        stress_delta = (
            int(costs["stress_round_trip_bps"]) - int(costs["base_round_trip_bps"])
        ) / 10_000
        rows = self.database.connection.execute(
            """
            SELECT net_return, notional_fraction
            FROM trades
            WHERE strategy_id=? AND phase='development'
            ORDER BY exit_date, symbol
            """,
            [evaluation.strategy_id],
        ).fetchall()
        stressed_account = [
            (float(net_return) - stress_delta) * float(notional)
            for net_return, notional in rows
        ]
        metrics["deflated_sharpe_probability"] = deflated_sharpe_probability(
            stressed_account,
            cumulative_trial_count,
        )
        return BacktestEvaluation(
            strategy_id=evaluation.strategy_id,
            rules_sha256=evaluation.rules_sha256,
            family_id=evaluation.family_id,
            phase=evaluation.phase,
            metrics=metrics,
            trades=evaluation.trades,
        )

    def _prior_development_passes(
        self,
        family_id: str,
    ) -> list[BacktestEvaluation]:
        rows = self.database.connection.execute(
            """
            SELECT result.strategy_id, result.rules_sha256, result.metrics_json
            FROM experiment_results AS result
            JOIN candidates AS candidate USING(strategy_id)
            WHERE result.family_id=?
              AND result.phase='development'
              AND candidate.state='DEVELOPMENT_PASS'
            QUALIFY row_number() OVER (
                PARTITION BY result.strategy_id ORDER BY result.created_at DESC
            ) = 1
            ORDER BY result.strategy_id
            """,
            [family_id],
        ).fetchall()
        return [
            BacktestEvaluation(
                strategy_id=str(strategy_id),
                rules_sha256=str(rules_sha256),
                family_id=family_id,
                phase="development",
                metrics=json.loads(str(metrics_json)),
                trades=(),
            )
            for strategy_id, rules_sha256, metrics_json in rows
        ]

    def _reject_stale_family_pass(
        self,
        evaluation: BacktestEvaluation,
        decision: GateDecision,
    ) -> None:
        self.database.connection.execute(
            """
            UPDATE candidates
            SET state='REJECTED', state_reason=?, updated_at=?
            WHERE strategy_id=? AND state='DEVELOPMENT_PASS'
            """,
            [
                "cumulative selection audit: " + ", ".join(decision.failures),
                utc_now(),
                evaluation.strategy_id,
            ],
        )
        self.database.emit_event(
            event_id=f"event-family-audit-{uuid4()}",
            event_type="DEVELOPMENT_PASS_REVOKED",
            severity="warning",
            entity_type="strategy",
            entity_id=evaluation.strategy_id,
            message=(
                "A prior development pass failed the expanded cumulative "
                "family-selection audit before holdout access."
            ),
            payload={"failures": list(decision.failures)},
        )

    def run_daily(
        self,
        *,
        research_date: date | None = None,
        target: int | None = None,
        allow_dirty: bool = False,
        progress_callback: Callable[[str], None] | None = None,
    ) -> dict[str, Any]:
        self._check_resources()
        data_version = self.database.latest_data_version()
        if not data_version:
            raise RunnerError("run `lab data sync` before daily research")
        if data_version["capacity_state"] != "CAPACITY_READY":
            raise RunnerError(
                f"active data version is {data_version['capacity_state']}; "
                "the locked development/holdout split is not large enough"
            )
        project_root = Path(__file__).resolve().parents[1]
        commit, dirty = code_commit(project_root)
        if dirty and not allow_dirty:
            raise RunnerError("daily research requires a clean committed worktree")
        settings = self.config.section("daily_run")
        requested = int(target or settings["target_configurations"])
        if (
            not int(settings["minimum_configurations"])
            <= requested
            <= int(settings["maximum_configurations"])
        ):
            raise RunnerError("target is outside the configured 50-500 daily range")
        day = research_date or datetime.now(UTC).date()
        run_id = f"daily-{day.isoformat()}-{uuid4().hex[:12]}"
        manifest = {
            "schema_version": 1,
            "research_date": day.isoformat(),
            "data_version_id": data_version["data_version_id"],
            "dataset_sha256": data_version["dataset_sha256"],
            "code_commit": commit,
            "dirty_override": dirty,
            "target_configurations": requested,
            "provider_requests": 0,
            "broker_actions": 0,
        }
        self.database.connection.execute(
            """
            INSERT INTO runs(
                run_id, run_kind, status, started_at, data_version_id, code_commit,
                target_configurations, manifest_json
            ) VALUES (?, 'daily', 'RUNNING', ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                utc_now(),
                data_version["data_version_id"],
                commit,
                requested,
                canonical_json(manifest),
            ],
        )
        try:
            ideas, generation = IdeaGenerator(self.config, self.database).generate(
                research_date=day
            )
            locked_families = {
                str(row[0])
                for row in self.database.connection.execute(
                    """
                    SELECT DISTINCT family_id FROM candidates
                    WHERE state IN (
                        'HISTORICALLY_VALIDATED',
                        'PAPER_ACTIVE',
                        'PILOT_READY',
                        'LIVE_EVALUATING',
                        'LIVE_VALIDATED'
                    )
                    """
                ).fetchall()
            }
            ideas = [idea for idea in ideas if idea.family_id not in locked_families]
            self._register_ideas(ideas)
            known_rules = {
                str(row[0])
                for row in self.database.connection.execute(
                    "SELECT rules_sha256 FROM specs"
                ).fetchall()
            }
            specs = expand_specs(
                ideas,
                target=requested,
                round_trip_bps=int(
                    self.config.section("execution")["base_round_trip_bps"]
                ),
                provenance={
                    "run_id": run_id,
                    "research_date": day.isoformat(),
                    "data_version_id": data_version["data_version_id"],
                    "code_commit": commit,
                    "idea_generation": generation,
                },
                existing_rules=known_rules,
            )
            for spec in specs:
                if not self.database.register_spec(spec):
                    raise RunnerError(
                        f"semantic duplicate escaped generation: {spec.rules_sha256}"
                    )
            self.database.connection.execute(
                "UPDATE runs SET generated_configurations=? WHERE run_id=?",
                [len(specs), run_id],
            )
            cumulative = self.database.table_count("specs")
            evaluations = []
            last_progress_publish = monotonic()
            for index, spec in enumerate(specs, 1):
                evaluations.append(
                    self.engine.evaluate(
                        spec,
                        phase="development",
                        cumulative_trial_count=cumulative,
                    )
                )
                if index % 10 == 0 or index == len(specs):
                    self.database.connection.execute(
                        "UPDATE runs SET tested_configurations=? WHERE run_id=?",
                        [index, run_id],
                    )
                    if progress_callback and monotonic() - last_progress_publish >= 10:
                        try:
                            progress_callback(run_id)
                        except Exception:
                            pass
                        last_progress_publish = monotonic()
            by_family: dict[str, list[BacktestEvaluation]] = defaultdict(list)
            for evaluation in evaluations:
                by_family[evaluation.family_id].append(evaluation)
            decisions: dict[str, GateDecision] = {}
            for family_id, family_rows in by_family.items():
                family_strategy_ids = {row.strategy_id for row in family_rows}
                prior_rows = self._family_selection_rows(
                    family_id,
                    excluding=family_strategy_ids,
                )
                adjustment = family_adjustment(
                    [*prior_rows, *(row.selection_row for row in family_rows)],
                    minimum_positive_neighbor_fraction=float(
                        self.config.section("validation")[
                            "minimum_positive_neighbor_fraction"
                        ]
                    ),
                )
                preliminary = {
                    row.strategy_id: decide(self.config, row, adjustment)
                    for row in family_rows
                }
                surviving_prior = []
                for prior in self._prior_development_passes(family_id):
                    refreshed = self._refresh_prior_dsr(
                        prior,
                        cumulative_trial_count=cumulative,
                    )
                    prior_decision = decide(self.config, refreshed, adjustment)
                    if prior_decision.passed:
                        surviving_prior.append(refreshed)
                    else:
                        self._reject_stale_family_pass(refreshed, prior_decision)
                passing = [
                    row for row in family_rows if preliminary[row.strategy_id].passed
                ]
                winner = (
                    max(
                        passing,
                        key=lambda row: (
                            row.metrics["stressed_total_log_growth"],
                            row.strategy_id,
                        ),
                    ).strategy_id
                    if passing and not surviving_prior
                    else None
                )
                for row in family_rows:
                    decision = preliminary[row.strategy_id]
                    if decision.passed and surviving_prior:
                        decision = GateDecision(
                            state=CandidateState.REJECTED,
                            failures=("family_already_has_development_pass",),
                            metrics=decision.metrics,
                        )
                    elif decision.passed and row.strategy_id != winner:
                        decision = GateDecision(
                            state=CandidateState.REJECTED,
                            failures=("not_deterministic_family_winner",),
                            metrics=decision.metrics,
                        )
                    decisions[row.strategy_id] = decision
            for spec, evaluation in zip(specs, evaluations, strict=True):
                persist_evaluation(
                    self.database,
                    spec,
                    run_id,
                    evaluation,
                    decisions[spec.strategy_id],
                )
            accepted = sum(
                decision.state == CandidateState.DEVELOPMENT_PASS
                for decision in decisions.values()
            )
            manifest["idea_generation"] = generation
            manifest["mechanism_ideas"] = len(ideas)
            manifest["generated_configurations"] = len(specs)
            manifest["tested_configurations"] = len(evaluations)
            manifest["accepted_configurations"] = accepted
            self.database.connection.execute(
                """
                UPDATE runs SET status='COMPLETED', completed_at=?,
                    generated_configurations=?, tested_configurations=?,
                    accepted_configurations=?, manifest_json=?
                WHERE run_id=?
                """,
                [
                    utc_now(),
                    len(specs),
                    len(evaluations),
                    accepted,
                    canonical_json(manifest),
                    run_id,
                ],
            )
            return {"run_id": run_id, "status": "COMPLETED", **manifest}
        except Exception as exc:
            self.database.connection.execute(
                "UPDATE runs SET status='FAILED', completed_at=?, error=? WHERE run_id=?",
                [utc_now(), str(exc), run_id],
            )
            raise

    def promote_holdout(
        self, strategy_id: str, *, allow_dirty: bool = False
    ) -> dict[str, Any]:
        row = self.database.connection.execute(
            """
            SELECT rules_sha256, family_id, state
            FROM candidates WHERE strategy_id = ?
            """,
            [strategy_id],
        ).fetchone()
        if not row:
            raise RunnerError(f"unknown candidate {strategy_id}")
        rules_sha256, family_id, state = map(str, row)
        if state != CandidateState.DEVELOPMENT_PASS:
            raise RunnerError("only DEVELOPMENT_PASS candidates may open holdout")
        cumulative = self.database.table_count("specs")
        metrics_row = self.database.connection.execute(
            """
            SELECT metrics_json FROM experiment_results
            WHERE strategy_id=? AND phase='development'
            ORDER BY created_at DESC LIMIT 1
            """,
            [strategy_id],
        ).fetchone()
        if not metrics_row:
            raise RunnerError("candidate development evidence is missing")
        development_evaluation = BacktestEvaluation(
            strategy_id=strategy_id,
            rules_sha256=rules_sha256,
            family_id=family_id,
            phase="development",
            metrics=json.loads(str(metrics_row[0])),
            trades=(),
        )
        development_evaluation = self._refresh_prior_dsr(
            development_evaluation,
            cumulative_trial_count=cumulative,
        )
        adjustment = family_adjustment(
            self._family_selection_rows(family_id),
            minimum_positive_neighbor_fraction=float(
                self.config.section("validation")["minimum_positive_neighbor_fraction"]
            ),
        )
        current_decision = decide(
            self.config,
            development_evaluation,
            adjustment,
        )
        if not current_decision.passed:
            self._reject_stale_family_pass(
                development_evaluation,
                current_decision,
            )
            raise RunnerError(
                "candidate no longer passes cumulative selection-aware development gates"
            )
        prior = self.database.connection.execute(
            "SELECT count(*) FROM holdout_access WHERE family_id = ?", [family_id]
        ).fetchone()[0]
        if prior:
            raise RunnerError("this mechanism family has already opened holdout")
        weekly = self.database.connection.execute(
            """
            SELECT count(*) FROM holdout_access
            WHERE date_trunc('week', opened_at) = date_trunc('week', current_timestamp)
            """
        ).fetchone()[0]
        maximum = int(
            self.config.section("validation")["maximum_holdout_promotions_per_week"]
        )
        if weekly >= maximum:
            raise RunnerError("weekly holdout promotion limit is exhausted")
        data_version = self.database.latest_data_version()
        if not data_version or data_version["capacity_state"] != "CAPACITY_READY":
            raise RunnerError("active data version is not holdout-capable")
        commit, dirty = code_commit(Path(__file__).resolve().parents[1])
        if dirty and not allow_dirty:
            raise RunnerError("holdout access requires a clean committed worktree")
        spec = self.database.load_spec(strategy_id)
        run_id = f"holdout-{datetime.now(UTC).date().isoformat()}-{uuid4().hex[:12]}"
        access_id = f"holdout-access-{uuid4()}"
        self.database.connection.execute(
            """
            INSERT INTO runs(
                run_id, run_kind, status, started_at, data_version_id, code_commit,
                target_configurations, generated_configurations, tested_configurations,
                manifest_json
            ) VALUES (?, 'holdout', 'RUNNING', ?, ?, ?, 1, 1, 0, ?)
            """,
            [
                run_id,
                utc_now(),
                data_version["data_version_id"],
                commit,
                canonical_json(
                    {
                        "strategy_id": strategy_id,
                        "rules_sha256": rules_sha256,
                        "family_id": family_id,
                    }
                ),
            ],
        )
        self.database.connection.execute(
            "INSERT INTO holdout_access VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                access_id,
                strategy_id,
                rules_sha256,
                family_id,
                run_id,
                utc_now(),
                data_version["data_version_id"],
            ],
        )
        evaluation = self.engine.evaluate(
            spec,
            phase="holdout",
            cumulative_trial_count=self.database.table_count("specs"),
        )
        development = self.database.connection.execute(
            """
            SELECT probability_backtest_overfit, holm_pass, neighbor_stability
            FROM experiment_results
            WHERE strategy_id=? AND phase='development'
            ORDER BY created_at DESC LIMIT 1
            """,
            [strategy_id],
        ).fetchone()
        if not development:
            raise RunnerError("candidate development evidence is missing")
        adjustment = FamilyAdjustment(
            pbo=float(development[0]),
            holm_pass={strategy_id: bool(development[1])},
            neighbor_stability={strategy_id: bool(development[2])},
        )
        decision = decide(self.config, evaluation, adjustment)
        persist_evaluation(self.database, spec, run_id, evaluation, decision)
        self.database.connection.execute(
            """
            UPDATE runs SET status='COMPLETED', completed_at=?,
                tested_configurations=1, accepted_configurations=?
            WHERE run_id=?
            """,
            [
                utc_now(),
                int(decision.state == CandidateState.HISTORICALLY_VALIDATED),
                run_id,
            ],
        )
        return {
            "run_id": run_id,
            "access_id": access_id,
            "strategy_id": strategy_id,
            "state": str(decision.state),
            "failures": list(decision.failures),
        }

    def reproduce(self, run_id: str) -> dict[str, Any]:
        run = self.database.connection.execute(
            "SELECT data_version_id, code_commit, status FROM runs WHERE run_id=?",
            [run_id],
        ).fetchone()
        if not run:
            raise RunnerError(f"unknown run {run_id}")
        active = self.database.latest_data_version()
        if not active or str(run[0]) != active["data_version_id"]:
            raise RunnerError("the exact run data version is not active")
        rows = self.database.connection.execute(
            """
            SELECT result.strategy_id, result.phase, result.metrics_json
            FROM experiment_results AS result
            WHERE result.run_id=? ORDER BY result.strategy_id
            """,
            [run_id],
        ).fetchall()
        mismatches: list[dict[str, str]] = []
        for strategy_id, phase, metrics_json in rows:
            expected = json.loads(str(metrics_json))
            spec = self.database.load_spec(str(strategy_id))
            rebuilt = self.engine.evaluate(
                spec,
                phase=str(phase),
                cumulative_trial_count=self.database.table_count("specs"),
            )
            if rebuilt.metrics["evaluation_sha256"] != expected["evaluation_sha256"]:
                mismatches.append(
                    {
                        "strategy_id": str(strategy_id),
                        "expected": str(expected["evaluation_sha256"]),
                        "rebuilt": str(rebuilt.metrics["evaluation_sha256"]),
                    }
                )
        return {
            "run_id": run_id,
            "status": "REPRODUCED" if not mismatches else "MISMATCH",
            "strategies_rebuilt": len(rows),
            "mismatches": mismatches,
            "provider_requests": 0,
            "broker_actions": 0,
        }

    def record_paper_signal(
        self,
        strategy_id: str,
        *,
        clean: bool,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        row = self.database.connection.execute(
            "SELECT state, rules_sha256 FROM candidates WHERE strategy_id=?",
            [strategy_id],
        ).fetchone()
        if not row or str(row[0]) not in {
            CandidateState.HISTORICALLY_VALIDATED,
            CandidateState.PAPER_ACTIVE,
        }:
            raise RunnerError(
                "paper evidence requires a historically validated candidate"
            )
        event_id = f"paper-{uuid4()}"
        self.database.connection.execute(
            "INSERT INTO paper_events VALUES (?, ?, 'signal', ?, ?, ?)",
            [
                event_id,
                strategy_id,
                clean,
                utc_now(),
                canonical_json(payload),
            ],
        )
        history = [
            bool(item[0])
            for item in self.database.connection.execute(
                """
                SELECT clean FROM paper_events
                WHERE strategy_id=? AND event_type='signal'
                ORDER BY observed_at DESC
                """,
                [strategy_id],
            ).fetchall()
        ]
        count = 0
        for is_clean in history:
            if not is_clean:
                break
            count += 1
        minimum = int(self.config.section("validation")["minimum_clean_paper_signals"])
        state = (
            CandidateState.PILOT_READY
            if count >= minimum
            else CandidateState.PAPER_ACTIVE
        )
        self.database.connection.execute(
            """
            UPDATE candidates SET state=?, state_reason=?, paper_clean_signals=?,
                updated_at=? WHERE strategy_id=?
            """,
            [
                str(state),
                f"{count}/{minimum} clean prospective paper signals",
                count,
                utc_now(),
                strategy_id,
            ],
        )
        return {
            "event_id": event_id,
            "strategy_id": strategy_id,
            "state": str(state),
            "clean_signals": count,
        }
