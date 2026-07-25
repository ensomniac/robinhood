from __future__ import annotations

import json
from datetime import date, datetime

import cross_style_breadth_successor as successor
import dense_collection_recovery
import dense_collection_plan_inspection as inspection
import outcome_exposure


PLAN = (
    inspection.PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-style-etf-breadth-continuation/development-collection-plan/"
    "cross-style-etf-breadth-continuation-development-collection-plan-"
    "7f762395047610635bd897ca0b7a908ccf42ec603edee879cb73264d51773f43.json"
)
FAILURE_INSPECTION = (
    inspection.PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-style-etf-breadth-continuation/"
    "development-collection-failure-inspection/"
    "cross-style-etf-breadth-continuation-development-collection-"
    "failure-inspection-1812d98042611a45a0a9d58ee6dc953f0a9f2ea"
    "882e4d1b0c33268d7c45660e6.json"
)


def test_independent_plan_inspection_rebuilds_all_nine_requests(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "0" * 64},
    )
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "read_index",
        lambda: [],
    )
    path, artifact = inspection.inspect_plan(
        PLAN,
        root=tmp_path,
        enforce_commit=False,
    )
    assert path.is_file()
    assert artifact["state"] == inspection.READY_STATE
    assert artifact["task_count"] == 9
    assert artifact["provider_requests"] == 0
    assert all(artifact["checks"].values())


def test_plan_inspection_rejects_contaminated_development(
    tmp_path, monkeypatch
):
    plan = json.loads(PLAN.read_text(encoding="utf-8"))
    date = plan["evaluation_dates"][0]
    symbol = plan["symbols"][0]
    record = outcome_exposure.build_record(
        exposure_id="synthetic-plan-contamination",
        campaign_id="multi-strategy-portfolio-validation-v2",
        lane="development",
        source_path="synthetic.json",
        source_sha256="1" * 64,
        recorded_at="2026-07-25T00:00:00Z",
        scope={"dates": [date], "symbols": [symbol]},
    )
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "read_index",
        lambda: [record],
    )
    try:
        inspection.inspect_plan(
            PLAN,
            root=tmp_path,
            enforce_commit=False,
        )
    except outcome_exposure.OutcomeExposureError:
        pass
    else:
        raise AssertionError("contaminated development plan was accepted")


def test_successor_accepts_only_its_bound_declared_contamination(
    tmp_path, monkeypatch
):
    original_repo_path = successor._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(successor, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(successor, "_repo_path", repo_path)
    _path, contract, _capacity = successor.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    state = inspection._development_outcome_state(
        contract,
        outcome_exposure.read_index(),
        enforce_commit=False,
    )

    assert state == "DECLARED_CONTAMINATION_BOUND"


def test_recovery_plan_inspection_rebuilds_alpaca_source_only_change(
    tmp_path, monkeypatch
):
    original_repo_path = inspection._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except inspection.DenseCollectionPlanInspectionError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(inspection, "_repo_path", repo_path)
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "0" * 64},
    )
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "read_index",
        lambda: [],
    )
    recovery_plan, _plan = (
        dense_collection_recovery.freeze_pullback_recovery(
            FAILURE_INSPECTION,
            as_of=date(2026, 7, 25),
            actual_today=date(2026, 7, 25),
            public_root=tmp_path / "recovery",
            enforce_commit=False,
        )
    )
    _path, artifact = inspection.inspect_plan(
        recovery_plan,
        root=tmp_path / "inspection",
        enforce_commit=False,
    )

    assert artifact["state"] == inspection.READY_STATE
    assert artifact["task_count"] == 9
    assert artifact["checks"]["recovery_lineage_rebuilt"] is True
    assert all(artifact["checks"].values())
