from __future__ import annotations

import json

import dense_collection_plan_inspection as inspection
import outcome_exposure


PLAN = (
    inspection.PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-style-etf-breadth-continuation/development-collection-plan/"
    "cross-style-etf-breadth-continuation-development-collection-plan-"
    "7f762395047610635bd897ca0b7a908ccf42ec603edee879cb73264d51773f43.json"
)


def test_independent_plan_inspection_rebuilds_all_nine_requests(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "0" * 64},
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
