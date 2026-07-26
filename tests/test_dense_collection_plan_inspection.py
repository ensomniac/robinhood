from __future__ import annotations

import json
from datetime import date, datetime

import cross_style_breadth_successor as successor
import dense_collection_recovery
import dense_collection_plan_inspection as inspection
import dense_data_collection
import dense_strategy_runtime as runtime
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
MASSIVE_RECOVERY_FAILURE_INSPECTION = (
    inspection.PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-market-residual-reversal-replication-v4/"
    "development-collection-failure-inspection/"
    "liquid-etf-market-residual-reversal-replication-v4-development-"
    "collection-failure-inspection-"
    "13461de6700578ef2eba95f50bf894a5df5e80f880f76897bf058cecc7cdb589.json"
)
YAHOO_RECOVERY_FAILURE_INSPECTION = (
    inspection.PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-market-residual-reversal-replication-v4/"
    "development-collection-failure-inspection/"
    "liquid-etf-market-residual-reversal-replication-v4-development-"
    "collection-failure-inspection-"
    "5fba3464fae7bab79e050a9ad73c6e5c1d9d5a81300fe4ebb1977eff4dd72fbc.json"
)
VIX_PARTIAL_FAILURE = (
    inspection.PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "vix-shock-low-volatility-equity-rebound/"
    "development-collection-failure/"
    "vix-shock-low-volatility-equity-rebound-development-collection-"
    "failure-90363424c1c2b61b9038045add8751517cd49b38416f8460dead30"
    "ff49772a9d.json"
)
VIX_SEARCH = (
    inspection.PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "vix-shock-low-volatility-equity-rebound/search/"
    "vix-shock-low-volatility-equity-rebound-search-"
    "98b61656f33bbd0c8d2cc17988a7cefce46b3d7890f601059300cf300"
    "abe67c0.json"
)


def test_vix_shock_family_is_supported_by_independent_inspector():
    assert (
        runtime.VIX_SHOCK_REBOUND_FAMILY
        in inspection.SUPPORTED_FAMILIES
    )


def test_vix_recovery_binds_only_its_inspected_partial_exposure():
    failure = json.loads(VIX_PARTIAL_FAILURE.read_text(encoding="utf-8"))
    search = json.loads(VIX_SEARCH.read_text(encoding="utf-8"))
    records = [
        record
        for record in outcome_exposure.read_index()
        if record["exposure_id"]
        == f"dense-collection-failure-{failure['artifact_sha256'][:20]}"
    ]
    assert len(records) == 1

    state = inspection._development_outcome_state(
        {
            "recovery_kind": (
                dense_data_collection.VIX_FEATURE_SCHEMA_RECOVERY
            ),
            "recovery_failure_path": str(
                VIX_PARTIAL_FAILURE.relative_to(inspection.PROJECT_ROOT)
            ),
            "recovery_failure_sha256": failure["artifact_sha256"],
        },
        search["family_contract"],
        records,
        enforce_commit=False,
    )

    assert state == "INSPECTED_PARTIAL_SOURCE_RECOVERY_BOUND"


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
    exposure_audit = outcome_exposure.audit()
    predecessor_records = [
        record
        for record in outcome_exposure.read_index()
        if record["exposure_id"] == successor.PREDECESSOR_EXPOSURE_ID
    ]
    assert len(predecessor_records) == 1
    monkeypatch.setattr(
        successor.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: predecessor_records,
    )
    monkeypatch.setattr(
        successor.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: exposure_audit,
    )
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
        {},
        contract,
        predecessor_records,
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


def test_recovery_plan_inspection_rebuilds_massive_source_only_change(
    tmp_path, monkeypatch
):
    original_repo_path = inspection._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except inspection.DenseCollectionPlanInspectionError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(inspection, "_repo_path", repo_path)
    recovery_plan, plan = (
        dense_collection_recovery.freeze_pullback_recovery(
            MASSIVE_RECOVERY_FAILURE_INSPECTION,
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

    assert plan["daily_provider"] == "massive"
    assert plan["adjustment_semantics"] == (
        dense_data_collection.MASSIVE_SOURCE_RECOVERY_ADJUSTMENT
    )
    assert {
        task["kind"] for task in plan["tasks"][1:]
    } == {"massive_daily_symbol_bars"}
    assert artifact["state"] == inspection.READY_STATE
    assert artifact["task_count"] == 11
    assert artifact["development_outcome_state"] == (
        "SELF_DEVELOPMENT_EXPOSURE_BOUND"
    )
    assert artifact["checks"]["recovery_lineage_rebuilt"] is True
    assert all(artifact["checks"].values())


def test_recovery_plan_inspection_rebuilds_yahoo_source_only_change(
    tmp_path, monkeypatch
):
    original_repo_path = inspection._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except inspection.DenseCollectionPlanInspectionError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(inspection, "_repo_path", repo_path)
    recovery_plan, plan = (
        dense_collection_recovery.freeze_pullback_recovery(
            YAHOO_RECOVERY_FAILURE_INSPECTION,
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

    assert plan["daily_provider"] == "yahoo"
    assert plan["adjustment_semantics"] == (
        dense_data_collection.YAHOO_SOURCE_RECOVERY_ADJUSTMENT
    )
    assert plan["source_request_semantics"]["no_purchase_required"] is True
    assert {
        task["kind"] for task in plan["tasks"][1:]
    } == {"yahoo_daily_symbol_bars"}
    assert artifact["state"] == inspection.READY_STATE
    assert artifact["task_count"] == 11
    assert artifact["development_outcome_state"] == (
        "SELF_DEVELOPMENT_EXPOSURE_BOUND"
    )
    assert artifact["checks"]["provider_semantics_rebuilt"] is True
    assert all(artifact["checks"].values())
