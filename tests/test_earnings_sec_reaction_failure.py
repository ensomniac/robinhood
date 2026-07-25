from pathlib import Path

import earnings_sec_reaction_failure as source
from historical_store import HistoricalDayStore


SEARCH = Path(
    "strategy_tournament/v2/discovery/"
    "earnings-sec-yoy-eps-reaction-drift/search/"
    "earnings-sec-yoy-eps-reaction-drift-search-"
    "0fc621564e338e96b6f3641a145cce3d5fd4fc4da35256c3dce9887da3399f1e.json"
)
INSPECTION = Path(
    "strategy_tournament/v2/continuous/"
    "earnings-positive-surprise-drift-v10-sec-reaction-search/"
    "search-inspection/"
    "inspection-"
    "d963335f1458d6792fedb1c3e2dfeefaec0d637f6d4abba016c6785c7473e0a0.json"
)


def test_v10_http_400_failure_retains_no_outcomes(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        source.strategy_discovery, "require_committed", lambda _path: None
    )
    failure = source.build_failure(
        SEARCH,
        INSPECTION,
        observed_at="2026-07-25T04:26:58Z",
        store=HistoricalDayStore(tmp_path / "store", min_free_bytes=0),
    )

    assert failure["failed_request"]["ordinal"] == 1
    assert failure["failed_request"]["symbol"] == "APC"
    assert failure["error"]["http_status"] == 400
    assert failure["failure_boundary"]["rows_retained"] == 0
    assert (
        failure["failure_boundary"]["outcome_exposure_index_changed"]
        is False
    )
    assert failure["disposition"]["same_search_resume_permitted"] is False
    assert (
        failure["disposition"][
            "remaining_108_symbols_successor_permitted"
        ]
        is True
    )
