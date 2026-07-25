from __future__ import annotations

from datetime import datetime

import cross_style_breadth_successor as successor


def test_source_capacity_successor_preserves_rule_and_declares_contamination(
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

    _path, contract, capacity = successor.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    predecessor = successor._predecessor_graph(
        enforce_commit=False
    )["contract"]
    assert capacity.is_file()
    assert contract["research_generation"] == "existing_family_successor"
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["parameter_grid"] == predecessor["parameter_grid"]
    assert len(contract["trial_family"]) == 1
    assert len(contract["development_dates"]) == 548
    assert len(contract["development_signal_dates"]) == 113
    assert contract["embargo_dates"] == [
        "2019-01-02",
        "2019-01-03",
        "2019-01-04",
        "2019-01-07",
        "2019-01-08",
    ]
    assert len(contract["confirmation_dates"]) == 495
    assert contract["confirmation_signal_capacity"] == 102
    assert contract["partitions"]["contaminated_training_declared"] is True
    assert contract["historical_data_contract"]["daily_provider"] == "alpaca"
