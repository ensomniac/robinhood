from __future__ import annotations

from datetime import datetime

import country_etf_opening_reversal_range as ranged


def test_range_version_changes_only_provider_request_topology(
    tmp_path, monkeypatch
):
    original_repo_path = ranged._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(ranged, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(ranged, "_repo_path", repo_path)

    _path, contract, capacity_path = ranged.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    previous = ranged._read_plain(ranged.BASE_CONTRACT)
    assert capacity_path.is_file()
    assert contract["parameter_grid"] == previous["parameter_grid"]
    assert contract["development_dates"] == previous["development_dates"]
    assert contract["confirmation_dates"] == previous["confirmation_dates"]
    assert contract["universe"] == previous["universe"]
    assert contract["historical_data_contract"][
        "minute_request_mode"
    ] == "symbol_range"
