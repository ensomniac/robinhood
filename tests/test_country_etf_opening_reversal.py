from __future__ import annotations

from datetime import datetime

import country_etf_opening_reversal as country


def test_country_replication_has_long_history_and_clean_fixed_universe():
    assert country.DEVELOPMENT_WARMUP_SESSIONS == 60
    assert country.DEVELOPMENT_SESSIONS == 1_000
    assert country.CONFIRMATION_SESSIONS == 500
    assert country.TOTAL_SESSIONS == 1_565
    assert country.SYMBOLS == [
        "EWC",
        "EWG",
        "EWP",
        "EWQ",
        "EWT",
        "EWU",
        "EWW",
        "EWY",
    ]


def test_country_replication_freezes_unchanged_intraday_grid(
    tmp_path, monkeypatch
):
    original_repo_path = country._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(country, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(country, "_repo_path", repo_path)
    monkeypatch.setattr(
        country.outcome_exposure, "read_index", lambda *_args, **_kwargs: []
    )
    monkeypatch.setattr(
        country.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

    _path, contract, capacity_path = country.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    assert capacity_path.is_file()
    assert len(contract["trial_family"]) == 32
    assert contract["parameter_grid"] == country._read_plain(
        country.BASE_CONTRACT
    )["parameter_grid"]
    assert contract["development_scope"]["symbols"] == country.SYMBOLS
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["confirmation_signal_capacity"] == 500
