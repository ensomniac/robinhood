from __future__ import annotations

from datetime import date, timedelta

import dense_strategy_runtime as runtime
import insider_purchase_discovery as discovery
from learning_experiment import enumerate_trials


def _dates(count: int) -> list[str]:
    start = date(2020, 1, 1)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _event(
    *,
    filing_date: str = "2020-01-20",
    entry_date: str = "2020-01-21",
    symbol: str = "TEST",
    notional: float = 75_000.0,
    owner: str = "1",
) -> dict:
    return {
        "symbol": symbol,
        "issuer_cik": "0000000001",
        "filing_date": filing_date,
        "entry_date": entry_date,
        "scope_dates": [entry_date],
        "purchase_notional": notional,
        "distinct_reporting_owners": 1,
        "transaction_count": 1,
        "accession_numbers": [f"accession-{owner}"],
        "owner_ciks": [owner],
        "transaction_dates": [filing_date],
        "event_semantics": "ORIGINAL_FORM4_DIRECT_OPEN_MARKET_PURCHASE",
    }


def test_parameter_family_contains_exactly_32_trials():
    trials = enumerate_trials(discovery.PARAMETER_GRID)

    assert len(trials) == 32
    assert len({trial["trial_id"] for trial in trials}) == 32


def test_same_entry_symbol_filings_are_aggregated_before_selection():
    first = _event(notional=75_000.0, owner="1")
    second = _event(
        filing_date="2020-01-19",
        notional=250_000.0,
        owner="2",
    )

    result = discovery._aggregate_entry_events([first, second])

    assert len(result) == 1
    assert result[0]["purchase_notional"] == 325_000.0
    assert result[0]["distinct_reporting_owners"] == 2
    assert result[0]["owner_ciks"] == ["1", "2"]
    assert result[0]["filing_dates"] == ["2020-01-19", "2020-01-20"]


def test_runtime_ranks_cluster_and_uses_only_prior_observable_bars():
    dates = _dates(30)
    entry_date = dates[20]
    bars = [
        {
            "date": day,
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1_000_000.0,
        }
        for day in dates
    ]
    metadata = {day: [] for day in dates}
    weaker = _event(
        filing_date=dates[19],
        entry_date=entry_date,
        symbol="WEAK",
        notional=50_000.0,
        owner="1",
    )
    stronger = _event(
        filing_date=dates[19],
        entry_date=entry_date,
        symbol="TEST",
        notional=300_000.0,
        owner="1",
    )
    stronger["distinct_reporting_owners"] = 2
    metadata[entry_date] = [weaker, stronger]
    dataset = runtime.prepare_dataset(
        {
            "schema_version": 1,
            "family_id": discovery.FAMILY_ID,
            "sample_phase": "development",
            "evaluation_dates": dates,
            "signal_dates": [entry_date],
            "event_metadata_by_date": metadata,
            "daily_bars": {"TEST": bars, "WEAK": bars},
        }
    )

    candidates = runtime.build_candidates(
        dataset,
        discovery.FAMILY_ID,
        {
            "maximum_hold_sessions": 3,
            "maximum_prior_20_session_return_fraction": 0.0,
            "minimum_distinct_reporting_owners": 1,
            "minimum_purchase_notional": 50_000.0,
            "stop_atr14": 1.5,
        },
    )

    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "TEST"
    assert candidates[0]["signal_date"] == entry_date
    assert candidates[0]["purchase_notional"] == 300_000.0
    assert candidates[0]["distinct_reporting_owners"] == 2
    assert candidates[0]["outcome"] == "eligible"


def test_runtime_does_not_substitute_when_candidate_data_is_missing():
    dates = _dates(30)
    entry_date = dates[20]
    bars = [
        {
            "date": day,
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1_000_000.0,
        }
        for day in dates
    ]
    metadata = {day: [] for day in dates}
    missing = _event(
        filing_date=dates[19],
        entry_date=entry_date,
        symbol="MISSING",
        notional=500_000.0,
    )
    available = _event(
        filing_date=dates[19],
        entry_date=entry_date,
        symbol="TEST",
        notional=300_000.0,
    )
    metadata[entry_date] = [missing, available]
    dataset = runtime.prepare_dataset(
        {
            "schema_version": 1,
            "family_id": discovery.FAMILY_ID,
            "sample_phase": "development",
            "evaluation_dates": dates,
            "signal_dates": [entry_date],
            "event_metadata_by_date": metadata,
            "daily_bars": {"TEST": bars},
        }
    )

    candidates = runtime.build_candidates(
        dataset,
        discovery.FAMILY_ID,
        {
            "maximum_hold_sessions": 3,
            "maximum_prior_20_session_return_fraction": 0.0,
            "minimum_distinct_reporting_owners": 1,
            "minimum_purchase_notional": 50_000.0,
            "stop_atr14": 1.5,
        },
    )

    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "MISSING"
    assert candidates[0]["outcome"] == "missed_fill"
    assert (
        candidates[0]["rejection_reason"]
        == "missing_required_history_or_entry_open"
    )
