import outcome_exposure
import sec_earnings_full_inventory_recovery as subject


def _record(scope):
    return outcome_exposure.build_record(
        exposure_id="test-exposure",
        campaign_id="test-campaign",
        lane="development",
        recorded_at="2026-07-26T00:00:00+00:00",
        source_path="tests/source.json",
        source_sha256="a" * 64,
        scope=scope,
    )


def test_fast_pair_membership_matches_cartesian_scope():
    records = [
        _record(
            {
                "dates": ["2024-01-02", "2024-01-03"],
                "symbols": ["AAA", "BBB"],
            }
        )
    ]
    assert subject._pair_exposed_fast("2024-01-02", "AAA", records)
    assert subject._pair_exposed_fast("2024-01-03", "BBB", records)
    assert not subject._pair_exposed_fast("2024-01-04", "AAA", records)
    assert not subject._pair_exposed_fast("2024-01-02", "CCC", records)


def test_fast_pair_membership_matches_symbols_by_date_and_wildcard():
    records = [
        _record(
            {
                "dates": ["2024-01-02", "2024-01-03"],
                "symbols_by_date": {
                    "2024-01-02": ["AAA"],
                    "2024-01-03": ["*"],
                }
            }
        )
    ]
    assert subject._pair_exposed_fast("2024-01-02", "AAA", records)
    assert not subject._pair_exposed_fast("2024-01-02", "BBB", records)
    assert subject._pair_exposed_fast("2024-01-03", "ANY", records)
