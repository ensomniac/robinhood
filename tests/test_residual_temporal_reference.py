from __future__ import annotations

import residual_temporal_reference as reference
import residual_temporal_reference_inspection as inspection


def test_temporal_reference_dates_are_deterministic_full_span():
    dates = reference.selected_dates()

    assert len(dates) == len(set(dates)) == 200
    assert dates == sorted(dates)
    assert dates[0] == "2021-01-04"
    assert dates[-1] == "2022-12-30"
    assert any(day.startswith("2021-") for day in dates)
    assert any(day.startswith("2022-") for day in dates)


def test_identity_builder_scopes_duplicate_composite_listings():
    fillers = [
        {
            "ticker": f"S{index:03d}",
            "primary_exchange": "XNYS",
            "type": "CS",
            "active": True,
            "composite_figi": f"UNIQUE-{index:03d}",
        }
        for index in range(498)
    ]
    snapshots = {
        "2021-01-04": [
            {
                "ticker": "AAA",
                "primary_exchange": "XNYS",
                "type": "CS",
                "active": True,
                "composite_figi": "DUPLICATE",
            },
            {
                "ticker": "AAB",
                "primary_exchange": "XNAS",
                "type": "CS",
                "active": True,
                "composite_figi": "DUPLICATE",
            },
            {
                "ticker": "ETF",
                "primary_exchange": "XNYS",
                "type": "ETF",
                "active": True,
                "composite_figi": "ETF-ID",
            },
            *fillers,
        ]
    }

    identities = inspection.build_identities(snapshots)

    assert {"AAA", "AAB"} <= set(identities["2021-01-04"])
    assert len(set(identities["2021-01-04"].values())) == 500
    assert all(
        ":LISTING:" in value
        for value in (
            identities["2021-01-04"]["AAA"],
            identities["2021-01-04"]["AAB"],
        )
    )


def test_identity_builder_excludes_noncanonical_when_issued_symbols():
    fillers = [
        {
            "ticker": f"S{index:03d}",
            "primary_exchange": "XNYS",
            "type": "CS",
            "active": True,
            "composite_figi": f"UNIQUE-{index:03d}",
        }
        for index in range(499)
    ]
    snapshots = {
        "2021-09-15": [
            {
                "ticker": "IPW",
                "primary_exchange": "XNAS",
                "type": "CS",
                "active": True,
                "composite_figi": "IPOWER",
            },
            {
                "ticker": "IPw",
                "primary_exchange": "XNYS",
                "type": "CS",
                "active": True,
                "name": "International Paper Company",
            },
            *fillers,
        ]
    }

    identities = inspection.build_identities(snapshots)

    assert len(identities["2021-09-15"]) == 500
    assert identities["2021-09-15"]["IPW"] == "FIGI-COMPOSITE:IPOWER"
