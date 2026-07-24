from __future__ import annotations

import json

import etf_sector_pullback_replication as replication


def test_v5_replication_preserves_exact_grid_dates_and_clean_symbols():
    base = json.loads(
        replication.BASE_CONTRACT.read_text(encoding="utf-8")
    )

    assert replication.SYMBOLS == [
        "VAW",
        "VCR",
        "VDC",
        "VDE",
        "VFH",
        "VGT",
        "VHT",
        "VIS",
        "VOX",
        "VPU",
    ]
    assert len(base["trial_family"]) == 32
    assert len(base["development_dates"]) == 1_000
    assert len(base["confirmation_dates"]) == 500
    assert base["development_dates"][-1] < base["embargo_dates"][0]
    assert base["embargo_dates"][-1] < base["confirmation_dates"][0]


def test_v5_adverse_predecessor_graph_is_terminal_and_hash_bound():
    base, search, result, inspection = replication._predecessor_graph(
        enforce_commit=True
    )

    assert base["family_id"] != replication.FAMILY_ID
    assert result["search_sha256"] == search["artifact_sha256"]
    assert inspection["result_sha256"] == result["artifact_sha256"]
    assert inspection["state"] == "REJECTED"
    assert inspection["selection"]["selected_trial_id"] is None
