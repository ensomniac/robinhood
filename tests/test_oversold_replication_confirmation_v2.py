from __future__ import annotations

import json

import oversold_replication_confirmation_v2 as confirmation


def test_candidate_graph_requires_exact_dates() -> None:
    detail = {
        "dates": {
            "2026-07-17": {"evaluations": []},
        }
    }
    try:
        confirmation._candidate_graph(
            detail,
            ["2026-07-16", "2026-07-17"],
        )
    except confirmation.OversoldReplicationConfirmationV2Error as exc:
        assert "exact reserve" in str(exc)
    else:
        raise AssertionError("missing scanner date must fail closed")


def test_private_inventory_gzip_is_deterministic(tmp_path) -> None:
    path = tmp_path / "inventory.json.gz"
    value = {"b": 2, "a": 1}
    confirmation._write_gzip(path, value)
    first = path.read_bytes()
    confirmation._write_gzip(path, json.loads('{"a":1,"b":2}'))
    assert path.read_bytes() == first
    assert confirmation._read_gzip(path) == value
