from __future__ import annotations

import gzip
import json

import asr_submission_tier as tier


def test_high_precision_private_graph_rebuilds_without_provider_access():
    graph, path = tier.build_private_graph()
    assert graph["selection_phrase_sha256"] == tier.HIGH_PRECISION_PHRASE_SHA256
    assert graph["selected_hit_count"] == 204
    assert graph["selected_accession_count"] == 201
    assert graph["candidate_url_count"] == 248
    assert graph["unselected_unique_hit_count"] == 17_278
    assert len(graph["requests"]) == 201
    assert sum(len(row["candidates"]) for row in graph["requests"]) == 248
    assert graph["market_outcomes_accessed"] is False
    assert graph["broker_actions"] == 0
    assert path.name == f"{graph['private_graph_sha256']}.json.gz"


def test_contract_freezes_staged_capacity_and_no_outcomes():
    contract, graph, _path = tier.build_contract()
    assert graph["selected_hit_count"] == 204
    assert contract["selection_contract"]["selected_accession_count"] == 201
    assert contract["selection_contract"]["candidate_url_count"] == 248
    assert (
        contract["selection_contract"]["unselected_terminal_reason"]
        == "OUTSIDE_FROZEN_HIGH_PRECISION_TIER_UNRESOLVED"
    )
    assert contract["staged_capacity_contract"][
        "tier_2_may_open_only_if_verified_unique_events_below"
    ] == 100
    assert contract["request_contract"]["candidate_404_action"] == (
        "try_next_frozen_candidate"
    )
    assert contract["access_contract"]["filing_semantic_classification_permitted"] is False
    assert contract["access_contract"]["market_price_access_permitted"] is False
    assert contract["verified_event_count"] is None
    assert contract["market_outcomes_accessed"] is False


def test_deterministic_private_gzip_round_trip(tmp_path):
    value = {"schema_version": 1, "rows": [{"ordinal": 1}]}
    path = tmp_path / "graph.json.gz"
    tier._write_gzip(value, path)
    first = path.read_bytes()
    tier._write_gzip(value, path)
    assert path.read_bytes() == first
    assert json.loads(gzip.decompress(first)) == value
