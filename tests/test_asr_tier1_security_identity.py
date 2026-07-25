from __future__ import annotations

import hashlib
from pathlib import Path

import asr_tier1_security_identity as tier1_identity


QUALIFIED = b"""
<SEC-DOCUMENT>
<DOCUMENT>
<TYPE>8-K
<TEXT><html><table>
<tr><th>Title of each class</th><th>Trading Symbol(s)</th>
<th>Name of each exchange on which registered</th></tr>
<tr><td>Common Stock</td><td>ABCD</td><td>Nasdaq</td></tr>
</table></html></TEXT>
</DOCUMENT>
"""


def test_contract_reuses_frozen_identity_rule_without_outcomes():
    contract = tier1_identity.build_contract()
    assert contract["source_lineage"]["semantic_event_count"] == 59
    assert contract["source_lineage"]["qualified_accession_count"] == 27
    assert (
        contract["identity_rule"]["frozen_contract_sha256"]
        == tier1_identity.IDENTITY_CONTRACT_SHA256
    )
    assert contract["identity_rule"]["rule_change_permitted"] is False
    assert (
        contract["capacity_contract"]["same_accession_events_are_one_trade_opportunity"]
        is True
    )
    assert contract["access_contract"]["new_provider_access_permitted"] is False
    assert contract["access_contract"]["market_price_access_permitted"] is False
    assert contract["verified_event_count"] is None
    assert contract["market_outcomes_accessed"] is False


def test_rebuild_reports_events_and_independent_disclosures(tmp_path: Path):
    source = tmp_path / "submission.txt"
    source.write_bytes(QUALIFIED)
    base = {
        "issuer_cik": "1234",
        "agreement_date": "2025-01-02",
        "committed_notional_dollars": 500_000_000,
        "acceptance_datetime_raw": "20250103120000",
        "accession": "0000001234-25-000001",
        "event_key_sha256": "e" * 64,
    }
    semantic_result = {
        "unique_events": [
            base,
            {
                **base,
                "committed_notional_dollars": 750_000_000,
                "event_key_sha256": "f" * 64,
            },
        ]
    }
    collection = {
        "records": [
            {
                "accession": base["accession"],
                "source_cache_relative_path": "submission.txt",
                "source_sha256": hashlib.sha256(QUALIFIED).hexdigest(),
                "source_bytes": len(QUALIFIED),
            }
        ]
    }
    result = tier1_identity.rebuild_result(semantic_result, collection, tmp_path)
    assert result["verified_event_count"] == 2
    assert result["independent_disclosure_signal_count"] == 1
    assert result["security_identity_resolution_complete"] is True
    assert result["market_outcomes_accessed"] is False
