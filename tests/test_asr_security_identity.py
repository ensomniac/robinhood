from __future__ import annotations

import hashlib
from pathlib import Path

import asr_security_identity as identity


def _submission(rows: str) -> bytes:
    return f"""
<SEC-DOCUMENT>
<ACCEPTANCE-DATETIME>20250103120000
<DOCUMENT>
<TYPE>8-K
<TEXT>
<html><body><table>
<tr><th>Title of each class</th><th>Trading Symbol(s)</th>
<th>Name of each exchange on which registered</th></tr>
{rows}
</table></body></html>
</TEXT>
</DOCUMENT>
""".encode()


QUALIFIED = _submission(
    "<tr><td>Common Stock, $0.01 par value</td><td>ABCD</td>"
    "<td>The Nasdaq Stock Market LLC</td></tr>"
)


def test_cover_classifier_accepts_one_us_listed_common_equity():
    values, terminal = identity.classify_security_identity(QUALIFIED)
    assert terminal == "QUALIFIED_UNAMBIGUOUS_COMMON_EQUITY"
    assert values == [
        {
            "security_title": "Common Stock, $0.01 par value",
            "ticker": "ABCD",
            "exchange": "NASDAQ",
        }
    ]


def test_cover_classifier_rejects_multiple_common_classes_as_ambiguous():
    raw = _submission(
        "<tr><td>Class A Common Stock</td><td>AAA</td><td>NYSE</td></tr>"
        "<tr><td>Class B Common Stock</td><td>BBB</td><td>NYSE</td></tr>"
    )
    values, terminal = identity.classify_security_identity(raw)
    assert len(values) == 2
    assert terminal == "AMBIGUOUS_MULTIPLE_COMMON_EQUITY_IDENTITIES"


def test_cover_classifier_rejects_non_us_or_disallowed_security():
    raw = _submission(
        "<tr><td>Preferred Stock</td><td>PREF</td><td>NYSE</td></tr>"
        "<tr><td>Common Stock Purchase Rights</td><td>RIGHT</td>"
        "<td>OTC Markets</td></tr>"
    )
    values, terminal = identity.classify_security_identity(raw)
    assert values == []
    assert terminal == "COMMON_TITLE_WITHOUT_ELIGIBLE_US_LISTING"


def test_contract_locks_identity_and_capacity_before_local_access():
    contract = identity.build_contract()
    assert contract["source_lineage"]["precise_semantic_event_count"] == 507
    assert (
        contract["identity_contract"][
            "exactly_one_eligible_identity_required_per_accession"
        ]
        is True
    )
    assert (
        contract["identity_contract"]["external_identity_substitution_permitted"]
        is False
    )
    assert contract["capacity_contract"]["fast_lane_threshold"] == 100
    assert contract["access_contract"]["new_provider_access_permitted"] is False
    assert contract["access_contract"]["market_price_access_permitted"] is False
    assert contract["verified_event_count"] is None
    assert contract["market_outcomes_accessed"] is False


def test_identity_rebuild_deduplicates_verified_event(tmp_path: Path):
    source = tmp_path / "submission.txt"
    source.write_bytes(QUALIFIED)
    event = {
        "issuer_cik": "1234",
        "agreement_date": "2025-01-02",
        "committed_notional_dollars": 500_000_000,
        "acceptance_datetime_raw": "20250103120000",
        "accession": "0000001234-25-000001",
        "event_key_sha256": "e" * 64,
    }
    private = {"events": [event, {**event, "event_key_sha256": "f" * 64}]}
    collection = {
        "records": [
            {
                "accession": event["accession"],
                "source_cache_relative_path": "submission.txt",
                "source_sha256": hashlib.sha256(QUALIFIED).hexdigest(),
                "source_bytes": len(QUALIFIED),
            }
        ]
    }
    result = identity.rebuild_result(private, collection, tmp_path)
    assert result["verified_event_count"] == 1
    assert result["capacity_disposition"] == ("RETIRED_INSUFFICIENT_FORMAL_CAPACITY")
    assert result["verified_events"][0]["ticker"] == "ABCD"
    assert result["market_outcomes_accessed"] is False
