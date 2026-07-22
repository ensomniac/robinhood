from __future__ import annotations

import json
from pathlib import Path

import pytest

import schedule13d_semantic_capacity as semantic
import schedule13d_semantic_capacity_inspection as inspection


def _request(ordinal: int = 0) -> dict[str, object]:
    return {
        "ordinal": ordinal,
        "request_sha256": "a" * 64,
        "filename": "edgar/data/1/0000000000-22-000001.txt",
        "cik": "1",
    }


def _submission(item4: str, *, symbol: str = "TEST") -> bytes:
    return f"""<SEC-DOCUMENT>
<ACCEPTANCE-DATETIME>20220103120000
<SUBJECT-COMPANY><COMPANY-DATA><CIK>0000001234</COMPANY-DATA></SUBJECT-COMPANY>
<TITLEOCLASS>Common Stock
<DOCUMENT><TYPE>SC 13D
<TEXT><html><body>
<issuerTradingSymbol>{symbol}</issuerTradingSymbol>
<p>Item 4. Purpose of Transaction</p>
<p>{item4}</p>
<p>Item 5. Interest in Securities of the Issuer</p>
</body></html></TEXT></DOCUMENT>
</SEC-DOCUMENT>""".encode()


def test_activation_freezes_semantic_parser_without_counts_or_outcomes():
    value = semantic.build_activation()
    assert value["denominator"]["indexed_initial_sc13d_filings"] == 6852
    assert value["denominator"]["verified_event_count"] is None
    assert value["parser_contract"]["same_paragraph_intent_required"] is True
    assert value["parser_contract"]["cooldown_calendar_days"] == 63
    assert value["access_contract"]["issuer_symbol_supplemental_access_permitted"] is False
    assert value["access_contract"]["market_price_access_permitted"] is False
    assert value["verified_event_count"] is None
    assert value["capacity_passed"] is None
    assert value["market_outcomes_accessed"] is False


def test_activation_inspection_opens_only_semantic_classification(tmp_path: Path):
    path, value = semantic.freeze_activation(
        root=tmp_path / "activations", status_path=tmp_path / "status.json"
    )
    result = inspection.inspect_activation(
        path, status_path=tmp_path / "status.json"
    )
    assert result["activation_sha256"] == value["activation_sha256"]
    assert result["classification_permitted"] is True
    assert result["issuer_symbol_supplemental_access_permitted"] is False
    assert result["verified_event_count"] is None
    assert result["market_price_access_permitted"] is False
    assert result["outcome_access_permitted"] is False


def test_activation_inspection_rejects_tampering(tmp_path: Path):
    path, value = semantic.freeze_activation(
        root=tmp_path / "activations", status_path=tmp_path / "status.json"
    )
    value["parser_contract"]["cooldown_calendar_days"] = 1
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
    with pytest.raises(semantic.Schedule13dSemanticCapacityError):
        inspection.inspect_activation(path, status_path=tmp_path / "status.json")


def test_base_classification_requires_same_paragraph_intent_and_category():
    passing = semantic._base_classification(
        _request(),
        _submission("The Reporting Persons intend to seek board representation."),
    )
    assert passing["base_semantic_candidate"] is True
    assert passing["event_symbol"] == "TEST"
    assert passing["control_category"] == "board_representation"
    split = semantic._base_classification(
        _request(),
        _submission(
            "The Reporting Persons intend to engage with the Issuer.</p>"
            "<p>Board representation remains under discussion."
        ),
    )
    assert split["base_semantic_candidate"] is False
    assert split["status"] == "ITEM4_NO_SAME_PARAGRAPH_CONTROL_CATEGORY"


def test_base_classification_rejects_amendment_type_and_ineligible_class():
    raw = _submission("The Reporting Persons intend to seek board representation.")
    amendment = raw.replace(b"<TYPE>SC 13D", b"<TYPE>SC 13D/A")
    assert semantic._base_classification(_request(), amendment)["status"] == (
        "MISSING_ACCESSION_BOUND_SC13D_DOCUMENT"
    )
    preferred = raw.replace(b"<TITLEOCLASS>Common Stock", b"<TITLEOCLASS>Preferred Stock")
    assert semantic._base_classification(_request(), preferred)["status"] == (
        "INELIGIBLE_SECURITY_CLASS"
    )


def test_base_classification_accepts_textual_sec_subject_header():
    raw = _submission(
        "The Reporting Persons intend to seek board representation."
    ).replace(
        b"<SUBJECT-COMPANY><COMPANY-DATA><CIK>0000001234</COMPANY-DATA></SUBJECT-COMPANY>",
        b"SUBJECT COMPANY:\n    COMPANY DATA:\n        CENTRAL INDEX KEY: 0000001234\n",
    )
    result = semantic._base_classification(_request(), raw)
    assert result["subject_cik"] == "1234"
    assert result["base_semantic_candidate"] is True


def test_event_rules_apply_same_day_and_63_day_cooldown_conservatively():
    rows = [
        {
            "base_semantic_candidate": True,
            "subject_cik": "1",
            "accepted_date": "2022-01-03",
            "accepted_at": "2022-01-03T10:00:00",
            "accession": "a",
            "event_symbol": None,
            "verified_event": False,
            "status": "PENDING_CAUSAL_SYMBOL_SUPPLEMENTAL",
        },
        {
            "base_semantic_candidate": True,
            "subject_cik": "1",
            "accepted_date": "2022-01-03",
            "accepted_at": "2022-01-03T11:00:00",
            "accession": "b",
            "event_symbol": "TST",
            "verified_event": False,
            "status": "POTENTIAL_EVENT_SYMBOL_ELIGIBLE",
        },
        {
            "base_semantic_candidate": True,
            "subject_cik": "1",
            "accepted_date": "2022-02-01",
            "accepted_at": "2022-02-01T10:00:00",
            "accession": "c",
            "event_symbol": "TST",
            "verified_event": False,
            "status": "POTENTIAL_EVENT_SYMBOL_ELIGIBLE",
        },
    ]
    semantic._apply_event_rules(rows)
    assert rows[0]["status"] == "PENDING_CAUSAL_SYMBOL_SUPPLEMENTAL"
    assert rows[1]["status"] == "SAME_ISSUER_ACCEPTANCE_DATE_DUPLICATE"
    assert rows[2]["status"] == "ISSUER_COOLDOWN"
    assert not any(row["verified_event"] for row in rows)
