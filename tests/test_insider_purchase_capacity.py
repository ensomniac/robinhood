from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import pytest

import insider_purchase_capacity as subject


def _tsv(rows: list[dict[str, object]]) -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(
        output, fieldnames=list(rows[0]), delimiter="\t", lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode()


def _archive(path: Path, *, accession: str = "0001-24-000001") -> None:
    submissions = [
        {
            "ACCESSION_NUMBER": accession,
            "FILING_DATE": "05-JAN-2024",
            "DOCUMENT_TYPE": "4",
            "ISSUERCIK": "123456",
            "ISSUERNAME": "Example Corp",
            "ISSUERTRADINGSYMBOL": "TEST",
            "AFF10B5ONE": "0",
        }
    ]
    owners = [
        {
            "ACCESSION_NUMBER": accession,
            "RPTOWNERCIK": "999",
            "RPTOWNER_RELATIONSHIP": "OFFICER",
        }
    ]
    transactions = [
        {
            "ACCESSION_NUMBER": accession,
            "NONDERIV_TRANS_SK": "1",
            "SECURITY_TITLE": "Common Stock",
            "SECURITY_TITLE_FN": "",
            "TRANS_DATE": "03-JAN-2024",
            "TRANS_DATE_FN": "",
            "TRANS_CODE": "P",
            "TRANS_SHARES": "1000",
            "TRANS_SHARES_FN": "",
            "TRANS_PRICEPERSHARE": "25",
            "TRANS_PRICEPERSHARE_FN": "",
            "TRANS_ACQUIRED_DISP_CD": "A",
            "TRANS_ACQUIRED_DISP_CD_FN": "",
            "DIRECT_INDIRECT_OWNERSHIP": "D",
            "DIRECT_INDIRECT_OWNERSHIP_FN": "",
        }
    ]
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("SUBMISSION.tsv", _tsv(submissions))
        archive.writestr("REPORTINGOWNER.tsv", _tsv(owners))
        archive.writestr("NONDERIV_TRANS.tsv", _tsv(transactions))


def test_contract_freezes_exact_quarter_graph(monkeypatch, tmp_path):
    monkeypatch.setattr(subject, "_store_root", lambda: tmp_path / "store")
    contract = subject.build_contract(created_at="2026-07-25T23:50:00-04:00")
    assert len(contract["source"]["requests"]) == 28
    assert contract["source"]["requests"][0]["filename"] == "2018q1_form345.zip"
    assert contract["source"]["requests"][-1]["filename"] == "2024q4_form345.zip"
    assert contract["outcome_boundary"]["market_prices_accessed"] is False
    assert contract["artifact_sha256"] == subject.self_hash(
        contract, "artifact_sha256"
    )


def test_normalization_retains_only_exact_open_market_common_purchase(tmp_path):
    archive = tmp_path / "sample.zip"
    _archive(archive)
    events, counts = subject.normalized_events([archive])
    assert counts["normalized_events"] == 1
    assert events == [
        {
            "accession_numbers": ["0001-24-000001"],
            "distinct_reporting_owners": 1,
            "filing_date": "2024-01-05",
            "issuer_cik": "123456",
            "issuer_name": "Example Corp",
            "owner_ciks": ["999"],
            "owner_relationships": ["OFFICER"],
            "purchase_notional": 25000.0,
            "symbol": "TEST",
            "transaction_count": 1,
            "transaction_dates": ["2024-01-03"],
        }
    ]


def test_normalization_excludes_10b5_and_footnoted_price(tmp_path):
    archive = tmp_path / "sample.zip"
    _archive(archive)
    with zipfile.ZipFile(archive) as source:
        submission = source.read("SUBMISSION.tsv").decode().replace(
            "\t0\n", "\t1\n"
        )
        owner = source.read("REPORTINGOWNER.tsv")
        transaction = source.read("NONDERIV_TRANS.tsv")
    with zipfile.ZipFile(archive, "w") as target:
        target.writestr("SUBMISSION.tsv", submission)
        target.writestr("REPORTINGOWNER.tsv", owner)
        target.writestr("NONDERIV_TRANS.tsv", transaction)
    events, counts = subject.normalized_events([archive])
    assert events == []
    assert counts["excluded_10b5_1"] == 1


def test_inspection_fails_closed_when_outputs_exist(monkeypatch, tmp_path):
    store = tmp_path / "store"
    monkeypatch.setattr(subject, "_store_root", lambda: store)
    monkeypatch.setattr(
        subject.strategy_discovery, "require_committed", lambda _path: None
    )
    root = tmp_path / "artifacts"
    path = subject.freeze_contract(
        created_at="2026-07-25T23:50:00-04:00", root=root
    )
    contract = json.loads(path.read_text())
    destination = (
        store
        / "_derived/form4_insider_purchase"
        / contract["artifact_sha256"]
    )
    destination.mkdir(parents=True)
    (destination / "unexpected").write_text("x")
    with pytest.raises(
        subject.InsiderPurchaseCapacityError,
        match="failed independent inspection",
    ):
        subject.inspect_contract(
            path,
            inspected_at="2026-07-25T23:51:00-04:00",
            root=root,
        )
