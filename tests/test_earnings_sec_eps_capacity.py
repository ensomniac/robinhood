import io
import zipfile
from pathlib import Path

import earnings_sec_eps_capacity as source
import earnings_sec_eps_capacity_inspection as inspection
import earnings_sec_eps_failure_inspection as failure_inspection
from historical_store import HistoricalDayStore


def _commit_bypass(monkeypatch):
    monkeypatch.setattr(
        source.strategy_discovery, "require_committed", lambda _path: None
    )
    monkeypatch.setattr(
        inspection.strategy_discovery, "require_committed", lambda _path: None
    )


def _tsv(rows):
    fields = sorted({key for row in rows for key in row})
    output = io.StringIO()
    output.write("\t".join(fields) + "\n")
    for row in rows:
        output.write("\t".join(str(row.get(field, "")) for field in fields) + "\n")
    return output.getvalue().encode()


def _archive(path: Path, *, accepted: str = "2010-04-20 16:15:00"):
    adsh = "0000000001-10-000001"
    empty_dimh = "d41d8cd98f00b204e9800998ecf8427e"
    sub = [
        {
            "adsh": adsh,
            "accepted": accepted,
            "cik": "1",
            "filed": accepted[:10].replace("-", ""),
            "form": "10-Q",
            "fp": "Q1",
            "fy": accepted[:4],
            "name": "EDGE CORP",
            "period": "20100331" if accepted[:4] == "2010" else "20110331",
            "sic": "3571",
        }
    ]
    dimh = "class-a"
    txt = [
        {
            "adsh": adsh,
            "dimh": dimh,
            "iprx": "1",
            "tag": "TradingSymbol",
            "value": "EDGE",
        },
        {
            "adsh": adsh,
            "dimh": dimh,
            "iprx": "1",
            "tag": "SecurityExchangeName",
            "value": "NASDAQ",
        },
        {
            "adsh": adsh,
            "dimh": dimh,
            "iprx": "1",
            "tag": "Security12bTitle",
            "value": "Class A Common Stock",
        },
    ]
    current = "20100331" if accepted[:4] == "2010" else "20110331"
    prior = "20090331" if accepted[:4] == "2010" else "20100331"
    num = [
        {
            "adsh": adsh,
            "coreg": "",
            "ddate": current,
            "dimh": empty_dimh,
            "dimn": "0",
            "iprx": "1",
            "qtrs": "1",
            "tag": "EarningsPerShareDiluted",
            "uom": "USD/shares",
            "value": "1.25",
        },
        {
            "adsh": adsh,
            "coreg": "",
            "ddate": prior,
            "dimh": empty_dimh,
            "dimn": "0",
            "iprx": "1",
            "qtrs": "1",
            "tag": "EarningsPerShareDiluted",
            "uom": "USD/shares",
            "value": "1.00",
        },
    ]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sub.tsv", _tsv(sub))
        archive.writestr("txt.tsv", _tsv(txt))
        archive.writestr("num.tsv", _tsv(num))


def test_contract_freezes_exact_archives_and_zero_outcomes(monkeypatch):
    _commit_bypass(monkeypatch)
    contract = source.build_contract(created_at="2026-07-25T02:00:00Z")

    assert len(contract["requests"]) == 8
    assert contract["requests"][0]["filename"] == "2010q1_notes_1.zip"
    assert contract["requests"][-1]["filename"] == "2011q4_notes.zip"
    assert (
        contract["event_semantics"][
            "external_or_current_ticker_mapping_permitted"
        ]
        is False
    )
    assert contract["partitions"] == {
        "development": ["2010-01-01", "2010-12-17"],
        "embargo": ["2010-12-18", "2011-01-09"],
        "confirmation": ["2011-01-10", "2011-12-31"],
        "confirmation_market_outcomes_remain_untouched": True,
    }
    assert contract["market_prices_accessed"] is False
    assert contract["forward_returns_accessed"] is False
    assert contract["broker_actions"] == 0


def test_archive_parser_requires_same_accession_identity_and_positive_yoy_eps(
    tmp_path,
):
    path = tmp_path / "quarter.zip"
    _archive(path)

    events, counts = source.derive_archive_events(path)

    assert counts == {
        "eligible_10q_submissions": 1,
        "resolved_common_equity_identities": 1,
        "positive_yoy_eps_events": 1,
    }
    assert events[0]["ticker"] == "EDGE"
    assert events[0]["eps_yoy_change"] == 0.25
    assert events[0]["current_eps"] == 1.25
    assert events[0]["prior_year_eps"] == 1.0


def test_archive_parser_streams_large_irrelevant_text_fact(tmp_path):
    path = tmp_path / "quarter.zip"
    _archive(path)
    with zipfile.ZipFile(path) as existing:
        sub = existing.read("sub.tsv")
        num = existing.read("num.tsv")
        original_txt = existing.read("txt.tsv").decode().splitlines()
    header = original_txt[0].split("\t")
    large = {field: "" for field in header}
    large.update(
        {
            "adsh": "0000000001-10-000001",
            "dimh": "large-text",
            "iprx": "1",
            "tag": "DisclosureTextBlock",
            "value": "x" * 131_073,
        }
    )
    original_rows = [
        dict(zip(header, line.split("\t"), strict=True))
        for line in original_txt[1:]
    ]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sub.tsv", sub)
        archive.writestr("num.tsv", num)
        archive.writestr("txt.tsv", _tsv([large, *original_rows]))

    events, counts = source.derive_archive_events(path)

    assert len(events) == 1
    assert counts["positive_yoy_eps_events"] == 1


def test_identity_with_multiple_common_tickers_is_rejected(tmp_path):
    path = tmp_path / "quarter.zip"
    _archive(path)
    with zipfile.ZipFile(path) as existing:
        sub = existing.read("sub.tsv")
        num = existing.read("num.tsv")
    rows = [
        {
            "adsh": "0000000001-10-000001",
            "dimh": dimh,
            "iprx": "1",
            "tag": tag,
            "value": value,
        }
        for dimh, ticker in (("class-a", "EDGE"), ("class-b", "EDGEB"))
        for tag, value in (
            ("TradingSymbol", ticker),
            ("SecurityExchangeName", "NASDAQ"),
            ("Security12bTitle", "Common Stock"),
        )
    ]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sub.tsv", sub)
        archive.writestr("num.tsv", num)
        archive.writestr("txt.tsv", _tsv(rows))

    events, counts = source.derive_archive_events(path)

    assert events == []
    assert counts["resolved_common_equity_identities"] == 0


class _Response:
    status_code = 200
    headers = {"ETag": '"fixture"', "Last-Modified": "now"}

    def __init__(self, payload):
        self.payload = payload

    def iter_content(self, chunk_size):
        del chunk_size
        yield self.payload


class _Session:
    def __init__(self, payload):
        self.payload = payload
        self.headers = {}
        self.calls = []

    def get(self, url, *, stream, timeout):
        self.calls.append((url, stream, timeout))
        return _Response(self.payload)

    def close(self):
        pass


def test_collection_resumes_from_valid_archives_without_duplicate_requests(
    tmp_path, monkeypatch
):
    _commit_bypass(monkeypatch)
    monkeypatch.setattr(source, "DEFAULT_ROOT", tmp_path / "public")
    monkeypatch.setattr(inspection.source, "DEFAULT_ROOT", tmp_path / "public")
    monkeypatch.setattr(source, "_repo_path", lambda path: str(path))
    monkeypatch.setattr(inspection.source, "_repo_path", lambda path: str(path))
    monkeypatch.setattr(
        source.SecConfig,
        "from_env",
        lambda *_args, **_kwargs: type("Config", (), {"user_agent": "test@example.com"})(),
    )
    monkeypatch.setattr(
        source.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "a" * 64},
    )
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "a" * 64},
    )
    archive = tmp_path / "fixture.zip"
    _archive(archive)
    payload = archive.read_bytes()
    store = HistoricalDayStore(tmp_path / "private")
    contract_path, _ = source.freeze_contract(
        created_at="2026-07-25T02:00:00Z", root=tmp_path / "public"
    )
    inspection_path, _ = inspection.inspect_contract(
        contract_path,
        inspected_at="2026-07-25T02:01:00Z",
        root=tmp_path / "public",
    )
    session = _Session(payload)
    collection_path, first = source.collect(
        contract_path,
        inspection_path,
        collected_at="2026-07-25T02:02:00Z",
        store=store,
        session=session,
        minimum_spacing_seconds=0,
    )
    second_session = _Session(payload)
    _second_path, second = source.collect(
        contract_path,
        inspection_path,
        collected_at="2026-07-25T02:03:00Z",
        store=store,
        session=second_session,
        minimum_spacing_seconds=0,
    )

    assert len(session.calls) == 8
    assert len(second_session.calls) == 0
    assert first["provider_telemetry"]["request_count"] == 8
    assert second["provider_telemetry"]["cache_hits"] == 8
    assert collection_path.exists()


def test_failure_inspector_reproduces_default_csv_field_limit(tmp_path):
    path = tmp_path / "large-field.zip"
    large_value = "x" * 131_073
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "txt.tsv",
            _tsv(
                [
                    {
                        "adsh": "0000000001-10-000001",
                        "tag": "DisclosureTextBlock",
                        "value": large_value,
                    }
                ]
            ),
        )

    assert failure_inspection.reproduce_csv_limit(path) == (
        "field larger than field limit (131072)"
    )
