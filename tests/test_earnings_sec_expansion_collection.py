import hashlib
import io
import zipfile
from pathlib import Path

import earnings_sec_expansion_capacity as capacity
import earnings_sec_expansion_collection as source
import earnings_sec_expansion_collection_inspection as inspection


def _commit_bypass(monkeypatch):
    monkeypatch.setattr(
        source.strategy_discovery, "require_committed", lambda _path: None
    )
    monkeypatch.setattr(
        inspection.strategy_discovery, "require_committed", lambda _path: None
    )


def _lineage_bypass(monkeypatch):
    contract = {
        "contract_sha256": "a" * 64,
        "provider": "SEC",
        "requests": capacity.requests(),
        "event_semantics": {
            "event_rank": [
                "descending EPS change ratio",
                "descending EPS absolute change",
                "canonical ticker",
                "accession",
            ]
        },
        "partitions": {
            "development": [
                capacity.DEVELOPMENT_START,
                capacity.DEVELOPMENT_END,
            ],
            "embargo": [capacity.EMBARGO_START, capacity.EMBARGO_END],
            "confirmation": [
                capacity.CONFIRMATION_START,
                capacity.CONFIRMATION_END,
            ],
        },
    }
    inspected = {"inspection_sha256": "b" * 64}
    monkeypatch.setattr(source, "_lineage", lambda: (contract, inspected))
    monkeypatch.setattr(source, "sha256_file", lambda _path: "c" * 64)
    return contract


def _tsv(rows):
    fields = sorted({key for row in rows for key in row})
    output = io.StringIO()
    output.write("\t".join(fields) + "\n")
    for row in rows:
        output.write(
            "\t".join(str(row.get(field, "")) for field in fields) + "\n"
        )
    return output.getvalue().encode()


def _archive(path: Path, *, ticker: str = "EDGE", ratio: float = 0.5):
    adsh = f"0000000001-12-{ticker.lower():0>6}"
    accepted = "2012-04-20 16:15:00"
    period = "20120331"
    prior = "20110331"
    current_eps = 1.0 + ratio
    sub = [
        {
            "adsh": adsh,
            "accepted": accepted,
            "cik": "1",
            "filed": "20120420",
            "form": "10-Q",
            "fp": "Q1",
            "fy": "2012",
            "name": f"{ticker} CORP",
            "period": period,
            "sic": "3571",
        }
    ]
    txt = [
        {
            "adsh": adsh,
            "dimh": "class-a",
            "dimn": "0",
            "iprx": "0",
            "tag": "TradingSymbol",
            "value": ticker,
        }
    ]
    num = [
        {
            "adsh": adsh,
            "coreg": "",
            "ddate": period,
            "dimh": "d41d8cd98f00b204e9800998ecf8427e",
            "dimn": "0",
            "iprx": "0",
            "qtrs": "1",
            "tag": "EarningsPerShareDiluted",
            "uom": "USD",
            "value": str(current_eps),
            "version": "us-gaap/2012",
        },
        {
            "adsh": adsh,
            "coreg": "",
            "ddate": prior,
            "dimh": "d41d8cd98f00b204e9800998ecf8427e",
            "dimn": "0",
            "iprx": "0",
            "qtrs": "1",
            "tag": "EarningsPerShareDiluted",
            "uom": "USD",
            "value": "1.0",
            "version": "us-gaap/2012",
        },
        {
            "adsh": adsh,
            "coreg": "",
            "ddate": period,
            "dimh": "d41d8cd98f00b204e9800998ecf8427e",
            "dimn": "0",
            "iprx": "0",
            "qtrs": "0",
            "tag": "EntityCommonStockSharesOutstanding",
            "uom": "shares",
            "value": "1000000",
            "version": "dei/2012",
        },
    ]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sub.tsv", _tsv(sub))
        archive.writestr("txt.tsv", _tsv(txt))
        archive.writestr("num.tsv", _tsv(num))


def test_collection_plan_freezes_exact_inspected_graph(monkeypatch):
    _commit_bypass(monkeypatch)
    _lineage_bypass(monkeypatch)

    plan = source.build_plan(created_at="2026-07-25T07:30:00Z")

    assert plan["request_count"] == 32
    assert plan["requests"] == capacity.requests()
    assert plan["transport"]["retries_permitted"] == 0
    assert plan["transport"]["substitutions_permitted"] == 0
    assert (
        plan["derivation"]["exposed_ranked_event_substitution_permitted"]
        is False
    )
    assert plan["market_prices_accessed"] is False
    assert plan["broker_actions"] == 0


def test_rank_and_cover_uses_frozen_rank_and_common_stock_gate(tmp_path):
    paths = []
    for ticker, ratio in (
        ("EDGE", 0.50),
        ("ALFA", 0.75),
        ("BETA", 0.25),
        ("GAMM", 1.00),
    ):
        path = tmp_path / f"{ticker}.zip"
        _archive(path, ticker=ticker, ratio=ratio)
        paths.append(path)

    events, counts, summary = source._rank_and_cover(paths)

    assert len(counts) == 4
    assert [row["ticker"] for row in events] == ["GAMM", "ALFA", "EDGE"]
    assert [row["accepted_date_rank"] for row in events] == [1, 2, 3]
    assert all(
        row["security_identity_state"]
        == "VERIFIED_COMMON_EQUITY_COVER_FACT"
        for row in events
    )
    assert summary["ranked_event_rows"] == 3
    assert summary["verified_common_equity_events"] == 3


def test_inspection_filters_prior_exposure_without_substitution(monkeypatch):
    event = {
        "accepted": "2012-04-20 16:15:00",
        "ticker": "EDGE",
    }
    monkeypatch.setattr(
        inspection.outcome_exposure,
        "read_index",
        lambda: [
            inspection.outcome_exposure.build_record(
                exposure_id="prior",
                campaign_id=capacity.CAMPAIGN_ID,
                lane="development",
                recorded_at="2026-07-25T07:00:00Z",
                source_path="prior.json",
                source_sha256="a" * 64,
                scope={"dates": ["2012-04-23"], "symbols": ["EDGE"]},
            )
        ],
    )

    admitted, exposed = inspection._scope_for_events(
        [event], ["2012-04-20", "2012-04-23"]
    )

    assert admitted == []
    assert exposed == 1


def test_plan_request_hashes_remain_valid(monkeypatch):
    _commit_bypass(monkeypatch)
    _lineage_bypass(monkeypatch)
    plan = source.build_plan(created_at="2026-07-25T07:30:00Z")

    for row in plan["requests"]:
        content = {
            key: value
            for key, value in row.items()
            if key != "request_sha256"
        }
        assert row["request_sha256"] == hashlib.sha256(
            capacity.canonical_bytes(content)
        ).hexdigest()
