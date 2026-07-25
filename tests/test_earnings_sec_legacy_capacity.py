import io
import zipfile

import earnings_sec_legacy_capacity as source


def _tsv(rows):
    fields = sorted({key for row in rows for key in row})
    output = io.StringIO()
    output.write("\t".join(fields) + "\n")
    for row in rows:
        output.write("\t".join(str(row.get(field, "")) for field in fields) + "\n")
    return output.getvalue()


def _legacy_archive(path):
    adsh = "0000000001-10-000001"
    sub = [
        {
            "adsh": adsh,
            "accepted": "2010-04-20 16:15:00",
            "cik": "1",
            "filed": "20100420",
            "form": "10-Q",
            "fp": "Q1",
            "fy": "2010",
            "name": "EDGE CORP",
            "period": "20100331",
            "sic": "3571",
        }
    ]
    txt = [
        {
            "adsh": adsh,
            "dimh": "0x00000000",
            "dimn": "0",
            "iprx": "0",
            "tag": "TradingSymbol",
            "value": "EDGE",
        }
    ]
    num = [
        {
            "adsh": adsh,
            "coreg": "",
            "ddate": ddate,
            "dimh": "0x00000000",
            "dimn": "0",
            "iprx": "0",
            "qtrs": "1",
            "tag": "EarningsPerShareDiluted",
            "uom": "USD",
            "value": value,
        }
        for ddate, value in (("20100331", "1.25"), ("20090331", "1.00"))
    ]
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("sub.tsv", _tsv(sub))
        archive.writestr("txt.tsv", _tsv(txt))
        archive.writestr("num.tsv", _tsv(num))


def test_legacy_parser_retains_provisional_positive_eps_event(tmp_path):
    path = tmp_path / "legacy.zip"
    _legacy_archive(path)

    events, counts = source.derive_archive(path)

    assert counts["provisional_single_ticker_identities"] == 1
    assert counts["positive_yoy_eps_events"] == 1
    assert events[0]["ticker"] == "EDGE"
    assert events[0]["security_identity_state"] == (
        "PROVISIONAL_TRADING_SYMBOL_ONLY"
    )
    assert events[0]["eps_yoy_change_ratio"] == 0.25


def test_provisional_ranking_is_deterministic_and_capped():
    events = [
        {
            "accepted": "2010-04-20 16:15:00",
            "ticker": f"E{index}",
            "adsh": f"A{index}",
            "eps_yoy_change": float(index),
            "eps_yoy_change_ratio": float(index),
        }
        for index in range(5)
    ]

    ranked = source._rank(events)

    assert [event["ticker"] for event in ranked] == ["E4", "E3", "E2"]
    assert [event["provisional_rank"] for event in ranked] == [1, 2, 3]


def test_legacy_contract_keeps_prices_and_cover_access_closed(monkeypatch):
    monkeypatch.setattr(
        source.strategy_discovery, "require_committed", lambda _path: None
    )
    contract = source.build_contract(created_at="2026-07-25T03:00:00Z")

    assert contract["provider_request_contract"][
        "additional_provider_requests_permitted"
    ] == 0
    assert contract["observed_legacy_encoding"]["eps_uom"] == "USD"
    assert contract["observed_legacy_encoding"]["eps_iprx"] == 0
    assert contract["next_transition"]["filing_cover_contract_required"] is True
    assert contract["next_transition"]["market_price_access_permitted"] is False
    assert contract["market_prices_accessed"] is False
    assert contract["broker_actions"] == 0
