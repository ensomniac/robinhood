import io
import zipfile

import earnings_sec_cover_identity as source


def _tsv(rows):
    fields = sorted({key for row in rows for key in row})
    output = io.StringIO()
    output.write("\t".join(fields) + "\n")
    for row in rows:
        output.write("\t".join(str(row.get(field, "")) for field in fields) + "\n")
    return output.getvalue()


def test_cover_fact_verifies_same_accession_common_equity(tmp_path):
    event = {
        "adsh": "0000000001-10-000001",
        "accepted": "2010-04-20 16:15:00",
        "accepted_date": "2010-04-20",
        "filed": "20100420",
        "period": "20100331",
        "ticker": "EDGE",
    }
    row = {
        "adsh": event["adsh"],
        "coreg": "",
        "ddate": "20100415",
        "dimn": "0",
        "iprx": "0",
        "qtrs": "0",
        "tag": source.COVER_TAG,
        "uom": "shares",
        "value": "1000000",
        "version": "dei/2009",
    }
    path = tmp_path / "cover.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("num.tsv", _tsv([row]))
    with zipfile.ZipFile(path) as archive:
        verified, reasons = source._cover_facts(archive, [event])

    assert len(verified) == 1
    assert verified[0]["security_identity_state"] == (
        "VERIFIED_COMMON_EQUITY_COVER_FACT"
    )
    assert verified[0]["common_stock_shares_outstanding"] == 1_000_000
    assert reasons["VERIFIED_COMMON_EQUITY_COVER_FACT"] == 1


def test_cover_fact_outside_period_to_filing_window_gets_zero_credit(tmp_path):
    event = {
        "adsh": "0000000001-10-000001",
        "accepted": "2010-04-20 16:15:00",
        "accepted_date": "2010-04-20",
        "filed": "20100420",
        "period": "20100331",
        "ticker": "EDGE",
    }
    row = {
        "adsh": event["adsh"],
        "coreg": "",
        "ddate": "20100501",
        "dimn": "0",
        "iprx": "0",
        "qtrs": "0",
        "tag": source.COVER_TAG,
        "uom": "shares",
        "value": "1000000",
        "version": "dei/2009",
    }
    path = tmp_path / "cover.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("num.tsv", _tsv([row]))
    with zipfile.ZipFile(path) as archive:
        verified, reasons = source._cover_facts(archive, [event])

    assert verified == []
    assert reasons["NO_ELIGIBLE_COMMON_STOCK_SHARES_FACT"] == 1
