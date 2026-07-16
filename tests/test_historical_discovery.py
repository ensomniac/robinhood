import json
import unittest

from historical_discovery import (
    _document_has_dilution,
    _ticker_map,
    extract_earnings_events,
    parse_master_index,
)


class HistoricalDiscoveryTests(unittest.TestCase):
    def test_extracts_connector_text_and_prefers_verified_duplicate(self):
        tentative = {
            "symbol": "abc",
            "year": 2026,
            "quarter": 1,
            "eps": {"estimate": "1.00", "actual": "1.10"},
            "report": {"date": "2026-03-03", "timing": "am", "verified": False},
        }
        verified = {
            **tentative,
            "report": {**tentative["report"], "verified": True},
        }
        payload = [
            {"content": [{"type": "text", "text": json.dumps({"data": {"results": [tentative]}})}]},
            {"data": {"results": [verified]}},
        ]

        events = extract_earnings_events(payload)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["symbol"], "ABC")
        self.assertTrue(events[0]["report"]["verified"])

    def test_parses_master_index_rows_after_header_separator(self):
        text = "header\n-----\n123|Example Corp|8-K|2026-03-03|edgar/data/123/a.txt\n"

        self.assertEqual(
            parse_master_index(text),
            [
                {
                    "cik": "123",
                    "company": "Example Corp",
                    "form": "8-K",
                    "filed_on": "2026-03-03",
                    "filename": "edgar/data/123/a.txt",
                }
            ],
        )

    def test_ticker_map_keeps_supported_primary_exchange_symbols_only(self):
        payload = {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [1, "Valid", "GOOD", "Nasdaq"],
                [2, "Class", "BRK-B", "NYSE"],
                [3, "OTC", "PINK", "OTC"],
            ],
        }

        self.assertEqual(
            _ticker_map(payload),
            {"1": [{"ticker": "GOOD", "exchange": "Nasdaq", "name": "Valid"}]},
        )

    def test_dilution_screen_uses_item_and_strong_document_language(self):
        self.assertTrue(_document_has_dilution("routine filing", ["3.02"]))
        self.assertTrue(
            _document_has_dilution("We entered an at-the-market offering.", ["8.01"])
        )
        self.assertFalse(_document_has_dilution("Quarterly operating results", ["2.02"]))


if __name__ == "__main__":
    unittest.main()
