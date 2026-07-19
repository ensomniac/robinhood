import unittest
from datetime import datetime

from nasdaq_halts import EASTERN, halt_overlaps, parse_halt_html


HTML = """
<div class="genTable"><table>
<tr><th>Halt Date</th><th>Halt Time</th><th>Issue Symbol</th><th>Issue Name</th>
<th>Market</th><th>Reason Code</th><th>Pause Threshold Price</th>
<th>Resumption Date</th><th>Resumption Quote Time</th><th>Resumption Trade Time</th></tr>
<tr><td>01/02/2026</td><td>09:30:40</td><td>TEST</td><td>Test Common</td>
<td>NASDAQ</td><td>LUDP</td><td></td><td>01/02/2026</td><td>09:30:40</td><td>09:35:40</td></tr>
</table></div>
"""


class NasdaqHaltTests(unittest.TestCase):
    def test_parse_and_interval_overlap(self):
        records = parse_halt_html(HTML)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["symbol"], "TEST")
        self.assertTrue(
            halt_overlaps(
                records[0],
                "TEST",
                datetime(2026, 1, 2, 9, 35, tzinfo=EASTERN),
                datetime(2026, 1, 2, 9, 35, 10, tzinfo=EASTERN),
            )
        )
        self.assertFalse(
            halt_overlaps(
                records[0],
                "TEST",
                datetime(2026, 1, 2, 9, 36, tzinfo=EASTERN),
                datetime(2026, 1, 2, 9, 36, 10, tzinfo=EASTERN),
            )
        )

    def test_header_change_fails_closed(self):
        with self.assertRaisesRegex(Exception, "header changed"):
            parse_halt_html(HTML.replace("Reason Code", "Reason"))

    def test_fractional_seconds_are_source_compatible(self):
        records = parse_halt_html(
            HTML.replace("09:30:40", "09:30:40.000").replace("09:35:40", "09:35:40.125")
        )
        self.assertEqual(records[0]["halted_at_et"], "2026-01-02T09:30:40-05:00")
        self.assertEqual(
            records[0]["resumed_at_et"], "2026-01-02T09:35:40.125000-05:00"
        )


if __name__ == "__main__":
    unittest.main()
