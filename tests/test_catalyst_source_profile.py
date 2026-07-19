import unittest

from catalyst_source_profile import _StructureParser


class CatalystSourceProfileTests(unittest.TestCase):
    def test_structure_parser_retains_candidates_without_accepting_them(self):
        parser = _StructureParser()
        parser.feed(
            """<html><head><title>Example</title>
            <link rel="canonical" href="https://issuer.example/release">
            <meta property="article:published_time" content="2026-01-02T08:00:00Z">
            </head><body><time datetime="2026-01-02">Jan 2</time></body></html>"""
        )
        self.assertEqual("".join(parser.title_parts), "Example")
        self.assertEqual(parser.canonical_urls, ["https://issuer.example/release"])
        self.assertEqual(
            parser.meta_timestamps,
            [
                {
                    "key": "article:published_time",
                    "value": "2026-01-02T08:00:00Z",
                }
            ],
        )
        self.assertEqual(parser.time_datetimes, ["2026-01-02"])


if __name__ == "__main__":
    unittest.main()
