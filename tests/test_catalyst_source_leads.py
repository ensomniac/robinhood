import unittest

from catalyst_source_leads import classify_url, normalize_url


class CatalystSourceLeadTests(unittest.TestCase):
    def test_normalization_keeps_query_but_removes_fragment(self):
        self.assertEqual(
            normalize_url("HTTPS://WWW.SEC.GOV/a/b?x=1#page"),
            "https://www.sec.gov/a/b?x=1",
        )
        self.assertIsNone(normalize_url("mailto:test@example.com"))
        self.assertIsNone(normalize_url("/relative"))

    def test_categories_are_routing_leads_not_verification(self):
        self.assertEqual(
            classify_url("https://www.sec.gov/Archives/example"),
            "AUTHORITY_CANDIDATE",
        )
        self.assertEqual(
            classify_url("https://ir.example.com/news"),
            "ISSUER_HOST_CANDIDATE",
        )
        self.assertEqual(
            classify_url("https://www.prnewswire.com/release"),
            "WIRE_CANDIDATE",
        )
        self.assertEqual(
            classify_url("https://www.benzinga.com/news"),
            "REJECTED_PLATFORM_OR_SOURCE",
        )


if __name__ == "__main__":
    unittest.main()
