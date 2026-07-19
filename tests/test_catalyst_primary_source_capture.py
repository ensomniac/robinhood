import unittest

from catalyst_primary_source_capture import _is_public_address, build_capture_selection


class PrimarySourceCaptureTests(unittest.TestCase):
    def test_private_and_special_addresses_are_blocked(self):
        self.assertFalse(_is_public_address("127.0.0.1"))
        self.assertFalse(_is_public_address("10.0.0.1"))
        self.assertFalse(_is_public_address("169.254.1.1"))
        self.assertFalse(_is_public_address("::1"))
        self.assertTrue(_is_public_address("8.8.8.8"))

    def test_selection_deduplicates_url_and_preserves_articles(self):
        source = {
            "article_records": {
                "a": {
                    "leads": [
                        {
                            "url": f"https://ir.example{index}.com/release",
                            "category": "ISSUER_HOST_CANDIDATE",
                        }
                        for index in range(134)
                    ]
                },
                "b": {
                    "leads": [
                        {
                            "url": "https://ir.example0.com/release",
                            "category": "ISSUER_HOST_CANDIDATE",
                        }
                    ]
                },
            }
        }
        result = build_capture_selection(source)
        self.assertEqual(result["selected_url_count"], 134)
        first = next(
            row
            for row in result["records"]
            if row["url"] == "https://ir.example0.com/release"
        )
        self.assertEqual(first["article_keys"], ["a", "b"])


if __name__ == "__main__":
    unittest.main()
