import unittest

from catalyst_pdf_source_profile import profile_text


class CatalystPdfSourceProfileTests(unittest.TestCase):
    def test_text_profile_retains_dates_as_unaccepted_candidates(self):
        result = profile_text(
            "Press Release - March 25, 2026 - Financial Results - 2026-03-25"
        )

        self.assertEqual(result["date_candidates"]["iso"], ["2026-03-25"])
        self.assertEqual(result["date_candidates"]["month_name"], ["March 25, 2026"])
        self.assertTrue(result["document_markers"]["press_release"])
        self.assertTrue(result["document_markers"]["earnings_or_results"])

    def test_marker_profile_does_not_infer_a_catalyst(self):
        result = profile_text("United States Court memorandum opinion May 22, 2024")

        self.assertTrue(result["document_markers"]["court_document"])
        self.assertTrue(result["document_markers"]["government_document"])
        self.assertNotIn("catalyst_verified", result)


if __name__ == "__main__":
    unittest.main()
