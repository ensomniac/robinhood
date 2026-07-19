import unittest

from primary_catalyst_evidence import (
    classify_primary_catalyst,
    evidence_documents,
    parse_submission_documents,
    visible_text,
)


SUBMISSION = """
<SEC-DOCUMENT>
<DOCUMENT>
<TYPE>8-K
<SEQUENCE>1
<FILENAME>main.htm
<DESCRIPTION>CURRENT REPORT
<TEXT><html><style>ignore me</style><body>Item 2.02 Results.</body></html></TEXT>
</DOCUMENT>
<DOCUMENT>
<TYPE>EX-99.1
<SEQUENCE>2
<FILENAME>release.htm
<DESCRIPTION>PRESS RELEASE
<TEXT><html><body>Acme Raises Full-Year Guidance after record revenue.</body></html></TEXT>
</DOCUMENT>
</SEC-DOCUMENT>
"""


class PrimaryCatalystEvidenceTests(unittest.TestCase):
    def test_submission_parser_and_evidence_selection(self):
        documents = parse_submission_documents(SUBMISSION)
        self.assertEqual(len(documents), 2)
        selected = evidence_documents("main.htm", documents)
        self.assertEqual(
            [row.filename for row in selected], ["main.htm", "release.htm"]
        )

    def test_visible_text_excludes_styles(self):
        self.assertEqual(
            visible_text("<style>bad</style><p>Good  news</p>"), "Good news"
        )

    def test_recent_direct_filing_with_positive_primary_phrase_is_verified(self):
        result = classify_primary_catalyst(
            {"primary_document": "main.htm", "items": ["2.02"]},
            parse_submission_documents(SUBMISSION),
            accepted_after_prior_close=True,
        )
        self.assertEqual(result["disposition"], "VERIFIED_POSITIVE_PRIMARY")
        self.assertTrue(result["verified_material_catalyst"])
        self.assertTrue(result["verified_positive_direction"])

    def test_direct_material_event_can_be_verified_with_unresolved_direction(self):
        documents = parse_submission_documents(
            SUBMISSION.replace(
                "Acme Raises Full-Year Guidance after record revenue.",
                "Acme reports quarterly operating results.",
            )
        )
        result = classify_primary_catalyst(
            {"primary_document": "main.htm", "items": ["2.02"]},
            documents,
            accepted_after_prior_close=True,
        )
        self.assertEqual(
            result["disposition"], "VERIFIED_MATERIAL_DIRECTION_UNRESOLVED"
        )
        self.assertTrue(result["verified_material_catalyst"])
        self.assertEqual(result["direction"], "UNRESOLVED")

    def test_financing_and_stale_events_fail_closed(self):
        financing = parse_submission_documents(
            SUBMISSION.replace(
                "Acme Raises Full-Year Guidance after record revenue.",
                "Acme announces a registered direct offering.",
            )
        )
        conflict = classify_primary_catalyst(
            {"primary_document": "main.htm", "items": ["2.02"]},
            financing,
            accepted_after_prior_close=True,
        )
        self.assertEqual(conflict["disposition"], "NEGATIVE_OR_FINANCING_CONFLICT")
        self.assertFalse(conflict["verified_material_catalyst"])

        stale = classify_primary_catalyst(
            {"primary_document": "main.htm", "items": ["2.02"]},
            parse_submission_documents(SUBMISSION),
            accepted_after_prior_close=False,
        )
        self.assertEqual(stale["disposition"], "STALE_BEFORE_PRIOR_CLOSE")
        self.assertFalse(stale["verified_material_catalyst"])


if __name__ == "__main__":
    unittest.main()
