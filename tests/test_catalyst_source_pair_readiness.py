import unittest

from catalyst_source_pair_readiness import build_pair_readiness


class CatalystSourcePairReadinessTests(unittest.TestCase):
    def test_join_counts_candidates_without_accepting_evidence(self):
        discovery = {
            "pair_records": [
                {"date": "2026-01-02", "symbol": "AAA", "article_keys": ["a"]},
                {"date": "2026-01-02", "symbol": "BBB", "article_keys": ["b"]},
            ]
        }
        selection = {
            "records": [
                {
                    "url_sha256": "u1",
                    "category": "ISSUER_HOST_CANDIDATE",
                    "article_keys": ["a"],
                }
            ]
        }
        capture_index = {
            "records": {
                "u1": {
                    "status": "RESPONSE_CAPTURED",
                    "http_status": 200,
                    "headers": {"content-type": "text/html; charset=utf-8"},
                }
            }
        }
        source_profile = {
            "records": {
                "u1": {
                    "html": {
                        "canonical_urls": ["https://issuer.example/release"],
                        "meta_timestamp_candidates": [
                            {"key": "date", "value": "2026-01-02"}
                        ],
                        "time_datetime_candidates": [],
                        "json_ld_date_published_present": False,
                    }
                }
            }
        }

        result = build_pair_readiness(
            discovery, selection, capture_index, source_profile
        )

        self.assertEqual(result["counts"]["selected_pairs"], 2)
        self.assertEqual(result["counts"]["pairs_has_primary_route"], 1)
        self.assertEqual(result["counts"]["pairs_without_primary_route"], 1)
        self.assertEqual(result["counts"]["pairs_has_http_200"], 1)
        self.assertEqual(result["counts"]["pairs_has_standard_timestamp_candidate"], 1)
        self.assertEqual(result["timestamp_candidates_accepted"], 0)
        self.assertFalse(result["primary_catalyst_verified"])


if __name__ == "__main__":
    unittest.main()
