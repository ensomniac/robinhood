import unittest

from catalyst_news_enrichment import (
    CatalystNewsEnrichmentError,
    _article_key,
    summarize_content,
)


class CatalystNewsEnrichmentTests(unittest.TestCase):
    def test_article_key_requires_provider_identity_and_timestamp(self):
        self.assertEqual(
            _article_key({"article_id": 7, "created_at": "2026-01-02T12:00:00Z"}),
            "7|2026-01-02T12:00:00Z",
        )
        with self.assertRaises(CatalystNewsEnrichmentError):
            _article_key({"article_id": 7})

    def test_summary_deduplicates_articles_and_preserves_pair_attribution(self):
        discovery = {
            "pair_count": 2,
            "date_count": 1,
            "unique_article_count": 2,
            "pair_records": [
                {"article_keys": ["a"]},
                {"article_keys": ["a", "b"]},
            ],
        }
        content = {
            "date_records": {
                "2026-01-02": {
                    "articles": {
                        "a": {
                            "returned": True,
                            "content_available": True,
                            "content_sha256": "one",
                            "content_bytes": 10,
                        },
                        "b": {
                            "returned": True,
                            "content_available": False,
                            "content_sha256": None,
                            "content_bytes": 0,
                        },
                    }
                }
            }
        }
        result = summarize_content(discovery, content)
        self.assertEqual(result["returned_unique_articles"], 2)
        self.assertEqual(result["content_available_unique_articles"], 1)
        self.assertEqual(result["pairs_with_content"], 2)
        self.assertEqual(result["content_bytes"], 10)

    def test_summary_rejects_content_drift_across_dates(self):
        discovery = {
            "pair_count": 1,
            "date_count": 2,
            "unique_article_count": 1,
            "pair_records": [{"article_keys": ["a"]}],
        }
        content = {
            "date_records": {
                "2026-01-02": {
                    "articles": {
                        "a": {
                            "content_available": True,
                            "content_sha256": "one",
                        }
                    }
                },
                "2026-01-03": {
                    "articles": {
                        "a": {
                            "content_available": True,
                            "content_sha256": "two",
                        }
                    }
                },
            }
        }
        with self.assertRaisesRegex(CatalystNewsEnrichmentError, "changed"):
            summarize_content(discovery, content)


if __name__ == "__main__":
    unittest.main()
