from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import asr_submission_collection as collection


VALID_SUBMISSION = (
    b"<SEC-DOCUMENT>0000000000-25-000001.txt : 20250101\\n"
    b"<ACCEPTANCE-DATETIME>20250101120000\\n"
)


class FakeResponse:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content


class FallbackSession:
    def __init__(self):
        self.headers = {}
        self.calls = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        if url.endswith("/first.txt"):
            return FakeResponse(404)
        return FakeResponse(200, VALID_SUBMISSION)


def _contract():
    return {
        "contract_sha256": "c" * 64,
        "request_contract": {
            "minimum_spacing_seconds": 0.0,
            "timeout_seconds": 30.0,
            "maximum_source_bytes": 50_000_000,
            "maximum_attempts_per_candidate": 3,
            "retryable_statuses": [429, 500, 502, 503, 504],
        },
    }


def test_collection_tries_next_frozen_cik_candidate(tmp_path: Path, monkeypatch):
    contract = _contract()
    graph = {
        "private_graph_sha256": "g" * 64,
        "requests": [
            {
                "ordinal": 0,
                "request_sha256": "r" * 64,
                "accession": "0000000000-25-000001",
                "file_date": "2025-01-01",
                "candidates": [
                    {
                        "candidate_ordinal": 0,
                        "url": "https://www.sec.gov/Archives/first.txt",
                    },
                    {
                        "candidate_ordinal": 1,
                        "url": "https://www.sec.gov/Archives/second.txt",
                    },
                ],
            }
        ],
    }
    monkeypatch.setattr(
        collection,
        "_load_authority",
        lambda: (
            Path("contract.json"),
            contract,
            {"inspection_sha256": "i" * 64},
            graph,
            tmp_path,
        ),
    )
    monkeypatch.setattr(
        collection.SecConfig,
        "from_env",
        lambda *args, **kwargs: SimpleNamespace(user_agent="tests test@example.com"),
    )
    result = collection.collect(
        status_path=tmp_path / "status.json",
        require_published=False,
        session=FallbackSession(),
        store_root=tmp_path,
    )
    assert result["request_count"] == 1
    assert result["success_count"] == 1
    assert result["failure_count"] == 0
    assert result["attempted_candidate_count"] == 2
    assert result["state"] == "SUBMISSIONS_COLLECTED_READY_FOR_INSPECTION"
    assert result["filing_semantic_classification_permitted"] is False
    assert result["verified_event_count"] is None
    assert result["market_outcomes_accessed"] is False


def test_acceptance_pattern_is_required():
    assert collection.ACCEPTANCE_PATTERN.search(VALID_SUBMISSION)
    assert collection.ACCEPTANCE_PATTERN.search(b"<SEC-DOCUMENT>") is None
