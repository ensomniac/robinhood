from __future__ import annotations

from pathlib import Path

import oversold_replication_development_collection as collection


def test_scope_excludes_zero_days_but_collection_retains_them() -> None:
    inventory = {
        "signal_dates": ["2023-01-26"],
        "zero_signal_dates": ["2023-01-27"],
        "candidates_by_date": {
            "2023-01-26": [
                {"symbol": "EDGE"},
            ],
            "2023-01-27": [],
        },
    }
    assert collection._scope(inventory) == {
        "dates": ["2023-01-26"],
        "symbols_by_date": {
            "2023-01-26": ["EDGE"],
        },
    }
    assert inventory["zero_signal_dates"] == ["2023-01-27"]


def test_hashed_artifact_rejects_drift(tmp_path: Path) -> None:
    content = {
        "artifact_kind": (
            "oversold_replication_development_collection_contract"
        ),
        "state": "DEVELOPMENT_COLLECTION_FROZEN_AWAITING_INSPECTION",
    }
    identity = collection.development._hash(content)
    path = tmp_path / f"contract-{identity}.json"
    collection._write(
        path,
        {**content, "contract_sha256": identity},
    )
    assert collection._load_contract(path)["contract_sha256"] == identity

    collection._write(
        path,
        {
            **content,
            "state": "DRIFTED",
            "contract_sha256": identity,
        },
    )
    try:
        collection._load_contract(path)
    except collection.OversoldReplicationCollectionError as exc:
        assert "invalid" in str(exc)
    else:
        raise AssertionError("artifact drift must fail closed")


def test_authorization_is_required_before_collection(monkeypatch) -> None:
    called = False

    def load_chain(_contract, _inspection):
        return (
            {
                "contract_sha256": "contract",
                "scope": {
                    "dates": ["2023-01-26"],
                    "symbols_by_date": {
                        "2023-01-26": ["EDGE"],
                    },
                },
            },
            {"inspection_sha256": "inspection"},
        )

    def reject_authorization(_path, _contract):
        raise collection.OversoldReplicationCollectionError(
            "authorization missing"
        )

    class Store:
        pass

    monkeypatch.setattr(collection, "_load_contract_chain", load_chain)
    monkeypatch.setattr(collection, "_load_authorization", reject_authorization)
    monkeypatch.setattr(
        collection.HistoricalDayStore,
        "from_env",
        lambda _path: Store(),
    )
    monkeypatch.setattr(
        collection,
        "_full_dataset",
        lambda *_args: called,
    )
    try:
        collection.collect(
            Path("contract"),
            Path("inspection"),
            Path("authorization"),
        )
    except collection.OversoldReplicationCollectionError as exc:
        assert "authorization missing" in str(exc)
    else:
        raise AssertionError("collection must require authorization")
    assert called is False
