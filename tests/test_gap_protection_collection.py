from __future__ import annotations

from pathlib import Path

import gap_protection_collection as collection


def test_development_scope_excludes_embargo_and_confirmation():
    inventory = {
        "partitions": {
            "development": ["2023-01-03", "2023-01-04"],
            "embargo": ["2023-01-05"],
            "confirmation": ["2023-01-06"],
        },
        "candidates_by_date": {
            "2023-01-03": [{"symbol": "BBB"}, {"symbol": "AAA"}],
            "2023-01-04": [{"symbol": "CCC"}],
            "2023-01-05": [{"symbol": "DDD"}],
            "2023-01-06": [{"symbol": "EEE"}],
        },
    }

    assert collection._development_scope(inventory) == {
        "dates": ["2023-01-03", "2023-01-04"],
        "symbols_by_date": {
            "2023-01-03": ["AAA", "BBB"],
            "2023-01-04": ["CCC"],
        },
    }


def test_hash_addressed_contract_loader_rejects_mutation(tmp_path: Path):
    content = {
        "schema_version": 1,
        "artifact_kind": "test",
        "state": "DEVELOPMENT_COLLECTION_FROZEN",
    }
    identity = collection._hash(content)
    path = tmp_path / f"contract-{identity}.json"
    collection._write_json(
        path,
        {**content, "contract_sha256": identity},
    )
    assert collection._load_contract(path)["contract_sha256"] == identity

    value = collection._load_json(path)
    value["state"] = "MUTATED"
    collection._write_json(path, value)
    try:
        collection._load_contract(path)
    except collection.GapProtectionCollectionError as exc:
        assert "hash is invalid" in str(exc)
    else:
        raise AssertionError("mutated contract unexpectedly loaded")
