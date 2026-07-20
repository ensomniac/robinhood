import json

import development_non_return_collection as base
import development_non_return_collection_v3 as adapter


def test_adapter_identity_and_paths_are_isolated_from_completed_collector() -> None:
    assert adapter.DATASET_ID != base.DATASET_ID
    assert adapter.BASE_MANIFEST != base.BASE_MANIFEST
    assert adapter.DEFAULT_MANIFEST_ROOT != base.DEFAULT_MANIFEST_ROOT
    assert adapter.DEFAULT_CONTRACT_STATUS != base.DEFAULT_CONTRACT_STATUS
    assert adapter.DEFAULT_COLLECTION_STATUS != base.DEFAULT_COLLECTION_STATUS
    assert "development_tranche_v3" in str(adapter.BASE_MANIFEST)
    assert adapter.EXPECTED_PAIRS == 102


def test_scoped_configuration_restores_completed_collector() -> None:
    original = {
        "source_contract": base.source_contract,
        "dataset_id": base.DATASET_ID,
        "base_manifest": base.BASE_MANIFEST,
        "expected_pairs": base.EXPECTED_PAIRS,
        "load_base": base._load_base,
        "implementation": base._implementation_contract,
    }
    with adapter._configured():
        assert base.source_contract is adapter.SOURCE_CONTRACT
        assert base.DATASET_ID == adapter.DATASET_ID
        assert base.BASE_MANIFEST == adapter.BASE_MANIFEST
        assert base.EXPECTED_PAIRS == 102
        assert base._load_base is adapter._load_base_v3
        assert base._implementation_contract is adapter._implementation_contract_v3
    assert base.source_contract is original["source_contract"]
    assert base.DATASET_ID == original["dataset_id"]
    assert base.BASE_MANIFEST == original["base_manifest"]
    assert base.EXPECTED_PAIRS == original["expected_pairs"]
    assert base._load_base is original["load_base"]
    assert base._implementation_contract is original["implementation"]


def test_v3_contract_status_is_aggregate_outcome_locked_and_counted() -> None:
    with adapter._configured():
        status = base._contract_status(
            {"manifest_sha256": "a" * 64},
            status="FROZEN_READY",
            inspected=True,
        )
    rendered = json.dumps(status, sort_keys=True)
    assert status["positive_pairs"] == 102
    assert status["maximum_one_second_trade_windows"] == 102 * 55 * 60
    assert status["target_artifacts"] == 0
    assert status["provider_access_performed"] is False
    assert status["target_outcomes_observed_or_derived"] is False
    assert "SECRET_TICKER" not in rendered


def test_v3_implementation_contract_binds_adapter_base_and_source() -> None:
    files = adapter._implementation_contract_v3()
    assert {
        "development_non_return_collection_v3.py",
        "development_non_return_collection.py",
        "development_non_return_v3.py",
        "development_non_return.py",
    } <= set(files)
    assert all(value["sha256"] for value in files.values())


def test_v3_private_collection_namespace_cannot_alias_completed_collection() -> None:
    root = adapter.Path("/historical")
    with adapter._configured():
        new = base._private_root(root)
    old = root / base.PRIVATE_NAMESPACE / base.DATASET_ID
    assert new != old
    assert new.name == adapter.DATASET_ID
