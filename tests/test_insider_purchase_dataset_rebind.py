from pathlib import Path

import insider_purchase_dataset_rebind as rebind


ROOT = Path("strategy_tournament/v2/discovery") / (
    "clustered-form4-open-market-purchase-continuation-replication-v2"
)
REPLACEMENT_SEARCH = ROOT / "search" / (
    "clustered-form4-open-market-purchase-continuation-replication-v2-"
    "search-c137ebdb8c1c4493ba533668084ff6d75f93dca942e240704d2e873ed7f2d28d.json"
)
SOURCE_MANIFEST = ROOT / "development-dataset" / (
    "dataset-clustered-form4-open-market-purchase-continuation-"
    "replication-v2-development-bce9dbf6a640f44a-"
    "55c94ade860250176a66ff9deceb9475fa4ac344f3a220f662b0a6c60f0e037a.json"
)


def test_rebind_changes_only_dataset_metadata_and_search_binding():
    value = rebind.build_manifest_value(
        REPLACEMENT_SEARCH,
        SOURCE_MANIFEST,
        registered_at="2026-07-26T11:42:00Z",
    )
    binding = value["dataset_payload"]["dataset_rebind"]

    assert value["dataset_payload"]["inspected"] is True
    assert binding["semantic_contract_identical"] is True
    assert binding["private_rows_opened"] is False
    assert binding["provider_requests"] == 0
    assert binding["confirmation_accessed"] is False
    assert (
        value["dataset_payload"]["development_search_sha256"]
        == "c137ebdb8c1c4493ba533668084ff6d75f93dca942e240704d2e873ed7f2d28d"
    )
