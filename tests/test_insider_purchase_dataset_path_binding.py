from pathlib import Path

import insider_purchase_dataset_path_binding as binding
import strategy_discovery


SEARCH = Path(
    "strategy_tournament/v2/discovery/"
    "clustered-form4-open-market-purchase-continuation-replication-v2/"
    "search/"
    "clustered-form4-open-market-purchase-continuation-replication-v2-"
    "search-625dcc3ed8e148664c4800c3d72075caaab5fb921675960947e92136bb73e235.json"
)


def test_path_binding_changes_only_the_spelling_of_the_same_manifest():
    search = strategy_discovery.load_artifact(
        SEARCH, expected_kind="frozen-development-search"
    )
    contract = binding.build_contract(SEARCH)
    source_path = (
        binding.PROJECT_ROOT
        / search["family_contract"]["dataset_manifest"]
    ).resolve()

    assert Path(contract["dataset_manifest"]).resolve() == source_path
    assert Path(contract["dataset_manifest"]).is_absolute()
    assert contract["trial_family"] == search["family_contract"][
        "trial_family"
    ]
    assert contract["implementation_hashes"] == search["family_contract"][
        "implementation_hashes"
    ]
