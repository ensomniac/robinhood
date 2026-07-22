from pathlib import Path

import challenger_orb_retest_acquisition as prior
import challenger_orb_retest_acquisition2 as acquisition


def test_configuration_is_scoped_and_restored():
    original_dataset = prior.DATASET_ID
    original_tranche = prior.tranche
    original_file = prior.__file__

    with acquisition.configured():
        assert prior.DATASET_ID == acquisition.DATASET_ID
        assert prior.SCANNER_DATASET_ID == acquisition.SCANNER_DATASET_ID
        assert prior.tranche is acquisition.tranche
        assert Path(prior.__file__).resolve() == Path(acquisition.__file__).resolve()

    assert prior.DATASET_ID == original_dataset
    assert prior.tranche is original_tranche
    assert prior.__file__ == original_file


def test_split_query_covers_complete_required_session_graph(monkeypatch):
    monkeypatch.setattr(
        prior,
        "_selection",
        lambda: {"selected_dates": ["2023-01-25", "2026-07-17"]},
    )
    monkeypatch.setattr(
        acquisition.tranche,
        "_calendar_dates",
        lambda: ["2023-01-04", "2023-01-25", "2026-07-17"],
    )
    monkeypatch.setattr(
        acquisition.scanner_replay,
        "required_sessions",
        lambda requested, calendar, prior_sessions: ["2023-01-04", "2026-07-17"],
    )

    with acquisition.configured():
        assert acquisition._split_query_bounds() == ("2023-01-04", "2026-07-17")


def test_expected_contract_binds_wrapper_and_base_controller(monkeypatch):
    monkeypatch.setattr(
        acquisition,
        "_BASE_EXPECTED_CONTRACT",
        lambda **kwargs: {"implementation_contract": {"controller": {}}},
    )
    monkeypatch.setattr(
        prior,
        "_binding",
        lambda path: {"path": str(path), "sha256": "bound"},
    )

    result = acquisition._expected_contract()

    assert result["implementation_contract"]["base_controller"] == {
        "path": str(acquisition.BASE_CONTROLLER),
        "sha256": "bound",
    }


def test_scanner_manifest_lookup_passes_second_tranche_root(monkeypatch):
    observed = []
    expected = acquisition.DEFAULT_SCANNER_MANIFEST_ROOT / "manifest.json"
    monkeypatch.setattr(
        prior,
        "_scanner_manifest_path",
        lambda root: observed.append(root) or expected,
    )

    assert acquisition._scanner_manifest_path() == expected
    assert observed == [acquisition.DEFAULT_SCANNER_MANIFEST_ROOT]
