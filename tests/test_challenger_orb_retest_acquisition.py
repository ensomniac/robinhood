import json
import gzip
from pathlib import Path

import pytest

import challenger_orb_retest_acquisition as acquisition


def test_source_rules_lock_primary_semantics():
    contract = acquisition._source_rules_contract()
    rules = contract["rules"]

    assert rules["primary_evidence_only"] is True
    assert rules["same_day_date_only_fails"] is True
    assert rules["financing_or_dilution_conflicts_classified_before_positive"] is True
    assert rules["selection_or_source_substitution_allowed"] is False
    assert contract["rules_sha256"] == acquisition._sha256_json(rules)


def test_scanner_manifest_path_requires_exactly_one(tmp_path):
    with pytest.raises(acquisition.ChallengerAcquisitionError, match="exactly one"):
        acquisition._scanner_manifest_path(tmp_path)

    manifest = tmp_path / f"{acquisition.SCANNER_DATASET_ID}-{'a' * 64}.json"
    manifest.write_text("{}\n", encoding="utf-8")
    assert acquisition._scanner_manifest_path(tmp_path) == manifest

    (tmp_path / f"{acquisition.SCANNER_DATASET_ID}-{'b' * 64}.json").write_text(
        "{}\n", encoding="utf-8"
    )
    with pytest.raises(acquisition.ChallengerAcquisitionError, match="exactly one"):
        acquisition._scanner_manifest_path(tmp_path)


def test_reference_status_is_resumable_and_hashes_only_ready_dates(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        acquisition,
        "_selection",
        lambda: {"selected_dates": ["2024-01-02", "2024-01-03"]},
    )
    monkeypatch.setattr(acquisition, "REFERENCE_ROOT", tmp_path)
    with gzip.open(tmp_path / "2024-01-02.json.gz", "wt", encoding="utf-8") as target:
        json.dump([{"ticker": "ABC"}], target)

    partial = acquisition.reference_status()
    assert partial["ready"] == 1
    assert partial["missing"] == 1
    assert partial["complete"] is False

    with gzip.open(tmp_path / "2024-01-03.json.gz", "wt", encoding="utf-8") as target:
        json.dump([{"ticker": "XYZ"}], target)
    complete = acquisition.reference_status()
    assert complete["ready"] == 2
    assert complete["missing"] == 0
    assert complete["complete"] is True
    assert complete["target_market_data_accessed"] is False


def test_split_attestation_rebuilds_exact_frozen_range(tmp_path, monkeypatch):
    split_path = tmp_path / "splits.json.gz"
    rows = [
        {
            "execution_date": "2023-06-01",
            "ticker": "ABC",
            "split_from": 1,
            "split_to": 2,
        }
    ]
    with gzip.open(split_path, "wt", encoding="utf-8") as target:
        json.dump(rows, target)
    monkeypatch.setattr(acquisition, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(acquisition, "SPLITS", split_path)

    attestation = acquisition._split_attestation()
    assert attestation["artifact"]["events"] == 1
    assert attestation["artifact"]["sha256"] == acquisition._sha256_file(split_path)
    assert attestation["source"]["query_range"] == {
        "execution_date_gte": "2023-01-04",
        "execution_date_lte": "2024-12-24",
    }


def test_binding_rejects_drift_and_unsafe_path(tmp_path, monkeypatch):
    artifact = tmp_path / "artifact.json"
    artifact.write_text(json.dumps({"ready": True}), encoding="utf-8")
    monkeypatch.setattr(acquisition, "PROJECT_ROOT", tmp_path)
    binding = acquisition._binding(artifact)
    acquisition._verify_binding(binding)

    artifact.write_text(json.dumps({"ready": False}), encoding="utf-8")
    with pytest.raises(acquisition.ChallengerAcquisitionError, match="drifted"):
        acquisition._verify_binding(binding)
    with pytest.raises(acquisition.ChallengerAcquisitionError, match="unsafe"):
        acquisition._verify_binding({"path": "../escape", "sha256": "0" * 64})


def test_zero_state_rejects_existing_market_artifacts(monkeypatch):
    selection = {
        "selected_dates": ["2024-01-02"],
        "selected_dates_sha256": "1" * 64,
        "required_sessions_sha256": "2" * 64,
    }
    manifest = {
        "manifest_sha256": "3" * 64,
        "collection_contract": {
            "source": "Alpaca historical SIP",
            "endpoint": "https://data.alpaca.markets/v2/stocks/bars",
            "feed": "sip",
            "adjustment": "raw",
            "regular_session_query": "regular",
            "opening_query": "opening",
            "required_session_count": 16,
        },
    }
    status = {
        "session_files": {"ready": 1},
        "provider_requests": 1,
        "provider_retries": 0,
        "derived_rows": 1,
        "canonical_day_merges": 1,
    }
    monkeypatch.setattr(acquisition, "_selection", lambda: selection)
    monkeypatch.setattr(
        acquisition,
        "_validate_scanner_manifest",
        lambda _path, store: (manifest, status),
    )
    monkeypatch.setattr(
        acquisition,
        "_read_object",
        lambda _path: {"contract_sha256": acquisition.retest.HYPOTHESIS_SHA256},
    )
    monkeypatch.setattr(acquisition, "_provider_config_contract", lambda _path: {})

    with pytest.raises(
        acquisition.ChallengerAcquisitionError, match="target scanner artifacts"
    ):
        acquisition._expected_contract(
            scanner_manifest_path=Path("scanner.json"),
            store=object(),
            env_path=Path(".env"),
            require_zero_market_state=True,
        )


def test_collect_requires_positive_max_days(monkeypatch):
    monkeypatch.setattr(acquisition, "_published", lambda _path: {})
    monkeypatch.setattr(
        acquisition,
        "load_frozen_dataset_contract",
        lambda _path: {
            "dataset_id": acquisition.DATASET_ID,
            "upstream_contract": {
                "scanner_manifest": {"path": "scanner.json", "sha256": "0" * 64}
            },
        },
    )
    monkeypatch.setattr(acquisition, "_verify_binding", lambda _value: None)
    monkeypatch.setattr(
        acquisition.HistoricalStoreConfig,
        "from_env",
        lambda _path: type("Config", (), {"root": Path("/tmp")})(),
    )
    monkeypatch.setattr(acquisition, "_expected_contract", lambda **_kwargs: {})

    with pytest.raises(acquisition.ChallengerAcquisitionError, match="positive"):
        acquisition.collect_scanner(
            manifest_path=Path("outer.json"),
            env_path=Path(".env"),
            max_days=0,
        )
