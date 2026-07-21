import json
import gzip
from pathlib import Path

import pytest

import challenger_orb_retest_acquisition as acquisition
import challenger_orb_retest_acquisition_inspection as acquisition_inspection
from learning_data import security_master_sha256


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
    assert len(complete["logical_snapshot_set_sha256"]) == 64
    assert complete["target_market_data_accessed"] is False


def test_reference_collector_lock_rejects_a_competing_process(tmp_path):
    lock = tmp_path / "reference.lock"
    with acquisition._exclusive_run_lock(lock):
        with pytest.raises(
            acquisition.ChallengerAcquisitionError, match="already running"
        ):
            with acquisition._exclusive_run_lock(lock):
                pass

    with acquisition._exclusive_run_lock(lock):
        assert json.loads(lock.read_text(encoding="utf-8"))["pid"] > 0
    assert lock.read_text(encoding="utf-8") == ""


def test_logical_reference_hash_ignores_gzip_container_metadata(tmp_path, monkeypatch):
    day = "2024-01-02"
    path = tmp_path / f"{day}.json.gz"
    rows = [{"ticker": "ABC", "name": "Example"}]
    monkeypatch.setattr(acquisition, "_selection", lambda: {"selected_dates": [day]})
    monkeypatch.setattr(acquisition, "REFERENCE_ROOT", tmp_path)

    def write_snapshot(mtime):
        with path.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=mtime) as target:
                target.write(json.dumps(rows).encode("utf-8"))

    write_snapshot(1)
    first = acquisition.reference_status()
    write_snapshot(2)
    second = acquisition.reference_status()

    assert first["snapshot_set_sha256"] != second["snapshot_set_sha256"]
    assert (
        first["logical_snapshot_set_sha256"]
        == second["logical_snapshot_set_sha256"]
    )


def test_reference_inspector_rebuilds_partial_cache(tmp_path, monkeypatch):
    days = ["2024-01-02", "2024-01-03"]
    with gzip.open(tmp_path / f"{days[0]}.json.gz", "wt", encoding="utf-8") as target:
        json.dump([{"ticker": "ABC"}], target)
    monkeypatch.setattr(acquisition, "_selection", lambda: {"selected_dates": days})
    monkeypatch.setattr(acquisition, "REFERENCE_ROOT", tmp_path)

    inspected = acquisition_inspection.inspect_reference()

    assert inspected["status"] == "REFERENCE_PARTIAL"
    assert inspected["ready"] == 1
    assert inspected["missing"] == 1
    assert inspected["unexpected_snapshots"] == 0
    assert inspected["temporary_artifacts"] == 0

    with gzip.open(tmp_path / "2024-01-04.json.gz", "wt", encoding="utf-8") as target:
        json.dump([{"ticker": "XYZ"}], target)
    with pytest.raises(
        acquisition_inspection.ChallengerAcquisitionInspectionError,
        match="unexpected",
    ):
        acquisition_inspection.inspect_reference()


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


def test_independent_input_inspection_rehashes_private_artifacts(tmp_path, monkeypatch):
    day = "2024-01-02"
    reference_root = tmp_path / "reference"
    reference_root.mkdir()
    snapshot = reference_root / f"{day}.json.gz"
    with gzip.open(snapshot, "wt", encoding="utf-8") as target:
        json.dump([{"ticker": "ABC"}], target)
    master = tmp_path / "security-master.jsonl"
    master.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "record_id": "record-one",
                "instrument_id": "instrument-one",
                "symbol": "ABC",
                "primary_exchange": "NASDAQ",
                "security_type": "COMMON",
                "valid_from": day,
                "valid_to": day,
                "observed_dates": [day],
                "status": "ACTIVE",
                "recorded_at": "2026-07-20T00:00:00+00:00",
                "provenance_paths": ["historical_batches/test.json"],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    master_source = tmp_path / "security-master-source.json"
    master_source.write_text(
        json.dumps(
            {
                "requested_dates": [day],
                "snapshots": [
                    {
                        "date": day,
                        "rows": 1,
                        "sha256": acquisition._sha256_file(snapshot),
                    }
                ],
                "security_master": {
                    "sha256": security_master_sha256(master),
                    "records": 1,
                    "instruments": 1,
                },
            }
        ),
        encoding="utf-8",
    )
    splits = tmp_path / "splits.json.gz"
    with gzip.open(splits, "wt", encoding="utf-8") as target:
        json.dump(
            [
                {
                    "execution_date": "2023-06-01",
                    "ticker": "ABC",
                    "split_from": 1,
                    "split_to": 2,
                }
            ],
            target,
        )
    split_source = tmp_path / "split-source.json"
    split_source.write_text(
        json.dumps(
            {
                "source": {
                    "provider": "Massive",
                    "endpoint": "https://api.massive.com/stocks/v1/splits",
                    "query_range": {
                        "execution_date_gte": "2023-01-04",
                        "execution_date_lte": "2024-12-24",
                    },
                },
                "artifact": {
                    "events": 1,
                    "sha256": acquisition._sha256_file(splits),
                },
            }
        ),
        encoding="utf-8",
    )
    store_root = tmp_path / "store"
    monkeypatch.setattr(acquisition, "_selection", lambda: {"selected_dates": [day]})
    monkeypatch.setattr(acquisition, "REFERENCE_ROOT", reference_root)
    monkeypatch.setattr(acquisition, "SECURITY_MASTER", master)
    monkeypatch.setattr(acquisition, "SECURITY_SOURCE", master_source)
    monkeypatch.setattr(acquisition, "SPLITS", splits)
    monkeypatch.setattr(acquisition, "SPLIT_SOURCE", split_source)
    monkeypatch.setattr(
        acquisition_inspection.HistoricalStoreConfig,
        "from_env",
        lambda _path: type("Config", (), {"root": store_root, "min_free_bytes": 0})(),
    )

    result = acquisition_inspection.inspect_inputs(env_path=tmp_path / ".env")
    assert result["snapshot_count"] == 1
    assert result["security_master_records"] == 1
    assert result["split_action_events"] == 1
    assert result["pre_freeze_target_market_artifacts"] == 0


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


def test_scanner_collection_uses_shared_acquisition_lock(tmp_path, monkeypatch):
    lock_state = {"held": False, "operation": None}
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
        lambda _path: type("Config", (), {"root": tmp_path})(),
    )
    monkeypatch.setattr(acquisition, "_expected_contract", lambda **_kwargs: {})
    monkeypatch.setattr(acquisition.alpaca, "load_contract", lambda _path: {})
    monkeypatch.setattr(
        acquisition.alpaca.AlpacaBulkConfig,
        "from_env",
        lambda _path: object(),
    )

    @acquisition.contextmanager
    def fake_lock(path, *, operation):
        assert path == acquisition.ACQUISITION_LOCK
        lock_state.update(held=True, operation=operation)
        try:
            yield
        finally:
            lock_state["held"] = False

    def fake_collect(_manifest, *, config, store, max_days):
        assert lock_state == {
            "held": True,
            "operation": "full-universe scanner collection",
        }
        assert config is not None
        assert store is not None
        assert max_days == 1
        return {"valid": True}

    monkeypatch.setattr(acquisition, "_exclusive_run_lock", fake_lock)
    monkeypatch.setattr(acquisition.alpaca, "collect_contract", fake_collect)

    assert acquisition.collect_scanner(
        manifest_path=Path("outer.json"),
        env_path=Path(".env"),
        max_days=1,
    ) == {"valid": True}
    assert lock_state["held"] is False
