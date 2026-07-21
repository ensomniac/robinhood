import gzip
import hashlib
import json
import tempfile
from pathlib import Path

import pytest

import challenger_orb_retest_selected_pairs as freezer
import challenger_orb_retest_selected_pairs_inspection as inspector
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract


def _sha256_json(value):
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _fixture(root: Path):
    day = "2026-03-03"
    scanner_id = "dataset-production-scanner-replay-challenger-selected-pair-test"
    selection_id = "dataset-selected-candidate-contract-challenger-test"
    preentry_id = "dataset-challenger-orb-retest-preentry-test"
    selected = {
        "symbol": "AAA",
        "instrument_id": "FIGI-COMPOSITE:AAA",
        "opening_relative_volume": 2.0,
        "opening_return": 0.02,
        "rank": 1,
    }
    row = {
        "symbol": selected["symbol"],
        "instrument_id": selected["instrument_id"],
        "primary_exchange": "XNAS",
        "disposition": "eligible",
        "opening_rvol_rank": 1,
        "open_price": 10.0,
        "opening_high": 10.3,
        "opening_low": 9.9,
        "opening_close": 10.2,
        "opening_volume": 200,
        "opening_relative_volume": 2.0,
        "opening_return": 0.02,
        "average_daily_volume_14": 2_000_000,
        "daily_atr_14": 1.0,
        "prior_close": 10.0,
    }
    detail = {
        "dataset_id": scanner_id,
        "scanner_rules_sha256": "r" * 64,
        "security_master_sha256": "s" * 64,
        "split_actions_sha256": "p" * 64,
        "dates": {
            day: {"selected_symbols": [selected["symbol"]], "evaluations": [row]}
        },
    }
    detail_path = root / "private-scanner-detail.json"
    _write_json(detail_path, detail)
    scanner_path, scanner_manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": scanner_id,
            "registered_at": "2026-03-01T00:00:00+00:00",
            "requested_dates": [day],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": ["README.md"],
                "inspected": False,
            },
            "collection_contract": {"provider": "fixture"},
        },
        root / "scanner-manifests",
    )
    outer_path, outer_manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-challenger-orb-retest-acquisition-test",
            "registered_at": "2026-03-01T00:00:00+00:00",
            "requested_dates": [day],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": ["README.md"],
                "inspected": False,
            },
            "full_universe_market_contract": {
                "scanner_manifest_sha256": scanner_manifest["manifest_sha256"]
            },
        },
        root / "outer-manifests",
    )
    summary = {
        "dataset_id": scanner_id,
        "status": "READY",
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "complete_universe": True,
        "selection_is_dynamic": True,
        "scanner_rules_sha256": detail["scanner_rules_sha256"],
        "security_master_sha256": detail["security_master_sha256"],
        "split_actions_sha256": detail["split_actions_sha256"],
        "requested_dates": [day],
        "dates": [
            {
                "date": day,
                "shortlist_count": 1,
                "shortlist_sha256": _sha256_json([selected]),
            }
        ],
        "detailed_artifact": {
            "local_path": str(detail_path.relative_to(freezer.PROJECT_ROOT)),
            "public": False,
            "sha256": _sha256_file(detail_path),
        },
        "source": {"contract_sha256": scanner_manifest["manifest_sha256"]},
    }
    summary_path = root / "scanner-summary.json"
    _write_json(summary_path, summary)
    inspection_path = root / "scanner-inspection.json"
    _write_json(
        inspection_path,
        {
            "dataset_id": scanner_id,
            "status": "INSPECTED",
            "valid": True,
            "manifest_sha256": scanner_manifest["manifest_sha256"],
            "summary_sha256": _sha256_file(summary_path),
            "detail_sha256": _sha256_file(detail_path),
            "completed_dates": 1,
            "total_selected": 1,
        },
    )
    return {
        "day": day,
        "scanner_id": scanner_id,
        "selection_id": selection_id,
        "preentry_id": preentry_id,
        "summary": summary_path,
        "inspection": inspection_path,
        "scanner_manifest": scanner_path,
        "scanner_manifest_sha256": scanner_manifest["manifest_sha256"],
        "outer_manifest": outer_path,
        "outer_manifest_sha256": outer_manifest["manifest_sha256"],
        "hypothesis": freezer.DEFAULT_HYPOTHESIS,
    }


def _patch_contract(monkeypatch, fixture):
    for module in (freezer, inspector):
        monkeypatch.setattr(module, "DATASET_ID", fixture["selection_id"])
        monkeypatch.setattr(module, "PREENTRY_DATASET_ID", fixture["preentry_id"])
        monkeypatch.setattr(module, "SCANNER_DATASET_ID", fixture["scanner_id"])
        monkeypatch.setattr(module, "EXPECTED_SELECTED_PAIRS", 1)
        monkeypatch.setattr(module, "EXPECTED_DATES", 1)
        monkeypatch.setattr(
            module,
            "SCANNER_MANIFEST_SHA256",
            fixture["scanner_manifest_sha256"],
        )
        monkeypatch.setattr(
            module, "OUTER_MANIFEST_SHA256", fixture["outer_manifest_sha256"]
        )
    monkeypatch.setattr(inspector, "DEFAULT_SUMMARY", fixture["summary"])
    monkeypatch.setattr(inspector, "DEFAULT_INSPECTION", fixture["inspection"])
    monkeypatch.setattr(
        inspector, "DEFAULT_SCANNER_MANIFEST", fixture["scanner_manifest"]
    )
    monkeypatch.setattr(inspector, "DEFAULT_OUTER_MANIFEST", fixture["outer_manifest"])
    monkeypatch.setattr(inspector, "DEFAULT_HYPOTHESIS", fixture["hypothesis"])


def _freeze(root, store, fixture):
    store.mkdir(parents=True, exist_ok=True)
    env = root / ".env"
    env.write_text(
        f"LOCAL_HISTORICAL_DATA_ROOT={store}\nLOCAL_HISTORICAL_MIN_FREE_GIB=1\n",
        encoding="utf-8",
    )
    manifest_path, manifest = freezer.freeze_selected_pairs(
        dataset_id=fixture["selection_id"],
        summary_path=fixture["summary"],
        inspection_path=fixture["inspection"],
        scanner_manifest_path=fixture["scanner_manifest"],
        outer_manifest_path=fixture["outer_manifest"],
        hypothesis_path=fixture["hypothesis"],
        env_path=env,
        output_root=root / "selected-pair-manifests",
        status_path=root / "freeze-status.json",
        require_published=False,
    )
    return env, manifest_path, manifest


def test_freeze_and_independent_inspection_keep_exact_pairs_private(monkeypatch):
    with (
        tempfile.TemporaryDirectory(dir=freezer.PROJECT_ROOT) as directory,
        tempfile.TemporaryDirectory() as store_directory,
    ):
        root = Path(directory)
        store = Path(store_directory) / "history"
        fixture = _fixture(root)
        _patch_contract(monkeypatch, fixture)

        env, manifest_path, manifest = _freeze(root, store, fixture)
        second_path, second = freezer.freeze_selected_pairs(
            dataset_id=fixture["selection_id"],
            summary_path=fixture["summary"],
            inspection_path=fixture["inspection"],
            scanner_manifest_path=fixture["scanner_manifest"],
            outer_manifest_path=fixture["outer_manifest"],
            hypothesis_path=fixture["hypothesis"],
            env_path=env,
            output_root=root / "selected-pair-manifests",
            status_path=root / "freeze-status.json",
            require_published=False,
        )
        result = inspector.inspect_selected_pairs(
            manifest_path=manifest_path,
            env_path=env,
            status_path=root / "inspection-status.json",
            require_published=False,
        )

        assert second_path == manifest_path
        assert second == manifest
        assert load_frozen_dataset_contract(manifest_path) == manifest
        assert result["status"] == "FROZEN_READY"
        assert result["selected_pair_count"] == 1
        assert result["target_outcomes_observed_or_derived"] is False
        assert "AAA" not in manifest_path.read_text(encoding="utf-8")
        assert "AAA" not in json.dumps(result)
        assert (
            store
            / "_derived/scanner_selected_pairs"
            / fixture["selection_id"]
            / "selected-pairs.json.gz"
        ).is_file()


def test_inspection_fails_closed_when_private_graph_changes(monkeypatch):
    with (
        tempfile.TemporaryDirectory(dir=freezer.PROJECT_ROOT) as directory,
        tempfile.TemporaryDirectory() as store_directory,
    ):
        root = Path(directory)
        store = Path(store_directory) / "history"
        fixture = _fixture(root)
        _patch_contract(monkeypatch, fixture)
        env, manifest_path, _ = _freeze(root, store, fixture)
        private_path = (
            store
            / "_derived/scanner_selected_pairs"
            / fixture["selection_id"]
            / "selected-pairs.json.gz"
        )
        with gzip.open(private_path, "rt", encoding="utf-8") as source:
            private = json.load(source)
        private["selected_pairs"][0]["rank"] = 2
        freezer.selected_pairs._write_private(private_path, private)

        with pytest.raises(
            inspector.ChallengerSelectedPairsInspectionError,
            match="private selected-pair graph does not rebuild",
        ):
            inspector.inspect_selected_pairs(
                manifest_path=manifest_path,
                env_path=env,
                status_path=root / "inspection-status.json",
                require_published=False,
            )


def test_inspection_rejects_bound_upstream_drift(monkeypatch):
    with (
        tempfile.TemporaryDirectory(dir=freezer.PROJECT_ROOT) as directory,
        tempfile.TemporaryDirectory() as store_directory,
    ):
        root = Path(directory)
        store = Path(store_directory) / "history"
        fixture = _fixture(root)
        _patch_contract(monkeypatch, fixture)
        env, manifest_path, _ = _freeze(root, store, fixture)
        inspection = json.loads(fixture["inspection"].read_text(encoding="utf-8"))
        inspection["valid"] = False
        _write_json(fixture["inspection"], inspection)

        with pytest.raises(
            inspector.ChallengerSelectedPairsInspectionError,
            match="bound file drifted",
        ):
            inspector.inspect_selected_pairs(
                manifest_path=manifest_path,
                env_path=env,
                status_path=root / "inspection-status.json",
                require_published=False,
            )


def test_freeze_refuses_preexisting_downstream_artifact(monkeypatch):
    with (
        tempfile.TemporaryDirectory(dir=freezer.PROJECT_ROOT) as directory,
        tempfile.TemporaryDirectory() as store_directory,
    ):
        root = Path(directory)
        store = Path(store_directory) / "history"
        fixture = _fixture(root)
        _patch_contract(monkeypatch, fixture)
        store.mkdir(parents=True)
        target = (
            store
            / "_derived/challenger_orb_retest_preentry"
            / fixture["preentry_id"]
            / "unexpected.json"
        )
        target.parent.mkdir(parents=True)
        target.write_text("{}\n", encoding="utf-8")
        env = root / ".env"
        env.write_text(
            f"LOCAL_HISTORICAL_DATA_ROOT={store}\nLOCAL_HISTORICAL_MIN_FREE_GIB=1\n",
            encoding="utf-8",
        )

        with pytest.raises(
            freezer.ChallengerSelectedPairsError,
            match="artifacts existed before pair freeze",
        ):
            freezer.freeze_selected_pairs(
                dataset_id=fixture["selection_id"],
                summary_path=fixture["summary"],
                inspection_path=fixture["inspection"],
                scanner_manifest_path=fixture["scanner_manifest"],
                outer_manifest_path=fixture["outer_manifest"],
                hypothesis_path=fixture["hypothesis"],
                env_path=env,
                output_root=root / "selected-pair-manifests",
                status_path=root / "freeze-status.json",
                require_published=False,
            )
