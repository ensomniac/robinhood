from __future__ import annotations

import gzip
import hashlib
import json
import subprocess
import tempfile
from pathlib import Path

import challenger_orb_retest_selected_pairs as base_freezer
import challenger_orb_retest_selected_pairs3_v2 as freezer
import challenger_orb_retest_selected_pairs3_v2_inspection as inspector
import challenger_orb_retest_selected_pairs_inspection as base_inspector


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")


def test_repair_keeps_scanner_v1_and_versions_only_the_pair_contract():
    assert freezer.DATASET_ID.endswith("challenger-orb-retest-tranche3-v2")
    assert freezer.PREENTRY_DATASET_ID.endswith("2026-07-22-tranche3-v2")
    assert freezer.SCANNER_DATASET_ID.endswith("challenger-orb-retest-tranche3-v1")
    assert "tranche3-v1" in str(freezer.DEFAULT_SCANNER_MANIFEST)
    assert "tranche3-v1" in str(freezer.DEFAULT_OUTER_MANIFEST)


def test_repaired_checker_accepts_exact_blob_from_pushed_ancestor(monkeypatch):
    commit = "a" * 40
    head = "b" * 40
    content = b"frozen input\n"
    digest = hashlib.sha256(content).hexdigest()
    binding = {"path": "fixture.json", "sha256": digest}
    manifest = {
        "source_bindings": {"source": binding},
        "implementation_contract": {"implementation": binding},
        "publication_contract": {
            "source": {**binding, "commit": commit},
            "implementation": {**binding, "commit": commit},
        },
    }
    monkeypatch.setattr(freezer, "source_paths", lambda: {"source": Path("a")})
    monkeypatch.setattr(
        freezer, "implementation_paths", lambda: {"implementation": Path("b")}
    )
    monkeypatch.setattr(base_inspector, "_verify_binding", lambda *_args: None)

    def fake_run(args, **_kwargs):
        if args[1:3] == ["rev-parse", "HEAD"]:
            return subprocess.CompletedProcess(args, 0, stdout=head + "\n")
        if args[1:3] == ["rev-parse", "@{upstream}"]:
            return subprocess.CompletedProcess(args, 0, stdout=head + "\n")
        if args[1:3] == ["merge-base", "--is-ancestor"]:
            return subprocess.CompletedProcess(args, 0, stdout=b"")
        if args[1] == "show":
            return subprocess.CompletedProcess(args, 0, stdout=content)
        if args[1:3] == ["status", "--porcelain"]:
            return subprocess.CompletedProcess(args, 0, stdout="")
        raise AssertionError(args)

    monkeypatch.setattr(inspector.subprocess, "run", fake_run)
    inspector._verify_contract_bindings_v2(manifest, require_published=True)


def test_freezer_binds_adapter_and_base_without_public_identities(monkeypatch):
    with (
        tempfile.TemporaryDirectory(dir=freezer.PROJECT_ROOT) as repo_directory,
        tempfile.TemporaryDirectory() as store_directory,
    ):
        root = Path(repo_directory)
        store = Path(store_directory) / "history"
        store.mkdir()
        paths = [root / f"source-{index}.json" for index in range(5)]
        for path in paths:
            _write_json(path, {"fixture": path.name})
        private = {
            "schema_version": 1,
            "dataset_id": freezer.DATASET_ID,
            "selected_pair_count": 1,
            "selected_pairs": [
                {
                    "date": "2026-03-03",
                    "symbol": "AAA",
                    "instrument_id": "FIGI-COMPOSITE:AAA",
                }
            ],
        }
        public = {
            "source_dataset_id": freezer.SCANNER_DATASET_ID,
            "source_manifest_sha256": freezer.SCANNER_MANIFEST_SHA256,
            "source_summary_sha256": "a" * 64,
            "source_detail_sha256": "b" * 64,
            "requested_dates": ["2026-03-03"],
            "selected_pair_count": 1,
            "daily_shortlists": [
                {
                    "date": "2026-03-03",
                    "shortlist_count": 1,
                    "shortlist_sha256": "c" * 64,
                }
            ],
            "private_selection_content_sha256": base_freezer._sha256_json(private),
        }
        hypothesis = {
            "contract_sha256": freezer.HYPOTHESIS_SHA256,
            "primary_trial_id": freezer.PRIMARY_TRIAL_ID,
        }
        monkeypatch.setattr(
            freezer,
            "_validated_selection",
            lambda **_kwargs: (private, public, hypothesis),
        )
        monkeypatch.setattr(freezer, "MINIMUM_FREE_BYTES", 1)
        env = root / ".env"
        env.write_text(
            f"LOCAL_HISTORICAL_DATA_ROOT={store}\nLOCAL_HISTORICAL_MIN_FREE_GIB=1\n",
            encoding="utf-8",
        )
        path, manifest = freezer.freeze_selected_pairs(
            summary_path=paths[0],
            inspection_path=paths[1],
            scanner_manifest_path=paths[2],
            outer_manifest_path=paths[3],
            hypothesis_path=paths[4],
            env_path=env,
            output_root=root / "manifests",
            status_path=root / "status.json",
            require_published=False,
        )

        assert path.is_file()
        assert set(manifest["implementation_contract"]) == {
            "freezer",
            "base_freezer",
            "inspector",
            "base_inspector",
            "selection_primitive",
            "trigger",
        }
        assert manifest["publication_contract"] == {}
        assert "AAA" not in path.read_text(encoding="utf-8")
        private_path = (
            store
            / "_derived/scanner_selected_pairs"
            / freezer.DATASET_ID
            / "selected-pairs.json.gz"
        )
        with gzip.open(private_path, "rt", encoding="utf-8") as source:
            assert json.load(source) == private


def test_inspector_scopes_third_tranche_and_restores_base(monkeypatch):
    original_dataset_id = base_inspector.DATASET_ID
    observed: dict[str, object] = {}

    def fake_inspect(**kwargs):
        observed.update(
            {
                "dataset_id": base_inspector.DATASET_ID,
                "summary": base_inspector.DEFAULT_SUMMARY,
                "implementations": base_inspector._expected_implementation_paths(),
                "publication_checker": base_inspector._verify_contract_bindings,
                "kwargs": kwargs,
            }
        )
        return {"status": "FROZEN_READY"}

    monkeypatch.setattr(base_inspector, "inspect_selected_pairs", fake_inspect)
    result = inspector.inspect_selected_pairs(
        manifest_path=Path("manifest.json"),
        env_path=Path("fixture.env"),
        status_path=Path("status.json"),
        require_published=False,
    )

    assert result == {"status": "FROZEN_READY"}
    assert observed["dataset_id"] == freezer.DATASET_ID
    assert observed["summary"] == freezer.DEFAULT_SUMMARY
    assert observed["implementations"] == freezer.implementation_paths()
    assert observed["publication_checker"] is inspector._verify_contract_bindings_v2
    assert base_inspector.DATASET_ID == original_dataset_id
