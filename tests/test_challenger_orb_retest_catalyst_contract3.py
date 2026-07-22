from __future__ import annotations

import hashlib
import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace

import challenger_orb_retest_catalyst_contract3 as contract
import challenger_orb_retest_catalyst_contract3_inspection as inspector
import development_catalyst_contract as base


def _stable() -> dict[str, object]:
    return {
        "selection_contract": {
            "requested_date_count": 1,
            "selected_pair_count": 1,
            "daily_shortlists": [
                {"date": "2026-03-03", "shortlist_count": 1, "shortlist_sha256": "a" * 64}
            ],
            "daily_shortlists_sha256": "b" * 64,
            "private_selection_content_sha256": "c" * 64,
        },
        "upstream_contract": {},
        "source_rules": {"primary_evidence_only": True},
        "acquisition_contract": {"whole_source_fidelity_required": True},
        "private_record_contract": {"exact_rows_outside_git": True},
        "outcome_lock": {"target_outcomes_observed_or_derived": False},
        "implementation_contract": contract._implementation_contract(),
        "pre_freeze_target_artifact_count": 0,
    }


def test_contract_uses_ready_pair_v2_and_scanner_v1():
    assert contract.SELECTION_DATASET_ID.endswith("challenger-orb-retest-tranche3-v2")
    assert contract.DATASET_ID.endswith("challenger-orb-retest-tranche3-v1")
    assert "tranche3-v2" in str(contract.SOURCE_MANIFEST)
    assert "tranche3-v1" in str(contract.SCANNER_MANIFEST)


def test_publication_accepts_exact_blob_from_pushed_ancestor(monkeypatch):
    commit = "a" * 40
    head = "b" * 40
    content = b"source contract input\n"
    digest = hashlib.sha256(content).hexdigest()
    publication = {
        "implementation": {
            "commit": commit,
            "path": "implementation.json",
            "sha256": digest,
        },
        "source": {
            "commit": commit,
            "path": "source.json",
            "sha256": digest,
        },
    }
    monkeypatch.setattr(
        contract,
        "implementation_paths",
        lambda: {"implementation": Path("implementation")},
    )
    monkeypatch.setattr(contract.base, "_repo_path", lambda path: f"{path}.json")
    monkeypatch.setattr(contract.base, "_sha256_file", lambda _path: digest)

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
    inspector._verify_publication(
        {"publication_contract": publication},
        paths={"source": Path("source")},
        require_published=True,
    )


def test_configuration_is_scoped_and_binds_adapter_surface():
    original_dataset = base.DATASET_ID
    with contract.configured_base():
        assert base.DATASET_ID == contract.DATASET_ID
        assert base.PRIVATE_NAMESPACE == contract.PRIVATE_NAMESPACE
        assert base._implementation_contract() == contract._implementation_contract()
    assert base.DATASET_ID == original_dataset
    assert set(contract._implementation_contract()["files"]) == {
        "contract_adapter",
        "base_contract_builder",
        "inspector_adapter",
        "source_semantics",
    }


def test_freeze_and_inspect_preserve_closed_source_contract(monkeypatch):
    with (
        tempfile.TemporaryDirectory(dir=contract.PROJECT_ROOT) as repo_directory,
        tempfile.TemporaryDirectory() as store_directory,
    ):
        root = Path(repo_directory)
        store = Path(store_directory)
        stable = _stable()
        config = SimpleNamespace(root=store, min_free_bytes=1)
        monkeypatch.setattr(
            contract, "stable_contract", lambda **_kwargs: (stable, config)
        )
        sources = [root / f"source-{index}.json" for index in range(7)]
        for path in sources:
            path.write_text("{}\n", encoding="utf-8")
        path, manifest = contract.freeze_contract(
            source_manifest_path=sources[0],
            env_path=root / ".env",
            output_root=root / "manifests",
            scanner_manifest_path=sources[1],
            scanner_summary_path=sources[2],
            scanner_inspection_path=sources[3],
            security_master_source_path=sources[4],
            strategy_source_path=sources[5],
            selection_doc_path=sources[6],
            require_published=False,
        )
        result = inspector.inspect_contract(
            manifest_path=path,
            env_path=root / ".env",
            status_path=root / "status.json",
            source_manifest_path=sources[0],
            scanner_manifest_path=sources[1],
            scanner_summary_path=sources[2],
            scanner_inspection_path=sources[3],
            security_master_source_path=sources[4],
            strategy_source_path=sources[5],
            selection_doc_path=sources[6],
            require_published=False,
        )

        assert manifest["publication_contract"] == {}
        assert result["status"] == "FROZEN_READY"
        assert result["selected_pair_count"] == 1
        assert result["target_sources_accessed"] is False
        assert result["target_outcomes_observed_or_derived"] is False
