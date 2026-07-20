from pathlib import Path
from tempfile import TemporaryDirectory

import development_sec_accession_chain_contract_inspection as inspection
import development_sec_accession_chain_recovery as recovery


def test_zero_response_artifact_surface_is_explicit() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        selection = recovery._selection_path(root)
        selection.parent.mkdir(parents=True, exist_ok=True)
        selection.write_bytes(b"selection")
        assert inspection._private_artifact_counts(root) == {
            "frozen_selections": 1,
            "terminal_wrappers": 0,
            "collection_indexes": 0,
            "reviewed_results": 0,
        }


def test_terminal_wrapper_is_detected_before_collection() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        wrapper = recovery._wrapper_path(root, "a" * 64)
        wrapper.parent.mkdir(parents=True, exist_ok=True)
        wrapper.write_bytes(b"wrapper")
        assert inspection._private_artifact_counts(root)["terminal_wrappers"] == 1


def test_reviewed_result_is_detected_before_collection() -> None:
    with TemporaryDirectory() as directory:
        root = Path(directory)
        reviewed = recovery._reviewed_path(root)
        reviewed.parent.mkdir(parents=True, exist_ok=True)
        reviewed.write_bytes(b"review")
        assert inspection._private_artifact_counts(root)["reviewed_results"] == 1
