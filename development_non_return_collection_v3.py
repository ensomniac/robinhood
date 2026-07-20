"""Run the causal pre-entry collector for the combined 102-pair v3 contract.

The completed 21-pair collector is hash-bound by prior public manifests.  This
adapter therefore leaves it unchanged and applies the second-tranche lineage,
identity, paths, and count only inside a scoped call.  Both implementations and
the new source contract are frozen before provider access.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import development_non_return_collection as collector
import development_non_return_v3 as source_v3
from historical_store import HistoricalDayStore
from learning_data import load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = (
    "dataset-development-non-return-preentry-collection-2026-07-20-"
    "tranche-v3-v1"
)
BASE_MANIFEST_SHA256 = (
    "7d5be928f1eea002294b0a407d88b38bc6b4ab047f9ab599419628bcedfdb829"
)
BASE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non_return_manifests"
    / f"{source_v3.DATASET_ID}-{BASE_MANIFEST_SHA256}.json"
)
BASE_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non-return-contract-status.json"
)
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non_return_collection_manifests"
)
DEFAULT_CONTRACT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/"
    "non-return-collection-contract-status.json"
)
DEFAULT_COLLECTION_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non-return-collection-status.json"
)
EXPECTED_PAIRS = source_v3.EXPECTED_COMBINED_POSITIVES
PRIVATE_NAMESPACE = "_derived/development_non_return_collection"


class _SourceContractCompatibility:
    DATASET_ID = source_v3.DATASET_ID
    CALENDAR_QUERY_START = source_v3.CALENDAR_QUERY_START
    CALENDAR_QUERY_END = source_v3.CALENDAR_QUERY_END
    MINIMUM_RESERVE_BYTES = source_v3.MINIMUM_RESERVE_BYTES

    @staticmethod
    def _private_contract_path(store_root: Path) -> Path:
        return source_v3._private_contract_path(store_root)

    @staticmethod
    def _sha256_json(value: Any) -> str:
        return source_v3.base._sha256_json(value)


SOURCE_CONTRACT = _SourceContractCompatibility()


def _load_base_v3(
    env_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], HistoricalDayStore]:
    manifest = load_frozen_dataset_contract(BASE_MANIFEST)
    public = collector._read_json(BASE_PUBLIC_STATUS)
    if (
        manifest.get("dataset_id") != source_v3.DATASET_ID
        or manifest.get("manifest_sha256") != BASE_MANIFEST_SHA256
        or public.get("manifest_sha256") != BASE_MANIFEST_SHA256
        or public.get("status") != "FROZEN_READY"
        or public.get("inspected") is not True
        or public.get("target_artifacts") != 0
        or public.get("target_outcomes_observed_or_derived") is not False
        or manifest.get("acquisition_contract", {}).get(
            "provider_rows_after_final_decision_snapshot_allowed"
        )
        is not False
        or manifest.get("outcome_lock", {}).get(
            "target_outcomes_observed_or_derived"
        )
        is not False
    ):
        raise collector.DevelopmentNonReturnCollectionError(
            "combined v3 non-return contract is not ready"
        )
    store = HistoricalDayStore.from_env(env_path)
    private_path = source_v3._private_contract_path(store.root)
    private = collector._read_gzip(private_path)
    selection = private.get("selection")
    graph = private.get("request_graph")
    selection_hash = (
        collector._sha256_json(selection) if isinstance(selection, Mapping) else ""
    )
    graph_hash = collector._sha256_json(graph) if isinstance(graph, Mapping) else ""
    if (
        not isinstance(selection, Mapping)
        or not isinstance(graph, Mapping)
        or private.get("dataset_id") != source_v3.DATASET_ID
        or selection.get("positive_pair_count") != EXPECTED_PAIRS
        or selection_hash
        != manifest.get("selection_contract", {}).get(
            "private_positive_selection_sha256"
        )
        or selection_hash != public.get("private_positive_selection_sha256")
        or graph_hash
        != manifest.get("acquisition_contract", {}).get(
            "private_request_graph_sha256"
        )
        or graph_hash != private.get("request_graph_sha256")
        or graph_hash != public.get("private_request_graph_sha256")
        or private.get("request_counts")
        != manifest.get("acquisition_contract", {}).get("request_counts")
        or collector._sha256_file(private_path)
        != public.get("private_contract_file_sha256")
        or source_v3._target_artifact_count(store.root) != 0
        or private.get("target_outcomes_observed_or_derived") is not False
    ):
        raise collector.DevelopmentNonReturnCollectionError(
            "private combined v3 graph drifted"
        )
    return manifest, public, private, store


def _implementation_contract_v3() -> dict[str, Any]:
    names = (
        "development_non_return_collection_v3.py",
        "development_non_return_collection.py",
        "development_non_return_v3.py",
        "development_non_return.py",
        "historical_providers.py",
        "historical_service.py",
        "historical_store.py",
        "nasdaq_halts.py",
        "scanner_replay.py",
        "sip_bar_aggregation.py",
        "sip_trade_conditions.py",
    )
    return {name: collector._binding(PROJECT_ROOT / name) for name in names}


@contextmanager
def _configured() -> Iterator[None]:
    replacements: dict[str, Any] = {
        "source_contract": SOURCE_CONTRACT,
        "DATASET_ID": DATASET_ID,
        "BASE_MANIFEST": BASE_MANIFEST,
        "BASE_PUBLIC_STATUS": BASE_PUBLIC_STATUS,
        "DEFAULT_MANIFEST_ROOT": DEFAULT_MANIFEST_ROOT,
        "DEFAULT_CONTRACT_STATUS": DEFAULT_CONTRACT_STATUS,
        "DEFAULT_COLLECTION_STATUS": DEFAULT_COLLECTION_STATUS,
        "PRIVATE_NAMESPACE": PRIVATE_NAMESPACE,
        "EXPECTED_PAIRS": EXPECTED_PAIRS,
        "_load_base": _load_base_v3,
        "_implementation_contract": _implementation_contract_v3,
    }
    originals = {name: getattr(collector, name) for name in replacements}
    try:
        for name, value in replacements.items():
            setattr(collector, name, value)
        yield
    finally:
        for name, value in originals.items():
            setattr(collector, name, value)


def freeze(
    *, env_path: Path, output_root: Path, public_status_path: Path
) -> tuple[Path, dict[str, Any]]:
    collector._published(Path(__file__))
    with _configured():
        return collector.freeze(
            env_path=env_path,
            output_root=output_root,
            public_status_path=public_status_path,
        )


def inspect_contract(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    with _configured():
        return collector.inspect_contract(
            manifest_path=manifest_path,
            env_path=env_path,
            public_status_path=public_status_path,
        )


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    collector._published(Path(__file__))
    with _configured():
        return collector.collect(
            manifest_path=manifest_path,
            env_path=env_path,
            public_status_path=public_status_path,
        )


def inspect_collection(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    with _configured():
        return collector.inspect_collection(
            manifest_path=manifest_path,
            env_path=env_path,
            public_status_path=public_status_path,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("freeze", "inspect-contract", "collect", "inspect")
    )
    parser.add_argument("manifest", nargs="?", type=Path)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_MANIFEST_ROOT)
    parser.add_argument("--contract-status", type=Path, default=DEFAULT_CONTRACT_STATUS)
    parser.add_argument(
        "--collection-status", type=Path, default=DEFAULT_COLLECTION_STATUS
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze(
                env_path=args.env,
                output_root=args.output_root,
                public_status_path=args.contract_status,
            )
            result: Any = {
                "dataset_id": manifest["dataset_id"],
                "manifest_sha256": manifest["manifest_sha256"],
                "path": source_v3._repo_path(path),
                "positive_pairs": EXPECTED_PAIRS,
            }
        elif args.manifest is None:
            raise collector.DevelopmentNonReturnCollectionError(
                f"{args.command} requires a manifest"
            )
        elif args.command == "inspect-contract":
            result = inspect_contract(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.contract_status,
            )
        elif args.command == "collect":
            result = collect(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.collection_status,
            )
        else:
            result = inspect_collection(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.collection_status,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        collector.DevelopmentNonReturnCollectionError,
        collector.HistoricalProviderError,
        collector.HistoricalStoreError,
        collector.LearningDataError,
        collector.NasdaqHaltError,
        collector.ScannerReplayError,
        OSError,
        ValueError,
        collector.requests.RequestException,
    ) as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
