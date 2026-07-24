from __future__ import annotations

import oversold_scanner_target_audit as audit


def test_target_boundary_inspection_rejects_target_reuse(
    monkeypatch,
) -> None:
    manifest = {
        "manifest_sha256": "manifest",
        "requested_dates": ["2026-07-17"],
        "collection_contract": {
            "required_session_dates": ["2026-07-16", "2026-07-17"],
            "target_session_collection": (
                "opening_query_only_no_post_09_35_rows"
            ),
            "reusable_source": {"session_dates": ["2026-07-17"]},
        },
    }
    monkeypatch.setattr(
        audit.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )
    monkeypatch.setattr(
        audit.source,
        "_scanner_manifest_path",
        lambda: audit.source.SCANNER_SELECTION_PATH,
    )
    monkeypatch.setattr(
        audit.source.base.alpaca,
        "load_contract",
        lambda _path: manifest,
    )
    monkeypatch.setattr(
        audit.source,
        "_read",
        lambda path: (
            {"scanner_manifest_sha256": "manifest"}
            if path == audit.source.CONTROLLER_BINDING_PATH
            else {"selected_dates": ["2026-07-17"]}
        ),
    )
    monkeypatch.setattr(
        audit.source.base.alpaca,
        "collection_status",
        lambda _manifest, store: {
            "session_files": {"ready": 0},
            "provider_requests": 0,
            "provider_retries": 0,
        },
    )

    try:
        audit.inspect(store=object())
    except audit.OversoldScannerTargetAuditError as exc:
        assert "boundary" in str(exc)
    else:
        raise AssertionError("target full-session reuse must fail closed")
