from __future__ import annotations

import oversold_scanner_builder_v2 as builder


def test_builder_uses_private_flat_run_root() -> None:
    assert builder.source.RUN_ROOT.name == "scanner_replay_v2"
    assert builder.source.DETAIL_PATH.parent == builder.source.RUN_ROOT
    assert builder.source.SUMMARY_PATH.parent == builder.source.RUN_ROOT
    assert builder.BUILD_STATUS_ROOT.parent == builder.source.ROOT
