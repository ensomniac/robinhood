from __future__ import annotations

from unittest.mock import patch

import challenger_orb_retest_preentry as preentry
import challenger_orb_retest_preentry_reader as causal_reader
import challenger_orb_retest_trigger_runner as runner


def test_exact_reader_substitution_is_scoped_and_restored() -> None:
    original = preentry.LocalHistoricalClient

    with runner.using_exact_reader():
        assert preentry.LocalHistoricalClient is causal_reader.FrozenCausalWindowClient

    assert preentry.LocalHistoricalClient is original


def test_derive_requires_inspected_ready_status(tmp_path) -> None:
    status_path = tmp_path / "status.json"
    preentry._write_json(
        status_path,
        {
            "manifest_sha256": "adapter-manifest",
            "status": "FROZEN_AWAITING_INSPECTION",
            "inspected": False,
            "trigger_artifacts_present": False,
        },
    )
    with patch.object(
        runner,
        "load_contract",
        return_value=({"manifest_sha256": "adapter-manifest"}, object(), {}),
    ), patch.object(preentry, "derive") as derive:
        try:
            runner.derive(
                manifest_path=tmp_path / "manifest.json",
                env_path=tmp_path / ".env",
                status_path=status_path,
            )
        except runner.ChallengerTriggerRunnerError as exc:
            assert "not FROZEN_READY" in str(exc)
        else:
            raise AssertionError("uninspected trigger runner was accepted")
    derive.assert_not_called()
