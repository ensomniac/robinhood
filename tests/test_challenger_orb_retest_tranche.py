import json
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import challenger_orb_retest_tranche as tranche


def _calendar(count=500):
    start = date.fromisoformat("2023-01-03")
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def test_selection_is_exact_disjoint_deterministic_and_challenger_bound(monkeypatch):
    monkeypatch.setattr(tranche, "ELIGIBLE_END", "2026-12-31")
    calendar = _calendar()
    excluded = calendar[30:80]
    snapshot = {
        "excluded_dates": excluded,
        "excluded_dates_sha256": tranche._sha256_json(excluded),
    }
    first, required = tranche.build_selection(
        calendar_dates=calendar, exclusion_snapshot=snapshot
    )
    second, _ = tranche.build_selection(
        calendar_dates=calendar, exclusion_snapshot=snapshot
    )

    assert first == second
    assert first["dataset_id"] == tranche.DATASET_ID
    assert first["hypothesis_sha256"] == tranche.retest.HYPOTHESIS_SHA256
    assert len(first["selected_dates"]) == 100
    assert not (set(first["selected_dates"]) & set(excluded))
    assert len(required) == first["required_session_count"]
    assert first["substitution_allowed"] is False
    assert first["target_outcomes_observed_or_derived"] is False


def test_insufficient_disjoint_pool_fails_without_substitution(monkeypatch):
    monkeypatch.setattr(tranche, "ELIGIBLE_END", "2026-12-31")
    calendar = _calendar(140)
    excluded = calendar[20:100]
    with pytest.raises(tranche.ChallengerTrancheError, match="needs 100"):
        tranche.build_selection(
            calendar_dates=calendar,
            exclusion_snapshot={
                "excluded_dates": excluded,
                "excluded_dates_sha256": tranche._sha256_json(excluded),
            },
        )


def test_v3_falsification_prerequisite_must_be_inspected(monkeypatch):
    valid_calendar = {
        "status": "COLLECTION_INSPECTED",
        "inspected": True,
        "sessions": 887,
        "calendar_sha256": "calendar",
        "target_outcomes_observed_or_derived": False,
    }
    values = {
        tranche.CALENDAR_STATUS: valid_calendar,
        tranche.CALENDAR_SOURCE: {
            "calendar_sha256": "calendar",
            "target_outcomes_observed_or_derived": False,
        },
        tranche.QUALIFICATION_RESULT: {
            "status": "QUALIFICATION_COMPLETE",
            "inspected": False,
            "survivors": 0,
            "outcome_contract_permitted": False,
            "target_outcomes_observed_or_derived": False,
        },
        tranche.HYPOTHESIS: {
            "contract_sha256": tranche.retest.HYPOTHESIS_SHA256,
            "trial_family": [{}],
        },
    }
    monkeypatch.setattr(tranche, "_read_json", lambda path: values[path])
    monkeypatch.setattr(tranche, "_sha256_file", lambda path: "calendar")
    with pytest.raises(tranche.ChallengerTrancheError, match="falsification"):
        tranche._validate_prerequisites()


def test_prior_selections_include_every_existing_frozen_tranche():
    assert tranche.base.DEFAULT_SELECTION in tranche.PRIOR_SELECTIONS
    assert len(tranche.PRIOR_SELECTIONS) == 4


def test_inspected_public_array_evidence_is_hash_bound_without_target_dates(monkeypatch):
    with TemporaryDirectory(dir=tranche.PROJECT_ROOT) as directory:
        root = Path(directory)
        relative = root.relative_to(tranche.PROJECT_ROOT) / "calendar.json"
        path = tranche.PROJECT_ROOT / relative
        path.write_text(json.dumps([{"date": "2023-01-03"}]), encoding="utf-8")
        registry = root / "registry"
        registry.mkdir()
        (registry / "DATASETS.jsonl").write_text("fixture\n", encoding="utf-8")
        monkeypatch.setattr(
            tranche,
            "current_entities",
            lambda name, root: {
                "dataset-calendar": {
                    "entity_id": "dataset-calendar",
                    "payload": {
                        "inspected": True,
                        "evidence_paths": [str(relative)],
                    },
                }
            },
        )

        dates, evidence = tranche._registered_inspected_exclusions(
            registry_root=registry
        )

        assert dates == set()
        assert evidence["json_evidence_artifacts"] == 1
        assert evidence["artifacts"][0]["sha256"] == tranche._sha256_file(path)
