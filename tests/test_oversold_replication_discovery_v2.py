from __future__ import annotations

import oversold_replication_discovery_v2 as discovery


def test_configured_restores_base_identity() -> None:
    before = (
        discovery.base.SUCCESSOR_ID,
        discovery.base.EXPERIMENT_ID,
        discovery.base._confirmation_chain,
    )
    with discovery.configured():
        assert discovery.base.SUCCESSOR_ID == discovery.SUCCESSOR_ID
        assert discovery.base.EXPERIMENT_ID == discovery.EXPERIMENT_ID
        assert (
            discovery.base._confirmation_chain
            is discovery._confirmation_chain
        )
    assert (
        discovery.base.SUCCESSOR_ID,
        discovery.base.EXPERIMENT_ID,
        discovery.base._confirmation_chain,
    ) == before
