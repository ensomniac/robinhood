from __future__ import annotations

import insider_purchase_data_recovery as recovery


def test_filtered_scope_registers_only_checkpointed_symbols():
    scope = {
        "dates": ["2020-01-02", "2020-01-03"],
        "symbols": ["AAA", "BBB", "CCC"],
    }

    result = recovery._filtered_source_scope(scope, {"AAA", "CCC", "ZZZ"})

    assert result == {
        "dates": ["2020-01-02", "2020-01-03"],
        "symbols": ["AAA", "CCC"],
    }
