import earnings_pead_backfill as backfill
import earnings_pead_expansion as expansion


def test_backfill_context_changes_only_the_historical_target() -> None:
    original = (
        expansion.EXPANSION_ID,
        expansion.EVENT_START,
        expansion.EVENT_END,
    )
    with backfill._configured():
        assert expansion.EXPANSION_ID == backfill.BACKFILL_ID
        assert expansion.EVENT_START == "2024-01-01"
        assert expansion.EVENT_END == "2024-12-31"
        assert (
            expansion._normalize(
                {
                    "symbol": "EDGE",
                    "report": {
                        "date": "2024-10-15",
                        "timing": "pm",
                        "verified": True,
                    },
                    "eps": {"actual": "1.2", "estimate": "1.0"},
                }
            )
            is not None
        )
    assert (
        expansion.EXPANSION_ID,
        expansion.EVENT_START,
        expansion.EVENT_END,
    ) == original
