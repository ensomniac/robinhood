from __future__ import annotations

from datetime import date
from textwrap import dedent

import pytest

import sp500_addition_capacity as source
import sp_mid_small_addition_capacity as capacity


def _release(body: str) -> bytes:
    return dedent(
        f"""
        <html>
        <!-- ITEMDATE: 2018-01-02 16:15:00 EST -->
        <body>{body}</body>
        </html>
        """
    ).encode()


def test_structured_external_smallcap_addition_is_eligible() -> None:
    parsed = capacity.parse_release(
        _release(
            """
            <table>
              <tr><td>Monday, January 8, 2018</td>
                  <td>S&P SmallCap 600</td><td>Addition</td>
                  <td>Example Corp</td><td>EXAM</td></tr>
            </table>
            """
        ),
        source_url="https://press.spglobal.com/2018-example",
        listed_date="2018-01-02",
    )
    assert parsed["terminal_reason"] == (
        "ELIGIBLE_EXTERNAL_MID_SMALL_ADDITION"
    )
    assert parsed["eligible_events"] == [
        {
            "action": "Addition",
            "announcement_at": "2018-01-02T16:15:00-05:00",
            "announcement_date": "2018-01-02",
            "company_name": "Example Corp",
            "effective_date": "2018-01-08",
            "index_name": "S&P SmallCap 600",
            "source_url": "https://press.spglobal.com/2018-example",
            "ticker": "EXAM",
        }
    ]


def test_same_security_cross_index_transfer_is_excluded() -> None:
    parsed = capacity.parse_release(
        _release(
            """
            <table>
              <tr><td>Monday, January 8, 2018</td>
                  <td>S&P MidCap 400</td><td>Deletion</td>
                  <td>Example Corp</td><td>EXAM</td></tr>
              <tr><td></td><td>S&P SmallCap 600</td><td>Addition</td>
                  <td>Example Corp</td><td>EXAM</td></tr>
            </table>
            """
        ),
        source_url="https://press.spglobal.com/2018-example",
        listed_date="2018-01-02",
    )
    assert parsed["eligible_events"] == []
    assert parsed["terminal_reason"] == "CROSS_INDEX_TRANSFERS_ONLY"
    assert parsed["ineligible_rows"] == [
        {
            "action": "Addition",
            "index_name": "S&P SmallCap 600",
            "terminal_reason": "CROSS_INDEX_TRANSFER_EXCLUDED",
            "ticker": "EXAM",
        }
    ]


def test_legacy_midcap_external_addition_is_eligible() -> None:
    parsed = capacity.parse_release(
        _release(
            """
            <p>Example Holdings (NASDAQ: EXAM) will join the index.</p>
            <table>
              <tr><th>S&P MIDCAP 400 - January 8, 2018</th></tr>
              <tr><td>ADDED</td><td>Example Holdings</td></tr>
            </table>
            """
        ),
        source_url="https://press.spglobal.com/2018-example",
        listed_date="2018-01-02",
    )
    assert len(parsed["eligible_events"]) == 1
    assert parsed["eligible_events"][0]["ticker"] == "EXAM"
    assert parsed["eligible_events"][0]["index_name"] == "S&P MidCap 400"


def test_missing_effective_date_preserves_denominator() -> None:
    parsed = capacity.parse_release(
        _release(
            """
            <table>
              <tr><td>TBA</td><td>S&P SmallCap 600</td><td>Addition</td>
                  <td>Example Corp</td><td>EXAM</td></tr>
            </table>
            """
        ),
        source_url="https://press.spglobal.com/2018-example",
        listed_date="2018-01-02",
    )
    assert parsed["eligible_events"] == []
    assert parsed["ineligible_rows"][0]["terminal_reason"] == (
        "MISSING_EFFECTIVE_DATE"
    )


def test_capacity_state_preserves_all_frozen_thresholds() -> None:
    assert (
        capacity.capacity_state(
            total_dates=100,
            development_dates=30,
            confirmation_dates=20,
        )
        == "CAPACITY_READY_FAST_LANE"
    )
    assert (
        capacity.capacity_state(
            total_dates=50,
            development_dates=30,
            confirmation_dates=20,
        )
        == "CAPACITY_READY_LATER_SINGLE_RULE"
    )
    assert (
        capacity.capacity_state(
            total_dates=100,
            development_dates=30,
            confirmation_dates=19,
        )
        == "RETIRED_INSUFFICIENT_FORMAL_CAPACITY"
    )


def test_exact_official_month_typo_requires_frozen_normalization() -> None:
    raw = _release(
        """
        <p>Example Holdings (NASDAQ: EXAM) will join the index.</p>
        <table>
          <tr><th>S&P SMALLCAP 600 - DECMEBER 3, 2018</th></tr>
          <tr><td>ADDED</td><td>Example Holdings</td></tr>
        </table>
        """
    )
    with pytest.raises(
        source.Sp500AdditionCapacityError,
        match="effective date is unparseable: DECMEBER 3, 2018",
    ):
        capacity.parse_release(
            raw,
            source_url="https://press.spglobal.com/2018-example",
            listed_date="2018-01-02",
            normalize_known_official_typo=False,
        )
    parsed = capacity.parse_release(
        raw,
        source_url="https://press.spglobal.com/2018-example",
        listed_date="2018-01-02",
        normalize_known_official_typo=True,
    )
    assert parsed["eligible_events"][0]["effective_date"] == "2018-12-03"


def test_known_typo_policy_rejects_multiple_replacements() -> None:
    with pytest.raises(
        capacity.SpMidSmallAdditionCapacityError,
        match="official month typo occurs more than once",
    ):
        capacity._effective_date(
            "DECMEBER DECMEBER 3, 2018",
            announcement=date(2018, 1, 2),
            normalize_known_official_typo=True,
        )
