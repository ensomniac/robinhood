import unittest

from session_mode import MODES, ModeSelectionError, select_mode


class ModeSelectionTests(unittest.TestCase):
    def test_numeric_and_named_choices_resolve_same_mode(self):
        self.assertEqual(select_mode(1), select_mode("live"))
        self.assertEqual(select_mode("3").key, "historical")

    def test_only_live_mode_allows_broker_actions(self):
        allowed = [mode.key for mode in MODES if mode.broker_actions_allowed]

        self.assertEqual(allowed, ["live"])

    def test_invalid_mode_is_rejected(self):
        with self.assertRaises(ModeSelectionError):
            select_mode("automatic")


if __name__ == "__main__":
    unittest.main()
