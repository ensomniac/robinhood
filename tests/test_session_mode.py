import unittest

from session_mode import MODES, ModeSelectionError, select_mode


class ModeSelectionTests(unittest.TestCase):
    def test_numeric_and_named_choices_resolve_same_mode(self):
        self.assertEqual(select_mode(1), select_mode("live"))
        self.assertEqual(select_mode("3").key, "historical")
        self.assertEqual(select_mode("5").key, "learning")

    def test_only_live_mode_allows_broker_actions(self):
        allowed = [mode.key for mode in MODES if mode.broker_actions_allowed]

        self.assertEqual(allowed, ["live"])

    def test_learning_mode_is_bounded_and_has_no_broker_authority(self):
        mode = select_mode("learning")

        self.assertEqual(mode.key, "learning")
        self.assertFalse(mode.broker_actions_allowed)
        self.assertIn("learning_loop.py audit", mode.next_step)
        self.assertIn("learning_cadence.py run", mode.next_step)

    def test_invalid_mode_is_rejected(self):
        with self.assertRaises(ModeSelectionError):
            select_mode("automatic")


if __name__ == "__main__":
    unittest.main()
