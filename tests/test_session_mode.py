import unittest

from session_mode import MODES, ModeSelectionError, select_mode


class ModeSelectionTests(unittest.TestCase):
    def test_numeric_and_named_choices_resolve_same_mode(self):
        self.assertEqual(select_mode(1), select_mode("data"))
        self.assertEqual(select_mode("2").key, "account")
        self.assertEqual(select_mode("4").key, "learning")

    def test_every_mode_forbids_broker_actions(self):
        self.assertEqual(
            [mode.key for mode in MODES if mode.broker_actions_allowed], []
        )

    def test_account_mode_requires_runtime_discovery(self):
        mode = select_mode("account")

        self.assertIn("runtime", mode.next_step)
        self.assertIn("agentic_allowed", mode.next_step)
        self.assertFalse(mode.broker_actions_allowed)

    def test_invalid_mode_is_rejected(self):
        with self.assertRaises(ModeSelectionError):
            select_mode("live")


if __name__ == "__main__":
    unittest.main()
