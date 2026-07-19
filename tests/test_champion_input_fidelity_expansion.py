import unittest
from pathlib import Path

from champion_input_fidelity_expansion import (
    SOURCE_FIDELITY_ID,
    SOURCE_TRIGGER_ID,
    _source_paths,
)


class ChampionInputFidelityExpansionTests(unittest.TestCase):
    def test_source_paths_join_complete_sec_and_clean_trigger_namespaces(self):
        root = Path("/tmp/history")

        paths = _source_paths(root)

        self.assertIn(SOURCE_FIDELITY_ID, str(paths["selection"]))
        self.assertIn(SOURCE_FIDELITY_ID, str(paths["sec"]))
        self.assertIn(SOURCE_TRIGGER_ID, str(paths["trigger"]))
        self.assertTrue(str(paths["trigger"]).endswith("clean-trigger-index.json.gz"))


if __name__ == "__main__":
    unittest.main()
