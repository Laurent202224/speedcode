from __future__ import annotations

import unittest

from backend.core.matching import load_matching_config, recommend_hospitals_for_diagnosis


class MatchingTests(unittest.TestCase):
    def test_active_config_uses_gaya_demo_dataset(self) -> None:
        config = load_matching_config()
        self.assertEqual(config.data_path.name, "demo_2_eye.json")

    def test_gaya_dentistry_query_returns_nearby_provider(self) -> None:
        matches = recommend_hospitals_for_diagnosis(
            "Dentistry",
            24.786,
            85.006,
            limit=3,
        )

        self.assertGreaterEqual(len(matches), 1)
        self.assertLess(matches[0]["distance_km"], 5)
        self.assertIn("Dental", matches[0]["name"])


if __name__ == "__main__":
    unittest.main()
