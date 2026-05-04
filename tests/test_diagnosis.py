from __future__ import annotations

import unittest

from backend.core.diagnosis import classify_diagnosis


class DiagnosisClassificationTests(unittest.TestCase):
    def test_toothache_maps_to_dentistry(self) -> None:
        self.assertEqual(classify_diagnosis("severe toothache").english_name, "Dentistry")

    def test_kidney_stone_maps_to_urology(self) -> None:
        self.assertEqual(
            classify_diagnosis("kidney stone pain").english_name,
            "Urology",
        )

    def test_eye_pain_maps_to_ophthalmology(self) -> None:
        self.assertEqual(
            classify_diagnosis("blurry vision and eye pain").english_name,
            "Ophthalmology",
        )


if __name__ == "__main__":
    unittest.main()
