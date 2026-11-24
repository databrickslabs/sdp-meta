from unittest import TestCase

from databricks.labs.sdp_meta.quality.targets import target_identity


class QualityTargetIdentityTests(TestCase):
    def test_normalizes_blank_values(self):
        self.assertEqual(
            target_identity({
                "catalog": "",
                "database": " quality ",
                "table": "invalid",
            }),
            (None, "quality", "invalid"),
        )

    def test_path_only_participates_for_non_uc_targets(self):
        target = {
            "catalog": "main",
            "database": "quality",
            "table": "invalid",
            "path": "/quality/invalid",
        }
        self.assertEqual(
            target_identity(target),
            ("main", "quality", "invalid"),
        )
        self.assertEqual(
            target_identity(target, include_path=True),
            ("main", "quality", "invalid", "/quality/invalid"),
        )
