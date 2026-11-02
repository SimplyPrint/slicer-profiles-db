import unittest

from slicer_profiles_db.parsers.cura import _SafeExpressionEvaluator


class CuraExpressionTests(unittest.TestCase):
    def test_extruder_minus_one_is_the_default_extruder(self) -> None:
        # Every *_extruder_nr setting defaults to -1; prime_tower_position_x
        # fell back to its default 200 on every printer while -1 was unresolved.
        scalar = _SafeExpressionEvaluator({"skirt_gap": 3}, default_extruder_position=0)
        self.assertEqual(scalar.evaluate("extruderValue(-1, 'skirt_gap')"), 3)

        per_tool = _SafeExpressionEvaluator(
            {}, {"skirt_gap": [3, 7]}, default_extruder_position=0
        )
        self.assertEqual(per_tool.evaluate("extruderValue(-1, 'skirt_gap')"), 3)
        self.assertEqual(per_tool.evaluate("extruderValue(1, 'skirt_gap')"), 7)


if __name__ == "__main__":
    unittest.main()
