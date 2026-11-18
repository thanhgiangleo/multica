"""Regression tests for JSON contract comparison; no servers or credentials."""
import unittest

from lab import difference_paths


class DifferencePathsTests(unittest.TestCase):
    def test_integral_json_numbers_ignore_encoder_spelling(self):
        self.assertEqual(difference_paths(0, 0.0), [])
        self.assertEqual(difference_paths(1, 1.0), [])
        self.assertEqual(difference_paths(-2.0, -2), [])
        self.assertEqual(difference_paths({"position": 0}, {"position": 0.0}), [])

    def test_changed_number_still_fails(self):
        self.assertEqual(difference_paths(1, 1.5), ["/"])
        self.assertEqual(difference_paths({"position": 0}, {"position": 1.0}), ["/position"])
        self.assertEqual(difference_paths(2**53 + 1, float(2**53 + 1)), ["/"])

    def test_booleans_and_strings_are_not_numbers(self):
        for left, right in ((True, 1), (False, 0.0), ("0", 0), (0, False)):
            with self.subTest(left=left, right=right):
                self.assertEqual(difference_paths(left, right), ["/"])

    def test_object_keys_and_array_order_remain_contract(self):
        self.assertEqual(difference_paths({"a": None}, {}), ["/a"])
        self.assertEqual(difference_paths([1, 2], [2, 1]), ["/0", "/1"])
        self.assertEqual(difference_paths([1], [1, 2]), ["/length"])


if __name__ == "__main__":
    unittest.main()
