import unittest
from decimal import Decimal

from shop_bot.bot.pricing import tier_price_as_decimal


class TierPriceNormalizationTests(unittest.TestCase):
    def test_empty_values_become_zero(self):
        for value in (None, "", "None"):
            with self.subTest(value=value):
                self.assertEqual(tier_price_as_decimal(value), Decimal("0"))

    def test_numeric_values_keep_their_amount(self):
        self.assertEqual(tier_price_as_decimal("12.50"), Decimal("12.50"))
        self.assertEqual(tier_price_as_decimal(7), Decimal("7"))


if __name__ == "__main__":
    unittest.main()
