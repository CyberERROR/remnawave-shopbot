from decimal import Decimal


def tier_price_as_decimal(value: object) -> Decimal:
    """Convert an optional tier price without turning None into Decimal("None")."""
    if value in (None, "", "None"):
        return Decimal("0")
    return Decimal(str(value))
