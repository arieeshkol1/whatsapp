"""Minimal stub of :mod:`packaging.licenses` for Poetry compatibility."""

from __future__ import annotations

from typing import Union

__all__ = [
    "InvalidLicenseExpression",
    "NormalizedLicenseExpression",
    "canonicalize_license_expression",
]


class InvalidLicenseExpression(ValueError):
    """Raised when a license expression cannot be parsed."""


NormalizedLicenseExpression = str


def canonicalize_license_expression(
    expression: Union[str, bytes]
) -> NormalizedLicenseExpression:
    """Return a normalised license expression string.

    Poetry only requires that we reject empty inputs and provide a stable
    string representation, so we trim whitespace and collapse internal
    whitespace to single spaces.
    """

    if isinstance(expression, bytes):
        expression = expression.decode("utf-8", "ignore")

    if not isinstance(expression, str):
        raise InvalidLicenseExpression("License expression must be a string")

    normalized = " ".join(expression.split()).strip()
    if not normalized:
        raise InvalidLicenseExpression("License expression cannot be empty")

    return normalized
