from __future__ import annotations

from typing import TypedDict


class DerivationTransformSpec(TypedDict):
    month_stride: int
    distributive: bool


DERIVATION_TRANSFORM_REGISTRY: dict[str, DerivationTransformSpec] = {
    "shift_year": {"month_stride": 12, "distributive": True},
    "shift_month": {"month_stride": 1, "distributive": True},
    "shift_quarter": {"month_stride": 3, "distributive": True},
}


def get_derivation_transform_spec(kind: str) -> DerivationTransformSpec | None:
    return DERIVATION_TRANSFORM_REGISTRY.get(kind)

