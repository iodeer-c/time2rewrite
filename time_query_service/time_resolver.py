from __future__ import annotations

"""
Legacy compatibility shim for modules that still import time_resolver.resolve_plan.

The old resolver implementation has been removed. New code must use
time_query_service.new_resolver directly.
"""

from time_query_service.new_resolver import resolve_plan

__all__ = ["resolve_plan"]
