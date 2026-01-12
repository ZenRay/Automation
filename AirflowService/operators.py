"""Operators shim for AirflowService

This module re-exports operator classes so DAGs can import from
`AirflowService.operators` while the implementation remains in
`dispatcher.operators`.
"""
from dispatcher.operators import (
    MaxcomputeOperator,
    LarkOperator,
    Maxcompute2LarkOperator,
)

__all__ = ["MaxcomputeOperator", "LarkOperator", "Maxcompute2LarkOperator"]
