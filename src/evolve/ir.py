from __future__ import annotations

import polars as pl
import pyarrow


class LazyIR:
    """Implementation of a Lazy Internal Representation (IR)."""

    @classmethod
    def from_arrow_table(cls, table: pyarrow.Table) -> LazyIR:
        """Initialize a new LazyIR."""
        return cls(ir=pl.from_arrow(table))

    def __init__(self, ir: pl.DataFrame) -> None:
        """Initialize a new Lazy IR."""
        self._ir = ir

    def get_ir(self) -> pl.DataFrame:
        """Get the IR."""
        return self._ir

    def set_ir(self, ir: pl.DataFrame) -> None:
        """Update the IR."""
        self._ir = ir
