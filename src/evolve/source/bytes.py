from pathlib import Path
from typing import Mapping
from .._utils import _try_get_file_system_from_uri
from ..ir import IR, BytesBackend, get_global_backend
from .base import BaseSource


class BytesSource(BaseSource):
    """Implementation of a bytes source."""

    def __init__(
        self,
        uri: str | Path,
        *,
        backend: BytesBackend | None = None,
        **options: Mapping[str, str],
    ) -> None:
        """Initialize the byte source object."""
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        file_system, file_path = _try_get_file_system_from_uri(uri, **options)

        self._file_system = file_system
        self._file_path = file_path

    def load(self) -> IR:
        with self._file_system.open_input_file(self._file_path) as f:
            return self._backend.ir_from_bytes(f.read())
