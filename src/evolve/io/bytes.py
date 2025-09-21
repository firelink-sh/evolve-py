from pathlib import Path

from .._utils import _try_get_file_system_from_uri
from ..ir import BytesBackend, IR, get_global_backend, BaseBackend
from ._base import BaseIO


class BytesIO(BaseIO):
    """Implementation of bytes I/O."""

    def __init__(
        self,
        uri: str | Path,
        *,
        backend: BaseBackend | None = BytesBackend(),
        **options,
    ) -> None:
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        file_system, file_path = _try_get_file_system_from_uri(uri, **options)

        self._file_system = file_system
        self._file_path = file_path

    def read(self) -> IR:
        """Read the bytes from the source path."""
        with self._file_system.open_input_file(self._file_path) as source:
            return self._backend.ir_from_bytes(source.read())

    def write(self, data: IR) -> None:
        """Write the data as bytes to the target path."""
        with self._file_system.open_output_stream(self._file_path) as sink:
            sink.write(self._backend.bytes_from_ir(data))
