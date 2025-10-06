from pathlib import Path

import pyarrow.dataset as ds

from .._utils import _try_get_file_system_from_uri
from ..ir import BaseBackend, get_global_backend, IR

from ._base import BaseIO


class ArrowDataset(BaseIO):
    def __init__(
        self,
        uri: str | Path,
        *,
        backend: BaseBackend | None = None,
        **options,
    ) -> None:
        super().__init__(
            name=self.__class__.__name__,
            backend=backend or get_global_backend(),
        )

        file_system, base_dir = _try_get_file_system_from_uri(uri, **options)

        self._file_system = file_system
        self._base_dir = base_dir

        self._format = options.get("format", "parquet")
        self._partitioning = options.get("partitioning")
        self._existing_data_behaviour = options.get("existing_data_behaviour", "error")

    def read(self) -> IR:
        pass

    def write(self, data: IR) -> None:
        ds.write_dataset(
            data=self._backend.ir_to_arrow_table(data),
            base_dir=self._base_dir,
            format=self._format,
            partitioning=self._partitioning,
            existing_data_behavior=self._existing_data_behaviour,
            filesystem=self._file_system,
        )
