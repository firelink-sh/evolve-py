import abc
from pathlib import Path
from typing import (
    Any,
    Dict,
    Optional,
)

import polars as pl
import pyarrow
from pyarrow import (
    csv,
    fs,
    parquet,
)

from .ir import LazyIR


class TargetBase(abc.ABC):
    """Base class for a target."""

    def __init__(self, name: str) -> None:
        """Initialize the target."""
        self._name = name

    @property
    def name(self) -> str:
        """Get the name of the target."""
        return self._name

    @abc.abstractmethod
    def write(self, data: LazyIR) -> None:
        """Write the data to the target format."""
        pass


class JsonTarget(TargetBase):
    """Implementation of a JSON target."""

    def __init__(
        self,
        uri: str | Path,
    ) -> None:
        """Initialize a jsontarget."""
        super(JsonTarget, self).__init__(name=self.__class__.__name__)

        if isinstance(uri, str) and "://" not in uri:
            # most likely a relative file path
            uri = Path(uri).resolve()

        if isinstance(uri, Path):
            uri = "file://" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._uri = uri
        self._file_system = file_system
        self._path = path

    def write(self, data: LazyIR) -> None:
        """Write the data to json file."""
        df = data.get_ir()
        with self._file_system.open_output_stream(self._path) as f:
            df.write_json(file=f)


class CsvTarget(TargetBase):
    """Implementation of a csv target."""

    def __init__(
        self,
        uri: str | Path,
        *,
        write_options: Optional[csv.WriteOptions] = None,
    ) -> None:
        """Initialize a csv target."""
        super(CsvTarget, self).__init__(name=self.__class__.__name__)

        if isinstance(uri, str) and "://" not in uri:
            # Most likely a relative local path
            uri = Path(uri).resolve()

        if isinstance(uri, Path):
            uri = "file://" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._uri = uri
        self._file_system = file_system
        self._path = path
        self._write_options = write_options

    def write(self, data: LazyIR) -> None:
        """Write the data to csv file."""
        with self._file_system.open_output_stream(self._path) as f:
            csv.write_csv(
                data=data,
                output_file=f,
                write_options=self._write_options,
            )


class ParquetTarget(TargetBase):
    """Implementation of a parquet target."""

    def __init__(
        self,
        uri: str | Path,
        **options: Dict[str, Any],
    ) -> None:
        """Initialize a csv target."""
        super(ParquetTarget, self).__init__(name=self.__class__.__name__)

        if isinstance(uri, str) and "://" not in uri:
            # Most likely a relative local path
            uri = Path(uri).resolve()

        if isinstance(uri, Path):
            uri = "file://" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._uri = uri
        self._file_system = file_system
        self._path = path
        self._options = options

    def write(self, data: LazyIR) -> None:
        """Write the data to parquet file."""
        with self._file_system.open_output_stream(self._path) as f:
            parquet.write_table(
                table=data.get_ir().to_arrow(),
                where=f,
                **self._options,
            )
