from __future__ import annotations

import abc
from pathlib import Path
from typing import (
    Any,
    Dict,
    Optional,
)

import duckdb
import pyarrow
import pyarrow.parquet as pq
from pyarrow import csv
from pyarrow import fs
from pyarrow import json


class Source(abc.ABC):
    """
    Base class of a Source frontend.

    What should a `Source` do?

    Specify where data comes from AND in what format.
    Should it also be responsible for fetching said data? Yea?
    But what if target makes it so data does not have to be fetched -
    or something?

    """

    def __init__(self, name: str) -> None:
        """Initialize the `Source`."""
        self._name = name

    @property
    def name(self) -> str:
        """Get the name of the Source."""
        return self._name

    @abc.abstractmethod
    def validate_config(self) -> None:
        """Validate the provided config, if invalid raise an `Exception`."""
        pass

    @abc.abstractmethod
    def load(self) -> pyarrow.Table:
        """Load data from the source to the internal representation (Arrow)."""
        pass


class PostgresSource(Source):
    """Implementation of a PostgreSQL source."""

    def __init__(
        self,
        host: str,
        db: str,
        schema: str,
        table: str,
        user: str,
        password: str,
    ) -> None:
        """Initialize a new PostgresSource."""
        super(PostgresSource, self).__init__(name=self.__class__.__name__)

        con = duckdb.connect()
        con.execute("INSTALL postgres_scanner;")
        con.execute("LOAD postgres_scanner;")

        self._con = con
        self._host = host
        self._db = db
        self._schema = schema
        self._table = table
        self._user = user
        self._password = password

    def load(self) -> pyarrow.Table:
        """Load the PostgreSQL table to a arrow.Table."""
        return self._con.execute(f"""  # SQL injection :)
            SELECT * FROM postgres_scan(
                'host={self._host} dbname={self._db}',
                'user={self._user} password={self._password}',
                '{self._schema}',
                '{self._table}'
            );
        """).fetch_arrow_table()

    def validate_config(self) -> None:
        """Validate the PostgreSQL config."""
        pass


class ParquetSource(Source):
    """Implementation of a CSV file source."""

    def __init__(
        self,
        uri: str | Path,
        **options: Dict[str, Any],
    ) -> None:
        """Initialize a new ParquetSource."""
        super(ParquetSource, self).__init__(name=self.__class__.__name__)

        if isinstance(uri, str) and "://" not in uri:
            # Most likely a relative local path
            uri = Path(uri).resolve()

        if isinstance(uri, Path):
            uri = "file:///" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._uri = uri
        self._file_system = file_system
        self._path = path
        self._options = options

    def load(self) -> pyarrow.Table:
        """Load the parquet to an Arrow table."""
        with self._file_system.open_input_file(self._path) as f:
            return pq.read_table(
                source=f,
                **self._options,
            )

    def validate_config(self) -> None:
        """Validate the csv options."""
        pass


class CsvSource(Source):
    """Implementation of a CSV file source."""

    def __init__(
        self,
        uri: str | Path,
        *,
        read_options: Optional[csv.ReadOptions] = None,
        parse_options: Optional[csv.ParseOptions] = None,
        convert_options: Optional[csv.ConvertOptions] = None,
    ) -> None:
        """Initialize a new CsvSource."""
        super(CsvSource, self).__init__(name=self.__class__.__name__)

        if isinstance(uri, str) and "://" not in uri:
            # Most likely a relative local path
            uri = Path(uri).resolve()

        if isinstance(uri, Path):
            uri = "file:///" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._uri = uri
        self._file_system = file_system
        self._path = path
        self._read_options = read_options
        self._parse_options = parse_options
        self._convert_options = convert_options

    def load(self) -> pyarrow.Table:
        """Load the csv to an Arrow table."""
        with self._file_system.open_input_file(self._path) as f:
            return csv.read_csv(
                input_file=f,
                read_options=self._read_options,
                parse_options=self._parse_options,
                convert_options=self._convert_options,
            )

    def validate_config(self) -> None:
        """Validate the csv options."""
        pass


class JsonSource(Source):
    """Implementation of a JSON file source."""

    def __init__(
        self,
        uri: str | Path,
        *,
        read_options: Optional[json.ReadOptions] = None,
        parse_options: Optional[json.ParseOptions] = None,
    ) -> None:
        """Initialize a JsonSource."""
        super(JsonSource, self).__init__(name=self.__class__.__name__)
        if isinstance(uri, str) and "://" not in uri:
            # Most likely a relative local path
            uri = Path(uri).resolve()

        if isinstance(uri, Path):
            uri = "file:///" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._uri = uri
        self._file_system = file_system
        self._path = path
        self._read_options = read_options
        self._parse_options = parse_options

    def load(self) -> pyarrow.Table:
        """
        Read the json file to an Arrow table.

        Automatic Type Inference
        ------------------------
        Arrow data types are inferred from the JSON types and values of each
        column:
        - JSON null values are converted to the `null` type, but can fall back
        to any other type,
        - JSON booleans convert to `bool_`,
        - JSON numbers convert to `int64`, failling back to `float64` if a
          non-integer is encountered,
        - JSON strings of the kind "YYYY-MM-DD hh:mm:ss" convert to
          `timestamp(s)`, falling back to `utf8` if a conversion occurs,
        - JSON arrays convert to a `list` type, and inference proceeds
          recursively on the JSON arrays' values,
        - Nested JSON objects convert to a `struct` type, and inference
          proceeds recursively on the JSON object's values.

        """
        with self._file_system.open_input_file(self._path) as f:
            return json.read_json(
                input_file=f,
                read_options=self._read_options,
                parse_options=self._parse_options,
            )

    def validate_config(self) -> None:
        """Validate the arrow json read and parse options."""
        pass


class FileType:
    """A filetype."""

    _filetypes = ("json", "csv", "parquet")

    @classmethod
    def try_from_suffix(cls, suffix: str) -> FileType:
        """Try and create a `FileType` from a file suffix."""
        if suffix not in cls._filetypes:
            raise ValueError(f"unknown file type suffix: {suffix}")

        return cls(_type=suffix)

    def __init__(self, _type: str) -> None:
        """Initialize a FileType."""
        self._type = _type


class LocalFile(Source):
    """Implementation of a local data source.

    But this is quite arbitrary right?
    A JSON file could exist locally, on S3, somewhere else, so what should the
    source say?

    Should it be more of a LocalFile?

    source = LocalFile("my_file.json")

    Yea, I think `Source` should more be about defining WHERE the data is,
    and the format is the config of the source. Does that make sense?

    So - for a LocalFile(Source)

    source = LocalFile("my_file.csv")

    ```python
    pipeline = Pipeline() \
        .with_source(LocalFile("my_file.csv")) \
        .with_target(LocalFile("my_file.parquet"))

    pipeline.run()
    ```

    so what does Pipeline.run() do?

    # Something like this?
    def run() -> None:
        # but how do we know when we need to load or not?
        # that depends on the source and target?
        # maybe for now - always load it to arrow
        inner = self._source.load()
        self._target.from(inner)

    """

    def __init__(
        self, path: str | Path, file_type: Optional[FileType] = None
    ) -> None:
        """Initialize a LocalFile source."""
        super(LocalFile, self).__init__(name=self.__class__.__name__)

        if isinstance(path, str):
            path = Path(path)

        if file_type is None:
            # have to determine filetype from path
            print(
                "no `file_type` specified, will try "
                "and determine from path name..."
            )
            suffix = path.suffix
            file_type = FileType.try_from_suffix(suffix)

        self._path = path
        self._file_type = file_type
