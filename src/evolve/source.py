from __future__ import annotations

import abc
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Optional,
    Tuple,
)

import duckdb
import polars as pl
import pyarrow.parquet as pq
from pyarrow import (
    csv,
    fs,
    json,
)

from .ir import LazyIR


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
    def load(self) -> LazyIR:
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

    def load(self) -> LazyIR:
        """Load the PostgreSQL table to a arrow.Table."""
        return LazyIR.from_arrow_table(
            self._con.execute(f"""  # SQL injection :)
            SELECT * FROM postgres_scan(
                'host={self._host} dbname={self._db}',
                'user={self._user} password={self._password}',
                '{self._schema}',
                '{self._table}'
            );
        """).fetch_arrow_table()
        )

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
            uri = "file://" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._uri = uri
        self._file_system = file_system
        self._path = path
        self._options = options

    def load(self) -> LazyIR:
        """Load the parquet to an Arrow table."""
        with self._file_system.open_input_file(self._path) as f:
            return LazyIR.from_arrow_table(
                pq.read_table(
                    source=f,
                    **self._options,
                )
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
            uri = "file://" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._uri = uri
        self._file_system = file_system
        self._path = path
        self._read_options = read_options
        self._parse_options = parse_options
        self._convert_options = convert_options

    def load(self) -> LazyIR:
        """Load the csv to an Arrow table."""
        with self._file_system.open_input_file(self._path) as f:
            return LazyIR.from_arrow_table(
                csv.read_csv(
                    input_file=f,
                    read_options=self._read_options,
                    parse_options=self._parse_options,
                    convert_options=self._convert_options,
                )
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
            uri = "file://" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._uri = uri
        self._file_system = file_system
        self._path = path
        self._read_options = read_options
        self._parse_options = parse_options

    def load(self) -> LazyIR:
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
            return LazyIR.from_arrow_table(
                json.read_json(
                    input_file=f,
                    read_options=self._read_options,
                    parse_options=self._parse_options,
                )
            )

    def validate_config(self) -> None:
        """Validate the arrow json read and parse options."""
        pass


class MultiFixedWidthSource(Source):
    """Implementation of a Multi Fixed Width File (fwf) source."""

    def __init__(
        self,
        uri: str | Path,
        schema_map: Dict[
            str, Dict[str, Iterable[str] | Iterable[Tuple[int, int]]]
        ],
        schema_spec_len: int,
        schema_spec_offset: int = 0,
        encoding: str = "utf8",
    ) -> None:
        """Initialize a new multi fixed width file (fwf) source."""
        super(MultiFixedWidthSource, self).__init__(
            name=self.__class__.__name__
        )

        if isinstance(uri, str) and "://" not in uri:
            # Most likely a relative local path
            uri = Path(uri).resolve()

        if isinstance(uri, Path):
            uri = "file://" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._file_system = file_system
        self._path = path
        self._schema_spec_offset = schema_spec_offset
        self._schema_spec_len = schema_spec_len
        self._schema_map = schema_map
        self._encoding = encoding

    def load(self) -> LazyIR:
        with self._file_system.open_input_file(self._path) as f:
            df = pl.read_csv(
                source=f,
                has_header=False,
                skip_rows=0,
                new_columns=["full_str"],
            )

        # Extract schema identifer from each row
        df = df.with_columns(
            [
                pl.col("full_str")
                .str.slice(self._schema_spec_offset, self._schema_spec_len)
                .alias("_schema_id")
            ]
        )

        dfs = []

        skip_n_chars = self._schema_spec_len + self._schema_spec_offset

        for schema_id, schema_def in self._schema_map.items():
            colspecs = schema_def["colspecs"]
            colnames = schema_def["colnames"]

            df_schema_spec = df.filter(pl.col("_schema_id") == schema_id)
            df_schema_spec = df_schema_spec.with_columns(
                [
                    pl.col("full_str")
                    .str.slice(start + skip_n_chars, width)
                    .str.strip_chars()
                    .alias(col)
                    for (start, width), col in zip(
                        colspecs, colnames, strict=True
                    )
                ]
            ).drop(["full_str", "_schema_id"])
            dfs.append(df_schema_spec)

        return LazyIR(ir=dfs)

    def validate_config(self) -> None:
        pass


class FixedWidthSource(Source):
    """Implementation of a fixed width file (fwf) source."""

    def __init__(
        self,
        uri: str | Path,
        colspecs: Iterable[Tuple[int, int]],
        colnames: Iterable[str],
        encoding: str = "utf8",
    ) -> None:
        """Initialize a new FixedWidthSource."""
        super(FixedWidthSource, self).__init__(name=self.__class__.__name__)

        if isinstance(uri, str) and "://" not in uri:
            # Most likely a relative local path
            uri = Path(uri).resolve()

        if isinstance(uri, Path):
            uri = "file://" + str(uri)

        file_system, path = fs.FileSystem.from_uri(uri)

        self._file_system = file_system
        self._path = path
        self._colspecs = colspecs
        self._colnames = colnames
        self._encoding = encoding

    def load(self) -> LazyIR:
        """Load the fixed width file to LazyIR."""
        with self._file_system.open_input_file(self._path) as f:
            df = pl.read_csv(
                source=f,
                has_header=False,
                skip_rows=0,
                new_columns=["full_str"],
            )

        # parse it to real form
        df = df.with_columns(
            [
                pl.col("full_str")
                .str.slice(st[0], st[1])
                .str.strip_chars()
                .alias(col)
                for st, col in zip(self._colspecs, self._colnames, strict=True)
            ]
        ).drop("full_str")

        return LazyIR(ir=df)

    def validate_config(self) -> None:
        """Validate the fwf reading config."""
        pass
