import os
from pathlib import Path
from typing import (
    Mapping,
    Tuple,
)

import pyarrow.parquet as pq
from pyarrow import fs

from .base import BaseSource


class ParquetSource(BaseSource):
    """Implementation of a parquet file source."""

    @staticmethod
    def _setup_file_system_from_uri(
        uri: str | Path,
        **options: Mapping[str, str],
    ) -> Tuple[fs.FileSystem, str]:
        """Try figure out file system based on the uri."""
        if isinstance(uri, Path):
            uri = str(uri.resolve())

        # It was most likely a path to a local file.
        if "://" not in uri:
            prefix = "file://"
            if uri.startswith("/"):
                prefix = "file:/"
            uri = prefix + uri

        if "file://" in uri:
            file_system = fs.LocalFileSystem()
            # If local file then remove 'file:/' to make absolute local path.
            file_path = uri.replace("file:/", "")
        elif "s3://" in uri:
            file_system = fs.S3FileSystem(
                access_key=options.get(
                    "access_key", os.environ.get("AWS_ACCESS_KEY_ID")
                ),
                secret_key=options.get(
                    "secret_key", os.environ.get("AWS_SECRET_ACCESS_KEY")
                ),
                endpoint_override=options.get("endpoint_override"),
                region=options.get("region", os.environ.get("AWS_REGION")),
                scheme=options.get("scheme", "https"),
                allow_bucket_creation=options.get(
                    "allow_bucket_creation", False
                ),
                allow_bucket_deletion=options.get(
                    "allow_bucket_deletion", False
                ),
                tls_ca_file_path=options.get("tls_ca_file_path"),
            )
            file_path = uri.replace("s3://", "")
        else:
            raise ValueError(
                f"could not figure out the file system from uri={uri}"
            )

        return (file_system, file_path)

    def __init__(self, uri: str | Path, **options: Mapping[str, str]) -> None:
        """Initialize a new `ParquetSource`."""
        super().__init__(name=self.__class__.__name__)

        file_system, file_path = self._setup_file_system_from_uri(
            uri,
            **options,
        )

        self._file_system = file_system
        self._file_path = file_path

    def load(self) -> None:
        """Load the parquet file from the source to IR."""
        with self._file_system.open_input_file(self._file_path) as f:
            return pq.read_table(source=f)

    def validate_config(self) -> None:
        """Validate the provided parquet file reading options."""
        pass
