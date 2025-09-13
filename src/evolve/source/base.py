import abc
import os
from pathlib import Path
from typing import (
    Any,
    Mapping,
    Tuple,
)

from pyarrow import fs

from ..exceptions import UnknownUriSchemeError
from ..ir import IR


def _try_get_file_system_from_uri(
    uri: str | Path,
    **fs_options: Mapping[str, Any],
) -> Tuple[fs.FileSystem, str]:
    """
    Try and figure out which file system to set up based on the provided
    file uri.

    Parameters
    ----------
    uri : str | Path
        The uniform resource identifier to the file/object.

    Returns
    -------
    Tuple[fs.FileSystem, str]
        The identified file system and the path to the file/object on the file
        system.

    """
    if (
        isinstance(uri, str)
        and "://" not in uri
        and not (uri.startswith("/") or uri.startswith("./"))
    ):
        # local RELATIVE path - so make it a path and it will be resolved below
        uri = Path(uri)

    if isinstance(uri, Path):
        uri = str(uri.resolve())

    # Most likely a path to a local file if no scheme defined.
    if "://" not in uri:
        uri = "file://" + uri

    if "file:///" in uri:
        file_system = fs.LocalFileSystem()
        file_path = uri.replace("file://", "")

    elif "s3://" in uri or "s3fs://" in uri:
        access_key = fs_options.get("access_key", os.environ.get("AWS_ACCESS_KEY_ID"))
        secret_key = fs_options.get(
            "secret_key", os.environ.get("AWS_SECRET_ACCESS_KEY")
        )
        endpoint_override = fs_options.get(
            "endpoint_override", os.environ.get("AWS_ENDPOINT_URL")
        )
        region = fs_options.get("region", os.environ.get("AWS_REGION"))
        scheme = fs_options.get("scheme", "https")
        allow_bucket_creation = fs_options.get("allow_bucket_creation", False)
        allow_bucket_deletion = fs_options.get("allow_bucket_deletion", False)
        tls_ca_file_path = fs_options.get("tls_ca_file_path")

        file_system = fs.S3FileSystem(
            access_key=access_key,
            secret_key=secret_key,
            endpoint_override=endpoint_override,
            region=region,
            scheme=scheme,
            allow_bucket_creation=allow_bucket_creation,
            allow_bucket_deletion=allow_bucket_deletion,
            tls_ca_file_path=tls_ca_file_path,
        )
        file_path = uri.replace("s3fs://", "").replace("s3://", "")

    elif "gs://" in uri or "gcs://" in uri:
        pass
    elif "hdfs://" in uri:
        host = fs_options.get("host", "default")
        port = fs_options.get("port", "8020")
        user = fs_options.get("user")
        replication = fs_options.get("replication")
        buffer_size = fs_options.get("buffer_size")
        default_block_size = fs_options.get("default_block_size")
        kerb_ticket = fs_options.get("kerb_ticket")
        extra_conf = fs_options.get("extra_conf")

        file_system = fs.HadoopFileSystem(
            host=host,
            port=port,
            user=user,
            replication=replication,
            buffer_size=buffer_size,
            default_block_size=default_block_size,
            kerb_ticket=kerb_ticket,
            extra_conf=extra_conf,
        )
        file_path = uri.replace("hdfs://", "")

    else:
        raise UnknownUriSchemeError(f"unsupported or invalid scheme in URI: '{uri}'")

    return (file_system, file_path)


class BaseSource(abc.ABC):
    """Abstract base class for a source (connector)."""

    def __init__(
        self,
        name: str,
    ) -> None:
        """Initialize base fields for the `BaseSource` object."""
        self._name = name

    @property
    def name(self) -> str:
        """Get the name of the `BaseSource`."""
        return self._name

    @abc.abstractmethod
    def load(self) -> IR:
        """Load data from the defined source to an ir representation."""
        pass

    @abc.abstractmethod
    def validate_config(self) -> None:
        """
        Validate the provided config for the `BaseSource`.

        Raises
        ------
        InvalidConfigError
            If the provided config was not valid.

        """
        pass
