"""SQL client handling.

This includes RawMysqlStream and RawMysqlConnector.
"""

from __future__ import annotations

import gzip
import json
import logging
from decimal import Decimal
from os import PathLike
from typing import Any, Iterable, Optional, Union, IO
from uuid import UUID, uuid4

import singer_sdk._singerlib as singer
import sqlalchemy

from singer_sdk import SQLConnector, Stream
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
from singer_sdk.helpers._typing import conform_record_data_types, TypeConformanceLevel
from singer_sdk.plugin_base import PluginBase as TapBaseClass


class RawMysqlConnector(SQLConnector):
    """Connects to the rawmysql SQL source."""
    def __init__(
        self,
        config: Optional[dict] = None,
        sqlalchemy_url: Optional[str] = None,
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> None:
        super().__init__(config, sqlalchemy_url)
        self._own_connection = connection

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source.

        Args:
            config: A dict with connection parameters

        Returns:
            SQLAlchemy connection string
        """
        connection_url = sqlalchemy.engine.url.URL.create(
            drivername="mysql",
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
        )

        return connection_url

    @staticmethod
    def to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            jsonschema_type: A dict

        Returns:
            SQLAlchemy type
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_sql_type(jsonschema_type)


def conform_record_data_types_and_uuid(  # noqa: C901
    stream_name: str, record: dict[str, Any], schema: dict, logger: logging.Logger
) -> dict[str, Any]:
    """Translate values in record dictionary to singer-compatible data types.

    Any property names not found in the schema catalog will be removed, and a
    warning will be logged exactly once per unmapped property name.
    """
    rec = conform_record_data_types(
        stream_name=stream_name, record=record, schema=schema, level=TypeConformanceLevel.RECURSIVE, logger=logger
    )

    # Fix any UUIDs as well
    for property_name, elem in record.items():
        if isinstance(elem, UUID):
            rec[property_name] = str(elem)
        elif isinstance(elem, Decimal):
            rec[property_name] = float(elem)

    return rec


class RawMysqlStream(Stream):
    """Stream class for rawmysql streams."""

    connector_class = RawMysqlConnector

    def __init__(
        self,
        tap: TapBaseClass,
        stream_config: dict,
        schema: Union[str, PathLike, dict[str, Any], singer.Schema, None] = None,
        name: Optional[str] = None,
        connector: Optional[SQLConnector] = None,
    ) -> None:
        self.stream_config = stream_config
        self.connector = connector or self.connector_class(dict(tap.config))
        super().__init__(tap, schema=schema, name=name)
        self.primary_keys = stream_config["key_properties"]
        self.replication_key = stream_config.get("replication_key", None)
        self.batch_size = self.config.get("batch_size", 100_000)

    def get_batches(
        self,
        batch_config: BatchConfig,
        context: Optional[dict] = None,
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Batch generator function.

        Developers are encouraged to override this method to customize batching
        behavior for databases, bulk APIs, etc.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest) for each batch.
        """
        sync_id = f"{self.tap_name}--{self.name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""

        i = 1
        chunk_size = 0
        filename: Optional[str] = None
        f: Optional[IO] = None
        gz: Optional[gzip.GzipFile] = None

        with batch_config.storage.fs() as fs:
            for record in self._sync_records(context, write_messages=False):
                if filename is None or f is None or gz is None:
                    filename = f"{prefix}{sync_id}-{i}.json.gz"
                    f = fs.open(filename, "wb")
                    gz = gzip.GzipFile(fileobj=f, mode="wb")

                record = conform_record_data_types_and_uuid(
                    stream_name=self.name,
                    record=record,
                    schema=self.schema,
                    logger=self.logger,
                )

                gz.write((json.dumps(record) + "\n").encode())
                chunk_size += 1

                if chunk_size >= self.batch_size:
                    gz.close()
                    gz = None
                    f.close()
                    f = None
                    file_url = fs.geturl(filename)
                    yield batch_config.encoding, [file_url]

                    filename = None

                    i += 1
                    chunk_size = 0

            if chunk_size > 0:
                if gz is None:
                    raise ValueError("'gz' was None but shouldn't have been")
                if f is None:
                    raise ValueError("'f' was None but shouldn't have been")
                if filename is None:
                    raise ValueError("'filename' was None but shouldn't have been")
                gz.close()
                f.close()
                file_url = fs.geturl(filename)
                yield batch_config.encoding, [file_url]

    def get_records(self, context: Optional[dict]) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Yield one dict per record.
        """
        sql: str = self.stream_config["sql"]

        if not sql:
            raise Exception("sql is empty: ")

        is_incremental = self.replication_method == "INCREMENTAL"

        rep_key = self.stream_config.get("replication_key", context)

        multiparams: list[dict] = []

        if is_incremental:
            if not rep_key:
                raise Exception(
                    "INCREMENTAL sync not possible with a specified 'replication_key'"
                )

            rep_key_val = self.get_starting_replication_key_value(context)
            if rep_key_val is None:
                rep_key_val = self.stream_config.get(
                    "replication_key_value_start", None
                )

            if rep_key_val is None:
                raise Exception(
                    "No value for replication key. INCREMENTAL sync not possible."
                )

            multiparams = [{"rep_key_val": rep_key_val}]
        else:
            rep_key_val = self.stream_config.get("replication_key_value_start", None)

            if rep_key:
                rep_key_val = self.stream_config.get(
                    "replication_key_value_start", None
                )
                if rep_key_val is None:
                    raise Exception(
                        "'replication_key' is specified but no "
                        "'replication_key_value_start'."
                        " FULL_TABLE sync not possible."
                    )

                multiparams = [{"rep_key_val": rep_key_val}]

        for record in self.connector.connection.execute(
            sqlalchemy.text(sql),
            *multiparams,
        ).mappings():
            yield dict(record)
