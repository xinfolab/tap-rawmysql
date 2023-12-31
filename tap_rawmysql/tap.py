"""rawmysql tap class."""

from __future__ import annotations

from pathlib import PurePath
from typing import Dict, Optional, List, Tuple, Any, Union

import sqlalchemy
from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from singer_sdk._singerlib import CatalogEntry, MetadataMapping, Schema
from singer_sdk.streams.sql import SQLConnector

from tap_rawmysql.client import RawMysqlConnector, RawMysqlStream


class TapRawMysql(Tap):
    """TapRawMysql tap class."""

    name = "tap-rawmysql"

    _tap_connection: Optional[sqlalchemy.engine.Connection] = None

    default_stream_class = RawMysqlConnector

    config_jsonschema = th.PropertiesList(
        th.Property(
            "user",
            th.StringType,
            required=True,
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
        ),
        th.Property(
            "host",
            th.StringType,
            required=True,
        ),
        th.Property(
            "port",
            th.IntegerType,
            required=True,
        ),
        th.Property(
            "database",
            th.StringType,
            required=True,
        ),
        th.Property(
            "streams",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        required=True,
                    ),
                    th.Property(
                        "key_properties",
                        th.ArrayType(th.StringType),
                        required=True,
                    ),
                    th.Property(
                        "replication_key",
                        th.StringType,
                        required=False,
                        default=None,
                    ),
                    th.Property(
                        "sql",
                        th.StringType,
                        required=True,
                    ),
                    th.Property(
                        "columns",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property(
                                    "name",
                                    th.StringType,
                                    required=True,
                                ),
                                th.Property(
                                    "type",
                                    th.StringType,
                                    required=True,
                                ),
                                th.Property(
                                    "nullable",
                                    th.BooleanType,
                                    required=False,
                                    default=True,
                                ),
                            ),
                        ),
                        required=True,
                    ),
                ),
            ),
            required=True,
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            required=False,
            default=100_000,
            description="Size of batch files",
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property(
                    "encoding",
                    th.ObjectType(
                        th.Property("format", th.StringType, required=True),
                        th.Property("compression", th.StringType, required=True),
                    ),
                    required=True,
                ),
                th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property("root", th.StringType, required=True),
                        th.Property(
                            "prefix", th.StringType, required=False, default=""
                        ),
                    ),
                    required=True,
                ),
            ),
            required=False,
        ),
    ).to_dict()

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        catalog: Union[PurePath, str, dict, None] = None,
        state: Union[PurePath, str, dict, None] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        connection: Optional[sqlalchemy.engine.Connection] = None,
    ) -> None:
        super().__init__(config=config,
                         catalog=catalog,
                         state=state,
                         parse_env_config=parse_env_config,
                         validate_config=validate_config)
        self._own_connection = connection

    def parse_raw_sql_stream(self, stream_config: dict) -> Dict[str, Any]:
        unique_stream_id = SQLConnector.get_fully_qualified_name(
            db_name=self.config["database"],
            table_name=stream_config["name"],
            delimiter="-",
        )

        key_properties = stream_config["key_properties"]
        replication_key = stream_config.get("replication_key", None)
        replication_method = "INCREMENTAL" if replication_key else "FULL_TABLE"

        table_schema = th.PropertiesList()
        for column_def in stream_config["columns"]:
            column_name = column_def["name"]
            is_nullable = column_def.get("nullable", True)
            jsonschema_type: dict = self.to_jsonschema_type(column_def["type"])
            table_schema.append(
                th.Property(
                    name=column_name,
                    wrapped=th.CustomType(jsonschema_type),
                    required=not is_nullable
                    or column_name in key_properties
                    or column_name == replication_key,
                )
            )
        schema = table_schema.to_dict()

        # Create the catalog entry object
        catalog_entry = CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=stream_config["name"],
            key_properties=key_properties,
            schema=Schema.from_dict(schema),
            is_view=False,
            replication_method=replication_method,
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=stream_config["name"],
                schema=schema,
                replication_method=replication_method,
                key_properties=key_properties,
                valid_replication_keys=[replication_key] if replication_key else None,
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=replication_key,
        )

        return catalog_entry.to_dict()

    def discover_raw_sql_streams(self) -> List[Tuple[Dict[str, Any], dict]]:
        return [
            (self.parse_raw_sql_stream(stream_config), stream_config)
            for stream_config in self.config["streams"]
        ]

    @property
    def streams(self) -> Dict[str, Stream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        result: Dict[str, Stream] = {}

        connector = (
            RawMysqlConnector(connection=self._tap_connection)
            if self._tap_connection
            else None
        )

        for catalog_entry, stream_config in self.discover_raw_sql_streams():
            s = RawMysqlStream(
                tap=self,
                schema=catalog_entry["schema"],
                name=catalog_entry["tap_stream_id"],
                stream_config=stream_config,
                connector=connector,
            )
            result[s.name] = s

        return result

    @staticmethod
    def to_jsonschema_type(
            from_type: (
                    str
                    | sqlalchemy.types.TypeEngine
                    | type[sqlalchemy.types.TypeEngine]
                    | Any
            ),
    ) -> dict:
        """Return the JSON Schema dict that describes the sql type.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Raises:
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

        Returns:
            A compatible JSON Schema type definition.
        """
        sqltype_lookup: dict[str, dict] = {
            # NOTE: This is an ordered mapping, with earlier mappings taking precedence.
            #       If the SQL-provided type contains the type name on the left, the mapping
            #       will return the respective singer type.
            "timestamp": th.DateTimeType.type_dict,
            "datetime": th.DateTimeType.type_dict,
            "date": th.DateType.type_dict,
            "int": th.IntegerType.type_dict,
            "number": th.NumberType.type_dict,
            "decimal": th.NumberType.type_dict,
            "double": th.NumberType.type_dict,
            "float": th.NumberType.type_dict,
            "string": th.StringType.type_dict,
            "text": th.StringType.type_dict,
            "char": th.StringType.type_dict,
            "bool": th.BooleanType.type_dict,
            "variant": th.StringType.type_dict,
        }

        sqltype_lookup.update(
            # User Add type
            {
                "time": th.TimeType.type_dict,
                "multipleOf": th.NumberType.type_dict, # decimal
                "object": th.StringType.type_dict,
            }
        )

        if isinstance(from_type, str):
            type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(
                from_type,
                sqlalchemy.types.TypeEngine,
        ):
            type_name = from_type.__name__
        else:
            msg = "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
            raise ValueError(msg)

        # Look for the type name within the known SQL type names:
        for sqltype, jsonschema_type in sqltype_lookup.items():
            if sqltype.lower() in type_name.lower():
                return jsonschema_type

        return sqltype_lookup["string"]  # safe failover to str


if __name__ == "__main__":
    TapRawMysql.cli()
