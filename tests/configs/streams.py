import json
from enum import Enum
from os import path

config_directory = path.abspath(path.join(path.dirname(__file__), "."))
config_path = path.join(config_directory, "config.json")

with open(config_path, "r") as f:
    config: dict = json.load(f)

TEST_INTEGER_TYPE_STREAM = dict(
    **config,
    streams=[
        {
            "name": "test_integer_type",
            "sql": "select "
                   "tinyint_col, "
                   "smallint_col, "
                   "mediumint_col, "
                   "int_col, "
                   "bigint_col, "
                   "tinyint_unsigned_col, "
                   "smallint_unsigned_col, "
                   "mediumint_unsigned_col, "
                   "int_unsigned_col, "
                   "bigint_unsigned_col "
                   "FROM test_integer_type "
                   "ORDER BY num",
            "key_properties": ["num"],
            "columns": [
                {
                    "name": "tinyint_col",
                    "type": "int"
                },
                {
                    "name": "smallint_col",
                    "type": "int"
                },
                {
                    "name": "mediumint_col",
                    "type": "int"
                },
                {
                    "name": "int_col",
                    "type": "int"
                },
                {
                    "name": "bigint_col",
                    "type": "int"
                },
                {
                    "name": "tinyint_unsigned_col",
                    "type": "int"
                },
                {
                    "name": "smallint_unsigned_col",
                    "type": "int"
                },
                {
                    "name": "mediumint_unsigned_col",
                    "type": "int"
                },
                {
                    "name": "int_unsigned_col",
                    "type": "int"
                },
                {
                    "name": "bigint_unsigned_col",
                    "type": "int"
                },
            ]
        }
    ]
)

TEST_FIXED_POINT_TYPE_STREAM = dict(
    **config,
    streams=[
        {
            "name": "test_fixed_point_type",
            "sql": "SELECT "
                   "decimal_min_scale_col, "
                   "decimal_max_scale_col, "
                   "float_col, "
                   "double_col "
                   "FROM test_fixed_point_type "
                   "ORDER BY num",
            "key_properties": ["num"],
            "columns": [
                {
                    "name": "decimal_min_scale_col",
                    "type": "decimal"
                },
                {
                    "name": "decimal_max_scale_col",
                    "type": "decimal"
                },
                {
                    "name": "float_col",
                    "type": "float"
                },
                {
                    "name": "double_col",
                    "type": "double"
                }
            ]
        }
    ]
)

TEST_DATE_TYPE_STREAM = dict(
    **config,

    streams=[
        {
            "name": "test_date_type",
            "sql": "SELECT timestamp_col, "
                   "date_col, "
                   "datetime_col, "
                   "time_col "
                   "FROM test_date_type "
                   "ORDER BY num",
            "key_properties": ["num"],
            "columns": [
                {
                    "name": "timestamp_col",
                    "type": "timestamp"
                },
                {
                    "name": "date_col",
                    "type": "date"
                },
                {
                    "name": "datetime_col",
                    "type": "datetime"
                },
                {
                    "name": "time_col",
                    "type": "time"
                },
            ]
        }
    ]
)

TEST_STRING_TYPE_STREAM = dict(
    **config,
    streams=[
        {
            "name": "test_string_type",
            "sql": "SELECT varchar_col, "
                   "char_col, "
                   "text_col "
                   "FROM test_string_type "
                   "ORDER BY num",
            "key_properties": ["num"],
            "columns": [
                {
                    "name": "varchar_col",
                    "type": "string"
                },
                {
                    "name": "char_col",
                    "type": "char"
                },
                {
                    "name": "text_col",
                    "type": "text"
                }
            ]
        }
    ]
)

TEST_ETC_TYPE_STREAM = dict(
    **config,
    streams=[
        {
            "name": "test_etc_type",
            "sql": "SELECT json_col, "
                   "bool_col "
                   "FROM test_etc_type "
                   "ORDER BY num",
            "key_properties": ["num"],
            "columns": [
                {
                    "name": "json_col",
                    "type": "111"
                },
                {
                    "name": "bool_col",
                    "type": "bool"
                }
            ]
        }
    ]
)

t2 = dict(
    **config,
    streams=[
        {
            "name": "test_integer_type",
            "sql": "",
            "key_properties": ["num"],
            "columns": [
                {
                    "name": "",
                    "type": ""
                }
            ]
        }
    ]
)

t3 = dict(
    **config,
    streams=[
        {
            "name": "test_integer_type",
            "sql": "",
            "key_properties": ["num"],
            "columns": [
                {
                    "name": "",
                    "type": ""
                }
            ]
        }
    ]
)

t4 = dict(
    **config,
    streams=[
        {
            "name": "test_integer_type",
            "sql": "",
            "key_properties": ["num"],
            "columns": [
                {
                    "name": "",
                    "type": ""
                }
            ]
        }
    ]
)

t5 = dict(
    **config,
    streams=[
        {
            "name": "test_integer_type",
            "sql": "",
            "key_properties": ["num"],
            "columns": [
                {
                    "name": "",
                    "type": ""
                }
            ]
        }
    ]
)