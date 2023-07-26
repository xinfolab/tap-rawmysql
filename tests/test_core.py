import gzip
import json
import os
import shutil
from urllib.parse import urlparse

from tap_rawmysql.tap import TapRawMysql
from tests.configs import streams


def test_integer_type():
    """Run standard tap tests from the SDK."""
    CONFIG = streams.TEST_INTEGER_TYPE_STREAM
    tap = TapRawMysql(CONFIG)

    catalog = tap.catalog.to_dict()

    for stream in catalog["streams"]:
        assert stream["schema"]["properties"]["tinyint_col"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["smallint_col"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["mediumint_col"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["int_col"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["bigint_col"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["tinyint_unsigned_col"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["smallint_unsigned_col"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["mediumint_unsigned_col"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["int_unsigned_col"]["type"] == ["integer", "null"]
        assert stream["schema"]["properties"]["bigint_unsigned_col"]["type"] == ["integer", "null"]


def test_fixed_point_type():
    """Run standard tap tests from the SDK."""
    CONFIG = streams.TEST_FIXED_POINT_TYPE_STREAM
    tap = TapRawMysql(CONFIG)

    catalog = tap.catalog.to_dict()

    for stream in catalog["streams"]:
        assert stream["schema"]["properties"]["decimal_min_scale_col"]["type"] == ["number", "null"]
        assert stream["schema"]["properties"]["decimal_max_scale_col"]["type"] == ["number", "null"]
        assert stream["schema"]["properties"]["float_col"]["type"] == ["number", "null"]
        assert stream["schema"]["properties"]["double_col"]["type"] == ["number", "null"]


def test_date_type():
    """Run standard tap tests from the SDK."""
    CONFIG = streams.TEST_DATE_TYPE_STREAM

    tap = TapRawMysql(CONFIG)

    catalog = tap.catalog.to_dict()

    for stream in catalog["streams"]:
        assert stream["schema"]["properties"]["timestamp_col"]["type"] == ["string", "null"]
        assert stream["schema"]["properties"]["date_col"]["type"] == ["string", "null"]
        assert stream["schema"]["properties"]["datetime_col"]["type"] == ["string", "null"]
        assert stream["schema"]["properties"]["time_col"]["type"] == ["string", "null"]

    for _, stream in tap.streams.items():
        records = list(stream.get_records(context=None))

        assert len(records) == 5
        assert str(records[0]['timestamp_col']) == '2023-07-24 01:12:34'
        assert str(records[0]['date_col']) == '2023-07-24'
        assert str(records[0]['datetime_col']) == '2023-07-24 01:12:34'
        assert str(records[0]['time_col']) == '1:12:34'

        assert str(records[1]['timestamp_col']) == '2023-07-24 01:12:34'
        assert records[1]['date_col'] == None
        assert records[1]['datetime_col'] == None
        assert records[1]['time_col'] == None

        assert records[2]['timestamp_col'] == None
        assert str(records[2]['date_col']) == '2023-07-24'
        assert records[2]['datetime_col'] == None
        assert records[2]['time_col'] == None

        assert records[3]['timestamp_col'] == None
        assert records[3]['date_col'] == None
        assert str(records[3]['datetime_col']) == '2023-07-24 01:12:34'
        assert records[3]['time_col'] == None

        assert records[4]['timestamp_col'] == None
        assert records[4]['date_col'] == None
        assert records[4]['datetime_col'] == None
        assert str(records[4]['time_col']) == '1:12:34'


def test_string_type():
    """Run standard tap tests from the SDK."""
    CONFIG = streams.TEST_STRING_TYPE_STREAM

    tap = TapRawMysql(CONFIG)

    catalog = tap.catalog.to_dict()
    assert len(catalog) == 1

    catalog_stream = catalog["streams"][0]

    assert catalog_stream["schema"]["properties"]["varchar_col"]["type"] == ["string", "null"]
    assert catalog_stream["schema"]["properties"]["char_col"]["type"] == ["string", "null"]
    assert catalog_stream["schema"]["properties"]["text_col"]["type"] == ["string", "null"]

    stream = tap.streams.values()
    assert len(stream) == 1

    for _, stream in tap.streams.items():
        records = list(stream.get_records(context=None))
        assert len(records) == 4

        # check just first row
        assert len(records[0]['char_col']) == 255
        assert len(records[0]['varchar_col']) == 1000
        assert len(records[0]['text_col']) == 65535


def test_etc_type():
    CONFIG = streams.TEST_ETC_TYPE_STREAM

    tap = TapRawMysql(CONFIG)

    catalog = tap.catalog.to_dict()
    assert len(catalog) == 1

    stream = catalog["streams"][0]

    assert stream["schema"]["properties"]["json_col"]["type"] == ["string", "null"]
    assert stream["schema"]["properties"]["bool_col"]["type"] == ["boolean", "null"]

    stream = tap.streams.values()
    assert len(stream) == 1

    for _, stream in tap.streams.items():
        records = list(stream.get_records(context=None))


def test_batch_type():
    """Run standard tap tests from the SDK."""
    CONFIG = streams.TEST_DATE_TYPE_STREAM

    storage_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))
    storage = os.path.join(storage_directory, "test_batch_type")
    os.makedirs(storage, exist_ok=True)

    batch_dict = dict(
        batch_size=10000,
        batch_config={
            "encoding": {"format": "jsonl", "compression": "gzip"},
            "storage": {"root": storage},
        }
    )

    CONFIG.update(batch_dict)
    tap = TapRawMysql(CONFIG)

    for _, stream in tap.streams.items():
        batches = list(
            stream.get_batches(batch_config=stream.get_batch_config(stream.config))
        )

        assert len(batches) == 1

        _, files = batches[0]

        assert len(files) == 1
        p = urlparse(files[0])
        batchfile = os.path.abspath(os.path.join(p.netloc, p.path))

        result_list = list()
        with open(batchfile, "rb") as f:
            with gzip.GzipFile(fileobj=f, mode="rb") as gz:
                for line in gz:
                    result_list.append(json.loads(line))

        assert result_list[0] == {
            'timestamp_col': '2023-07-24T01:12:34+00:00',
            'date_col': '2023-07-24T00:00:00+00:00',
            'datetime_col': '2023-07-24T01:12:34+00:00',
            'time_col': '1970-01-01T01:12:34+00:00'
        }

        assert result_list[1] == {
            'timestamp_col': '2023-07-24T01:12:34+00:00',
            'date_col': None,
            'datetime_col': None,
            'time_col': None
        }

        assert result_list[2] == {
            'timestamp_col': None,
            'date_col': '2023-07-24T00:00:00+00:00',
            'datetime_col': None,
            'time_col': None
        }

        assert result_list[3] == {
            'timestamp_col': None,
            'date_col': None,
            'datetime_col': '2023-07-24T01:12:34+00:00',
            'time_col': None
        }

        assert result_list[4] == {
            'timestamp_col': None,
            'date_col': None,
            'datetime_col': None,
            'time_col': '1970-01-01T01:12:34+00:00'
        }

    shutil.rmtree(storage)
