import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    LongType,
    MapType,
)
from dbxconfig import _utils as utils


@pytest.fixture
def spark_schema():
    spark_schema = StructType(
        [
            StructField("firstname", StringType(), True),
            StructField("middlename", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("id", LongType(), True),
            StructField("gender", StringType(), True),
            StructField("salary", DecimalType(4, 2), True),
            StructField("age", IntegerType(), True),
        ]
    )

    return spark_schema


@pytest.fixture
def replacements():
    replacements = {
        utils.JinjaVariables.DATABASE: "test_database",
        utils.JinjaVariables.TABLE: "test_table",
        utils.JinjaVariables.CHECKPOINT: "test_checkpoint",
        utils.JinjaVariables.FILENAME_DATE_FORMAT: "test_filename_date_format",
        utils.JinjaVariables.PATH_DATE_FORMAT: "test_path_date_format",
        utils.JinjaVariables.CONTAINER: "test_container",
    }
    return replacements


def test_utils_get_dll_header(spark_schema):
    actual = utils.get_ddl(spark_schema=spark_schema, header=True)
    expected = [
        "firstname string",
        "middlename string",
        "lastname string",
        "id bigint",
        "gender string",
        "salary decimal(4,2)",
        "age int",
    ]

    assert actual == expected


def test_utils_get_dll_(spark_schema):
    actual = utils.get_ddl(spark_schema=spark_schema, header=False)
    expected = [
        "_c0 string",
        "_c1 string",
        "_c2 string",
        "_c3 bigint",
        "_c4 string",
        "_c5 decimal(4,2)",
        "_c6 int",
    ]

    assert actual == expected


def test_render_jinja(replacements):
    data = """
        {{database}}
        {{table}}
        {{checkpoint}}
        {{filename_date_format}}
        {{path_date_format}}
        {{container}}
    """

    actual = utils.render_jinja(data, replacements)
    expected = """
        test_database
        test_table
        test_checkpoint
        test_filename_date_format
        test_path_date_format
        test_container
    """

    assert actual == expected
