
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
)

from pyspark_job import clean_data


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pyspark-ci-tests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
])


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema=schema)


def test_valid_records_are_kept(spark):
    df = make_df(spark, [
        (1, "Alice", 100.0),
    ])

    result = clean_data(df).collect()

    assert len(result) == 1
    assert result[0]["id"] == 1
    assert result[0]["name"] == "Alice"
    assert result[0]["amount"] == 100.0


def test_amount_less_than_or_equal_zero_removed(spark):
    df = make_df(spark, [
        (1, "Zero", 0.0),
        (2, "Negative", -10.0),
        (3, "Positive", 10.0),
    ])

    result = clean_data(df).collect()
    ids = {row["id"] for row in result}

    assert ids == {3}


def test_null_name_removed(spark):
    df = make_df(spark, [
        (1, None, 10.0),
        (2, "Valid", 10.0),
    ])

    result = clean_data(df).collect()
    ids = {row["id"] for row in result}

    assert ids == {2}


def test_amount_with_tax_calculated_correctly(spark):
    df = make_df(spark, [
        (1, "Alice", 100.0),
        (2, "Bob", 20.0),
    ])

    result = {row["id"]: row for row in clean_data(df).collect()}

    assert result[1]["amount_with_tax"] == pytest.approx(120.0)
    assert result[2]["amount_with_tax"] == pytest.approx(24.0)