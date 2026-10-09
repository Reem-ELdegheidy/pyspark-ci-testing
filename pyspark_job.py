
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def clean_data(df: DataFrame) -> DataFrame:
    return (
        df.filter((col("amount") > 0) & (col("name").isNotNull()))
          .withColumn("amount_with_tax", col("amount") * 1.20)
    )