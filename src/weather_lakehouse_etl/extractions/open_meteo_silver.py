from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_TABLE_NAME = "workspace.default.weather_data_bronze"
SILVER_TABLE_NAME = "workspace.default.weather_data_silver"


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def main():
    spark = get_spark_session()

    df_bronze = spark.read.table(BRONZE_TABLE_NAME)

    window_spec = Window.partitionBy("city", "timestamp").orderBy(
        F.col("timestamp").desc()
    )

    df_silver = (
        df_bronze.filter(
            F.col("city").isNotNull()
            & (F.trim(F.col("city")) != "")
            & F.col("timestamp").isNotNull()
        )
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("measurement_date", F.to_date(F.col("timestamp")))
        .withColumn(
            "temperature",
            F.when(
                (F.col("temperature") >= F.lit(-90))
                & (F.col("temperature") <= F.lit(60)),
                F.col("temperature").cast("DECIMAL(10,2)"),
            ).otherwise(F.lit(None).cast("DECIMAL(10,2)")),
        )
        .withColumn(
            "precipitation",
            F.when(
                F.col("precipitation") >= F.lit(0),
                F.col("precipitation").cast("DECIMAL(10,2)"),
            ).otherwise(F.lit(None).cast("DECIMAL(10,2)")),
        )
        .withColumn(
            "wind",
            F.when(
                F.col("wind") >= F.lit(0),
                F.col("wind").cast("DECIMAL(10,2)"),
            ).otherwise(F.lit(None).cast("DECIMAL(10,2)")),
        )
        .withColumn("silver_processed_at", F.current_timestamp())
        .select(
            "city",
            "timestamp",
            "measurement_date",
            "temperature",
            "precipitation",
            "wind",
            "silver_processed_at",
        )
    )

    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SILVER_TABLE_NAME)
    )


if __name__ == "__main__":
    main()