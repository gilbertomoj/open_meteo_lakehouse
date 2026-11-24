from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_TABLE_NAME = "workspace.default.weather_data_silver"
GOLD_TABLE_NAME = "workspace.default.weather_data_gold"


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def main():
    spark = get_spark_session()

    df_silver = spark.read.table(SILVER_TABLE_NAME)

    df_gold = (
        df_silver.groupBy("city", "measurement_date")
        .agg(
            F.round(F.avg("temperature"), 2).alias("avg_temperature"),
            F.max("temperature").cast("DECIMAL(10,2)").alias("max_temperature"),
            F.min("temperature").cast("DECIMAL(10,2)").alias("min_temperature"),
            F.round(F.sum("precipitation"), 2).alias("total_precipitation"),
            F.round(F.avg("wind"), 2).alias("avg_wind"),
            F.max("wind").cast("DECIMAL(10,2)").alias("max_wind"),
            F.count("*").alias("total_records"),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
        .select(
            "city",
            "measurement_date",
            "avg_temperature",
            "max_temperature",
            "min_temperature",
            "total_precipitation",
            "avg_wind",
            "max_wind",
            "total_records",
            "gold_updated_at",
        )
        .orderBy("measurement_date", "city")
    )

    (
        df_gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_TABLE_NAME)
    )


if __name__ == "__main__":
    main()