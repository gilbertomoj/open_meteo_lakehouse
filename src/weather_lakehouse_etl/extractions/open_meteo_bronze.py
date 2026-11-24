import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

VOLUME_PATH = "/Volumes/workspace/default/data/weather_data"
OUTPUT_FORMAT = "parquet"
BRONZE_TABLE_NAME = "workspace.default.weather_data_bronze"


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def main():
    spark = get_spark_session()

    timestamp_str = datetime.now().strftime("%d%m%Y")
    file_name = f"weather_data_{timestamp_str}.{OUTPUT_FORMAT.lower()}"
    file_path = os.path.join(VOLUME_PATH, file_name)

    df_raw = spark.read.format(OUTPUT_FORMAT.lower()).load(file_path)

    df_bronze = (
        df_raw.select(
            F.col("city").cast("STRING").alias("city"),
            F.col("datetime").cast("TIMESTAMP").alias("timestamp"),
            F.col("temperature_2m").cast("DECIMAL(38,0)").alias("temperature"),
            F.col("precipitation").cast("DECIMAL(38,0)").alias("precipitation"),
            F.col("wind_speed_10m").cast("DECIMAL(38,0)").alias("wind"),
        )
        .filter(F.col("city").isNotNull() & F.col("timestamp").isNotNull())
    )

    (
        df_bronze.write.format("delta")
        .mode("append")
        .saveAsTable(BRONZE_TABLE_NAME)
    )


if __name__ == "__main__":
    main()