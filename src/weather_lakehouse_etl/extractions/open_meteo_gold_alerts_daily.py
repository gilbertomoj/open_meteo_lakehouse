from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_TABLE_NAME = "workspace.default.weather_data_silver"
GOLD_ALERTS_TABLE_NAME = "workspace.default.weather_data_gold_alerts_daily"

# Limiares configuráveis de alerta
HEAVY_RAIN_MM_THRESHOLD = 20.0
STRONG_WIND_THRESHOLD = 40.0
HIGH_TEMP_RANGE_THRESHOLD = 12.0


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def build_gold_alerts_dataframe(spark: SparkSession):
    df_silver = spark.read.table(SILVER_TABLE_NAME)

    impacted_dates_df = (
        df_silver.filter(
            F.col("silver_processed_at") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")
        )
        .select("measurement_date")
        .distinct()
    )

    df_filtered = df_silver.join(
        impacted_dates_df, on="measurement_date", how="inner"
    )

    daily_agg = (
        df_filtered.groupBy("city", "measurement_date")
        .agg(
            F.max("temperature").cast("DECIMAL(10,2)").alias("max_temperature"),
            F.min("temperature").cast("DECIMAL(10,2)").alias("min_temperature"),
            F.round(F.sum("precipitation"), 2)
            .cast("DECIMAL(10,2)")
            .alias("total_precipitation"),
            F.max("wind").cast("DECIMAL(10,2)").alias("max_wind"),
            F.count("*").alias("total_records"),
        )
        .withColumn(
            "temp_range",
            (F.col("max_temperature") - F.col("min_temperature")).cast("DECIMAL(10,2)"),
        )
        .withColumn(
            "has_heavy_rain",
            F.col("total_precipitation") >= F.lit(HEAVY_RAIN_MM_THRESHOLD),
        )
        .withColumn(
            "has_strong_wind",
            F.col("max_wind") >= F.lit(STRONG_WIND_THRESHOLD),
        )
        .withColumn(
            "has_high_temp_range",
            F.col("temp_range") >= F.lit(HIGH_TEMP_RANGE_THRESHOLD),
        )
        .withColumn(
            "alert_count",
            F.col("has_heavy_rain").cast("INT")
            + F.col("has_strong_wind").cast("INT")
            + F.col("has_high_temp_range").cast("INT"),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
        .select(
            "city",
            "measurement_date",
            "max_temperature",
            "min_temperature",
            "temp_range",
            "total_precipitation",
            "max_wind",
            "has_heavy_rain",
            "has_strong_wind",
            "has_high_temp_range",
            "alert_count",
            "total_records",
            "gold_updated_at",
        )
    )

    return daily_agg


def main():
    spark = get_spark_session()
    df_gold = build_gold_alerts_dataframe(spark)

    if df_gold.rdd.isEmpty():
        return

    if not spark.catalog.tableExists(GOLD_ALERTS_TABLE_NAME):
        (
            df_gold.write.format("delta")
            .mode("overwrite")
            .saveAsTable(GOLD_ALERTS_TABLE_NAME)
        )
        return

    gold_table = DeltaTable.forName(spark, GOLD_ALERTS_TABLE_NAME)

    (
        gold_table.alias("target")
        .merge(
            df_gold.alias("source"),
            "target.city = source.city AND target.measurement_date = source.measurement_date",
        )
        .whenMatchedUpdate(
            set={
                "max_temperature": "source.max_temperature",
                "min_temperature": "source.min_temperature",
                "temp_range": "source.temp_range",
                "total_precipitation": "source.total_precipitation",
                "max_wind": "source.max_wind",
                "has_heavy_rain": "source.has_heavy_rain",
                "has_strong_wind": "source.has_strong_wind",
                "has_high_temp_range": "source.has_high_temp_range",
                "alert_count": "source.alert_count",
                "total_records": "source.total_records",
                "gold_updated_at": "source.gold_updated_at",
            }
        )
        .whenNotMatchedInsert(
            values={
                "city": "source.city",
                "measurement_date": "source.measurement_date",
                "max_temperature": "source.max_temperature",
                "min_temperature": "source.min_temperature",
                "temp_range": "source.temp_range",
                "total_precipitation": "source.total_precipitation",
                "max_wind": "source.max_wind",
                "has_heavy_rain": "source.has_heavy_rain",
                "has_strong_wind": "source.has_strong_wind",
                "has_high_temp_range": "source.has_high_temp_range",
                "alert_count": "source.alert_count",
                "total_records": "source.total_records",
                "gold_updated_at": "source.gold_updated_at",
            }
        )
        .execute()
    )


if __name__ == "__main__":
    main()