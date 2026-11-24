from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_TABLE_NAME = "workspace.default.weather_data_silver"
GOLD_WEEKLY_TABLE_NAME = "workspace.default.weather_data_gold_weekly"


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def build_gold_weekly_dataframe(spark: SparkSession):
    df_silver = spark.read.table(SILVER_TABLE_NAME)

    # Identifica datas impactadas recentemente (ajuste a janela conforme a frequência do job)
    max_processed_ts = df_silver.agg(
        F.max("silver_processed_at").alias("max_ts")
    ).collect()[0]["max_ts"]

    if max_processed_ts is None:
        return spark.createDataFrame([], schema="city STRING, week_start_date DATE")

    impacted_weeks_df = (
        df_silver.filter(
            F.col("silver_processed_at") >= F.expr("timestamp(:max_ts) - INTERVAL 1 HOUR")
        )
        .withColumn(
            "week_start_date",
            F.to_date(F.date_trunc("WEEK", F.col("measurement_date"))),
        )
        .select("week_start_date")
        .distinct()
    )

    df_with_week = df_silver.withColumn(
        "week_start_date",
        F.to_date(F.date_trunc("WEEK", F.col("measurement_date"))),
    )

    df_filtered = df_with_week.join(
        impacted_weeks_df, on="week_start_date", how="inner"
    )

    return (
        df_filtered.groupBy("city", "week_start_date")
        .agg(
            F.round(F.avg("temperature"), 2)
            .cast("DECIMAL(10,2)")
            .alias("avg_temperature"),
            F.max("temperature").cast("DECIMAL(10,2)").alias("max_temperature"),
            F.min("temperature").cast("DECIMAL(10,2)").alias("min_temperature"),
            F.round(F.sum("precipitation"), 2)
            .cast("DECIMAL(10,2)")
            .alias("total_precipitation"),
            F.round(F.avg("wind"), 2).cast("DECIMAL(10,2)").alias("avg_wind"),
            F.max("wind").cast("DECIMAL(10,2)").alias("max_wind"),
            F.countDistinct("measurement_date").alias("active_days"),
            F.count("*").alias("total_records"),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
        .select(
            "city",
            "week_start_date",
            "avg_temperature",
            "max_temperature",
            "min_temperature",
            "total_precipitation",
            "avg_wind",
            "max_wind",
            "active_days",
            "total_records",
            "gold_updated_at",
        )
    )


def main():
    spark = get_spark_session()
    df_gold = build_gold_weekly_dataframe(spark)

    if df_gold.rdd.isEmpty():
        return

    if not spark.catalog.tableExists(GOLD_WEEKLY_TABLE_NAME):
        (
            df_gold.write.format("delta")
            .mode("overwrite")
            .saveAsTable(GOLD_WEEKLY_TABLE_NAME)
        )
        return

    gold_table = DeltaTable.forName(spark, GOLD_WEEKLY_TABLE_NAME)

    (
        gold_table.alias("target")
        .merge(
            df_gold.alias("source"),
            "target.city = source.city AND target.week_start_date = source.week_start_date",
        )
        .whenMatchedUpdate(
            set={
                "avg_temperature": "source.avg_temperature",
                "max_temperature": "source.max_temperature",
                "min_temperature": "source.min_temperature",
                "total_precipitation": "source.total_precipitation",
                "avg_wind": "source.avg_wind",
                "max_wind": "source.max_wind",
                "active_days": "source.active_days",
                "total_records": "source.total_records",
                "gold_updated_at": "source.gold_updated_at",
            }
        )
        .whenNotMatchedInsert(
            values={
                "city": "source.city",
                "week_start_date": "source.week_start_date",
                "avg_temperature": "source.avg_temperature",
                "max_temperature": "source.max_temperature",
                "min_temperature": "source.min_temperature",
                "total_precipitation": "source.total_precipitation",
                "avg_wind": "source.avg_wind",
                "max_wind": "source.max_wind",
                "active_days": "source.active_days",
                "total_records": "source.total_records",
                "gold_updated_at": "source.gold_updated_at",
            }
        )
        .execute()
    )


if __name__ == "__main__":
    main()