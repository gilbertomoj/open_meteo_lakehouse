from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_TABLE_NAME = "workspace.default.weather_data_silver"
GOLD_MONTHLY_TABLE_NAME = "workspace.default.weather_data_gold_monthly"


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def build_gold_monthly_dataframe(spark: SparkSession):
    df_silver = spark.read.table(SILVER_TABLE_NAME)

    df_with_month = df_silver.withColumn(
        "year_month", F.date_format(F.col("measurement_date"), "yyyy-MM")
    )

    impacted_months_df = (
        df_with_month.filter(
            F.col("silver_processed_at") >= F.current_timestamp() - F.expr("INTERVAL 24 HOURS")
        )
        .select("year_month")
        .distinct()
    )

    df_filtered = df_with_month.join(
        impacted_months_df, on="year_month", how="inner"
    )

    return (
        df_filtered.groupBy("city", "year_month")
        .agg(
            F.round(F.avg("temperature"), 2)
            .cast("DECIMAL(10,2)")
            .alias("avg_temperature"),
            F.max("temperature").cast("DECIMAL(10,2)").alias("max_temperature"),
            F.min("temperature").cast("DECIMAL(10,2)").alias("min_temperature"),
            F.round(F.sum("precipitation"), 2)
            .cast("DECIMAL(10,2)")
            .alias("total_precipitation"),
            F.sum(
                F.when(F.col("precipitation") > F.lit(0), F.lit(1)).otherwise(F.lit(0))
            ).alias("rainy_records"),
            F.round(F.avg("wind"), 2).cast("DECIMAL(10,2)").alias("avg_wind"),
            F.max("wind").cast("DECIMAL(10,2)").alias("max_wind"),
            F.count("*").alias("total_records"),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
        .select(
            "city",
            "year_month",
            "avg_temperature",
            "max_temperature",
            "min_temperature",
            "total_precipitation",
            "rainy_records",
            "avg_wind",
            "max_wind",
            "total_records",
            "gold_updated_at",
        )
    )


def main():
    spark = get_spark_session()
    df_gold = build_gold_monthly_dataframe(spark)

    if df_gold.rdd.isEmpty():
        return

    if not spark.catalog.tableExists(GOLD_MONTHLY_TABLE_NAME):
        (
            df_gold.write.format("delta")
            .mode("overwrite")
            .saveAsTable(GOLD_MONTHLY_TABLE_NAME)
        )
        return

    gold_table = DeltaTable.forName(spark, GOLD_MONTHLY_TABLE_NAME)

    (
        gold_table.alias("target")
        .merge(
            df_gold.alias("source"),
            "target.city = source.city AND target.year_month = source.year_month",
        )
        .whenMatchedUpdate(
            set={
                "avg_temperature": "source.avg_temperature",
                "max_temperature": "source.max_temperature",
                "min_temperature": "source.min_temperature",
                "total_precipitation": "source.total_precipitation",
                "rainy_records": "source.rainy_records",
                "avg_wind": "source.avg_wind",
                "max_wind": "source.max_wind",
                "total_records": "source.total_records",
                "gold_updated_at": "source.gold_updated_at",
            }
        )
        .whenNotMatchedInsert(
            values={
                "city": "source.city",
                "year_month": "source.year_month",
                "avg_temperature": "source.avg_temperature",
                "max_temperature": "source.max_temperature",
                "min_temperature": "source.min_temperature",
                "total_precipitation": "source.total_precipitation",
                "rainy_records": "source.rainy_records",
                "avg_wind": "source.avg_wind",
                "max_wind": "source.max_wind",
                "total_records": "source.total_records",
                "gold_updated_at": "source.gold_updated_at",
            }
        )
        .execute()
    )


if __name__ == "__main__":
    main()