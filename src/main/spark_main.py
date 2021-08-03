from enrich import enrich_hotels
from utils import create_spark_session, get_storage_uris


def main():
    spark = create_spark_session()
    in_storage_uri, out_storage_uri = get_storage_uris()

    hotels_raw = (
        spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(f"{in_storage_uri}/hotels")
    )

    hotels_enriched = enrich_hotels(spark, hotels_raw)
    hotels_enriched.show()

    weather_raw = spark.read.format("parquet").load(f"{in_storage_uri}/weather")
    weather_raw.printSchema()

    """
    root
    |-- lng: double (nullable = true)
    |-- lat: double (nullable = true)
    |-- avg_tmpr_f: double (nullable = true)
    |-- avg_tmpr_c: double (nullable = true)
    |-- wthr_date: string (nullable = true)
    |-- year: integer (nullable = true)
    |-- month: integer (nullable = true)
    |-- day: integer (nullable = true)
    """

    hotels_raw.write.parquet(f"{out_storage_uri}/hotels")

    spark.stop()


if __name__ == "__main__":
    main()
