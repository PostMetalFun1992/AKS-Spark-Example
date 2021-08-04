from pyspark.sql.functions import col

from transform.geohash import calc_geohash_udf, enrich_hotels_with_geohash
from transform.utils import create_spark_session, get_storage_uris


def main():
    spark = create_spark_session()

    in_storage_uri, out_storage_uri = get_storage_uris()

    hotels_raw = (
        spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(f"{in_storage_uri}/hotels")
    )
    hotels_enriched = enrich_hotels_with_geohash(spark, hotels_raw)
    # hotels_enriched.show()

    weather_raw = spark.read.format("parquet").load(f"{in_storage_uri}/weather")
    weather_enriched = weather_raw.withColumn(
        "weather_geohash", calc_geohash_udf(col("lat"), col("lng"))
    )
    # weather_enriched.show()

    weather_hotels = weather_enriched.join(
        hotels_enriched,
        weather_enriched.weather_geohash == hotels_enriched.HotelGeohash,
        "leftouter",
    )
    # weather_hotels.show()

    weather_hotels.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
        f"{out_storage_uri}/weather_hotels"
    )

    spark.stop()


if __name__ == "__main__":
    main()
