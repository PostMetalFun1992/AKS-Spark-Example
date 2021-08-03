from pyspark.sql.functions import col

from enrich import calc_geohash_udf, enrich_hotels
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

    weather_raw = spark.read.format("parquet").load(f"{in_storage_uri}/weather")

    hotels_enriched = enrich_hotels(spark, hotels_raw)
    hotels_enriched.show()

    weather_enriched = weather_raw.withColumn(
        "geohash", calc_geohash_udf(col("lat"), col("lng"))
    )
    weather_enriched.show()

    hotels_raw.write.parquet(f"{out_storage_uri}/hotels")

    spark.stop()


if __name__ == "__main__":
    main()
