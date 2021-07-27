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

    """
    weather_raw = spark.read.format("parquet").load(
        f"{input_storage_uri}/weather"
    )
    """

    hotels_raw.show()
    # weather_raw.show()

    hotels_raw.write.parquet(f"{out_storage_uri}/hotels")


if __name__ == "__main__":
    main()
