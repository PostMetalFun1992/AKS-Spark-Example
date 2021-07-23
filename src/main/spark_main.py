from utils import AZ_STORAGE_ACCOUNT, create_spark_session


def main():
    spark = create_spark_session()

    hotels_raw = (
        spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(
            f"abfss://m06sparkbasics@{AZ_STORAGE_ACCOUNT}.dfs.core.windows.net/hotels"
        )
    )

    weather_raw = spark.read.format("parquet").load(
        f"abfss://m06sparkbasics@{AZ_STORAGE_ACCOUNT}.dfs.core.windows.net/weather"
    )

    hotels_raw.show()
    weather_raw.show()


if __name__ == "__main__":
    main()
