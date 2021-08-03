from pyspark.sql import SparkSession

from constants import AZStorage


def get_storage_uris():
    return (
        f"abfss://{AZStorage.IN_CONTAINER}@{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"abfss://{AZStorage.OUT_CONTAINER}@{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
    )


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()

    spark = _configure_input_storage_settings(spark)
    spark = _configure_output_storage_settings(spark)

    return spark


def _configure_input_storage_settings(spark):
    spark.conf.set(
        f"fs.azure.account.auth.type.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "OAuth",
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZStorage.IN_CLIENT_ID}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZStorage.IN_CLIENT_SECRET}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZStorage.IN_CLIENT_ENDPOINT}",
    )

    return spark


def _configure_output_storage_settings(spark):
    spark.conf.set(
        f"fs.azure.account.auth.type.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "OAuth",
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZStorage.OUT_CLIENT_ID}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZStorage.OUT_CLIENT_SECRET}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZStorage.OUT_CLIENT_ENDPOINT}",
    )

    return spark
