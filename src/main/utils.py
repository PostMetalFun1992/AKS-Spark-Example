import configparser

from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read("/etc/secrets/storage-creds.ini")

AZ_IN_STORAGE_ACCOUNT = config["INPUT"]["AZ_STORAGE_ACCOUNT"]
AZ_IN_CONTAINER = config["INPUT"]["AZ_CONTAINER"]
AZ_IN_CLIENT_ID = config["INPUT"]["AZ_CLIENT_ID"]
AZ_IN_CLIENT_SECRET = config["INPUT"]["AZ_CLIENT_SECRET"]
AZ_IN_CLIENT_ENDPOINT = config["INPUT"]["AZ_CLIENT_ENDPOINT"]

AZ_OUT_STORAGE_ACCOUNT = config["OUTPUT"]["AZ_STORAGE_ACCOUNT"]
AZ_OUT_CONTAINER = config["OUTPUT"]["AZ_CONTAINER"]
AZ_OUT_CLIENT_ID = config["OUTPUT"]["AZ_CLIENT_ID"]
AZ_OUT_CLIENT_SECRET = config["OUTPUT"]["AZ_CLIENT_SECRET"]
AZ_OUT_CLIENT_ENDPOINT = config["OUTPUT"]["AZ_CLIENT_ENDPOINT"]


def get_storage_uris():
    return (
        f"abfss://{AZ_IN_CONTAINER}@{AZ_IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"abfss://{AZ_OUT_CONTAINER}@{AZ_OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
    )


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()

    spark = _configure_input_storage_settings(spark)
    spark = _configure_output_storage_settings(spark)

    return spark


def _configure_input_storage_settings(spark):
    spark.conf.set(
        f"fs.azure.account.auth.type.{AZ_IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "OAuth",
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{AZ_IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{AZ_IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZ_IN_CLIENT_ID}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{AZ_IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZ_IN_CLIENT_SECRET}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{AZ_IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZ_IN_CLIENT_ENDPOINT}",
    )

    return spark


def _configure_output_storage_settings(spark):
    spark.conf.set(
        f"fs.azure.account.auth.type.{AZ_OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "OAuth",
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{AZ_OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{AZ_OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZ_OUT_CLIENT_ID}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{AZ_OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZ_OUT_CLIENT_SECRET}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{AZ_OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZ_OUT_CLIENT_ENDPOINT}",
    )

    return spark
