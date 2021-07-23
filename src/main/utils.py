import os

from pyspark.sql import SparkSession

AZ_STORAGE_ACCOUNT = os.getenv("AZ_STORAGE_ACCOUNT")
AZ_CLIENT_ID = os.getenv("AZ_CLIENT_ID")
AZ_CLIENT_SECRET = os.getenv("AZ_CLIENT_SECRET")
AZ_CLIENT_ENDPOINT = os.getenv("AZ_CLIENT_ENDPOINT")


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()

    spark.conf.set(
        f"fs.azure.account.auth.type.{AZ_STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth"
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{AZ_STORAGE_ACCOUNT}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{AZ_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZ_CLIENT_ID}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{AZ_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZ_CLIENT_SECRET}",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{AZ_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"{AZ_CLIENT_ENDPOINT}",
    )

    return spark
