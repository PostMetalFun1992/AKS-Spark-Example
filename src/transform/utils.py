from pyspark.sql import SparkSession

from transform.constants import AZStorage


def get_storage_uris():
    return (
        f"abfss://{AZStorage.IN_CONTAINER}@{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net",
        f"abfss://{AZStorage.OUT_CONTAINER}@{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net",
    )


def create_spark_session():
    spark = SparkSession.builder.getOrCreate()

    for setting_name, setting_value in {
        **_get_input_storage_settings(),
        **_get_output_storage_settings(),
    }.items():
        spark.conf.set(setting_name, setting_value)

    return spark


def _get_input_storage_settings():
    return {
        f"fs.azure.account.auth.type.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net": "OAuth",
        f"fs.azure.account.oauth.provider.type.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net": f"{AZStorage.IN_CLIENT_ID}",
        f"fs.azure.account.oauth2.client.secret.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net": f"{AZStorage.IN_CLIENT_SECRET}",
        f"fs.azure.account.oauth2.client.endpoint.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net": f"{AZStorage.IN_CLIENT_ENDPOINT}",
    }


def _get_output_storage_settings():
    return {
        f"fs.azure.account.auth.type.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net": "OAuth",
        f"fs.azure.account.oauth.provider.type.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net": f"{AZStorage.OUT_CLIENT_ID}",
        f"fs.azure.account.oauth2.client.secret.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net": f"{AZStorage.OUT_CLIENT_SECRET}",
        f"fs.azure.account.oauth2.client.endpoint.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net": f"{AZStorage.OUT_CLIENT_ENDPOINT}",
    }
