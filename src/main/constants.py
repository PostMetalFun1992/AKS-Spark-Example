import configparser

from pyspark.sql import types as t
from pyspark.sql.types import StructField, StructType

config = configparser.ConfigParser()
config.read("/etc/secrets/storage-creds.ini")


class AZStorage:
    IN_STORAGE_ACCOUNT = config["INPUT"]["AZ_STORAGE_ACCOUNT"]
    IN_CONTAINER = config["INPUT"]["AZ_CONTAINER"]
    IN_CLIENT_ID = config["INPUT"]["AZ_CLIENT_ID"]
    IN_CLIENT_SECRET = config["INPUT"]["AZ_CLIENT_SECRET"]
    IN_CLIENT_ENDPOINT = config["INPUT"]["AZ_CLIENT_ENDPOINT"]

    OUT_STORAGE_ACCOUNT = config["OUTPUT"]["AZ_STORAGE_ACCOUNT"]
    OUT_CONTAINER = config["OUTPUT"]["AZ_CONTAINER"]
    OUT_CLIENT_ID = config["OUTPUT"]["AZ_CLIENT_ID"]
    OUT_CLIENT_SECRET = config["OUTPUT"]["AZ_CLIENT_SECRET"]
    OUT_CLIENT_ENDPOINT = config["OUTPUT"]["AZ_CLIENT_ENDPOINT"]


OPENCAGE_API_KEY = config["OPENCAGE"]["API_KEY"]


HotelsEnrichedSchema = StructType(
    [
        StructField("Id", t.LongType(), False),
        StructField("Name", t.StringType(), False),
        StructField("Country", t.StringType(), False),
        StructField("City", t.StringType(), False),
        StructField("Address", t.StringType(), False),
        StructField("Latitude", t.DoubleType(), True),
        StructField("Longitude", t.DoubleType(), True),
        StructField("HotelGeohash", t.StringType(), True),
    ]
)
