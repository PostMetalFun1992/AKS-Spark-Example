import pygeohash as pgh
import requests
from pyspark.sql.functions import udf

from constants import OPENCAGE_API_KEY, HotelsEnrichedSchema


def enrich_hotels(spark, hotels_raw):
    rows = hotels_raw.rdd.map(lambda r: r.asDict()).collect()

    for row in rows:
        try:
            row["Latitude"] = float(row["Latitude"])
            row["Longitude"] = float(row["Longitude"])
        except (TypeError, ValueError):
            row["Latitude"], row["Longitude"] = _request_coords(row["Name"])

        row["Geohash"] = _calc_geohash(row["Latitude"], row["Longitude"])

    return spark.createDataFrame(rows, HotelsEnrichedSchema)


def _request_coords(hotel_name):
    resp = requests.get(
        f"https://api.opencagedata.com/geocode/v1/json?q={hotel_name}&key={OPENCAGE_API_KEY}"
    )

    if not resp.status_code == 200:
        return None, None

    results = resp.json()["results"]

    if not len(results):
        return None, None

    geometry = next(iter(results))["geometry"]

    return geometry["lat"], geometry["lng"]


@udf
def calc_geohash_udf(lat, lng):
    return _calc_geohash(lat, lng)


def _calc_geohash(lat, lng):
    if not lat or not lng:
        return None

    return pgh.encode(lat, lng, precision=4)
