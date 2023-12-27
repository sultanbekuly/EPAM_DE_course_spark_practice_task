from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, count, when, udf, lit
from pyspark.sql.types import StringType, StructType, StructField, DoubleType
import requests
import geohash2
from geopy.geocoders import OpenCage
import logging
from datetime import datetime
import configparser

# Function to generate a four-character geohash
def generate_geohash(lat, lng):
    if lat is not None and lng is not None:
        return geohash2.encode(lat, lng, precision=4)
    else:
        return None


def geocode_restaurant(franchise_name, city, country, api_key):
    geocoder = OpenCage(api_key)
    try:
        print(f"{franchise_name}, {city}, country={country}")
        location = geocoder.geocode(f"{franchise_name}, {city}", country=country)
        if location:
            print( (location.latitude, location.longitude) )
            return (location.latitude, location.longitude)
    except Exception as e:
        logging.warning(f"Geocoding not possible for '{franchise_name}, {city}': {e}")
    return (None, None)


def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("RestaurantWeatherDataIntegration") \
        .getOrCreate()

    # Read data from CSV files
    path_to_csv_folder = "raw/restaurant_csv"
    df_restaurant = spark.read.csv(path_to_csv_folder, header=True, inferSchema=True)

    # Print the schema (data types) and show first few rows of the df
    df_restaurant.printSchema()
    df_restaurant.show()
    print("a")

    config = configparser.ConfigParser()
    config.read('config.ini')
    api_key = config['DEFAULT']['OpenCageApiKey']

    # Define the schema for the UDF's return type
    return_schema = StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True)
    ])

    # Register the Python function as a UDF
    geocode_udf = udf(geocode_restaurant, return_schema)

    # Filter out rows where lat/lng is null and need geocoding
    rows_to_geocode = df_restaurant.filter(df_restaurant["lat"].isNull() | df_restaurant["lng"].isNull())

    # Apply the UDF to create new lat and lng columns for these rows
    rows_to_geocode = rows_to_geocode.withColumn("new_coordinates",
                                                 geocode_udf(col("franchise_name"), col("city"), col("country"),
                                                             lit(api_key)))

    # Split the coordinates into separate columns
    rows_to_geocode = rows_to_geocode.withColumn("new_lat", col("new_coordinates.latitude")) \
        .withColumn("new_lng", col("new_coordinates.longitude")) \
        .drop("new_coordinates")

    # Replace lat/lng with new values in rows_to_geocode DataFrame
    rows_to_geocode = rows_to_geocode.withColumn("lat", col("new_lat")) \
        .withColumn("lng", col("new_lng")) \
        .drop("new_lat", "new_lng")

    # Filter out rows from the original DataFrame where lat/lng is not null
    rows_without_geocode = df_restaurant.filter(df_restaurant["lat"].isNotNull() & df_restaurant["lng"].isNotNull())

    # Union the two DataFrames: rows_without_geocode, rows_to_geocode
    df_restaurant = rows_without_geocode.union(rows_to_geocode)

    # Register the Python function as a UDF
    generate_geohash_udf = udf(generate_geohash, StringType())

    # Apply the UDF to create the geohash column
    df_restaurant = df_restaurant.withColumn(
        "geohash",
        generate_geohash_udf(col("lat"), col("lng")))
    df_restaurant = df_restaurant.withColumnsRenamed({"lat": "restaurant_lat", "lng": "restaurant_lng"}) \
                                .dropDuplicates(["franchise_id", "geohash"])

    df_restaurant.printSchema()
    df_restaurant.show()
    print("df shape", df_restaurant.count(), len(df_restaurant.columns))
    print("b")

    weather_data_path = "raw/weather/datasets/"
    df_weather = spark.read.parquet(weather_data_path)

    # df_weather = df_weather.limit(1000000)  # for test

    # Apply the UDF to create the geohash column
    df_weather = df_weather.withColumn("geohash", generate_geohash_udf(col("lat"), col("lng")))
    df_weather = df_weather.withColumnsRenamed({"lat": "weather_lat", "lng": "weather_lng"}) \
                           .dropDuplicates(["geohash", "year", "month", "day"])

    df_weather.printSchema()
    df_weather.show()
    print("df shape", df_weather.count(), len(df_weather.columns))
    print("c")

    df_enriched = df_weather.join(df_restaurant, "geohash", "left") \
        .where("restaurant_lat is not null")\
        .dropDuplicates(["geohash", "year", "month", "day", "restaurant_franchise_id"])

    df_enriched.printSchema()
    df_enriched.show()
    print("d")

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = f"df_enriched_{timestamp}"
    print(output_path)
    df_enriched.write.partitionBy("year", "month", "day").parquet(output_path)
    print("e")

    # Close the Spark session
    spark.stop()
    print("z")


if __name__ == '__main__':
    main()
