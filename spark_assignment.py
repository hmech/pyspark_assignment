# To run: spark-submit --driver-memory 4g spark_assignment.py

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import StringType, IntegerType

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("NY Taxi Data Analysis") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# 2. Load data
# Load taxi trip data (parquet)
ny_taxi = spark.read.parquet("/Users/hmechalchuk/Documents/repos/scratch/pyspark/ny_taxi_test_data/ny_taxi")
# Load zones data (CSV)
ny_taxi_zones = spark.read.option("header", True).csv("/Users/hmechalchuk/Documents/repos/scratch/pyspark/ny_taxi_test_data/ny_zones/ny_taxi_zones.csv")
# Load taxi drivers to be forgotten (CSV)
rtbf_taxies = spark.read.option("header", True).csv("/Users/hmechalchuk/Documents/repos/scratch/pyspark/ny_taxi_test_data/ny_taxi_rtbf/rtbf_taxies.csv")

# 5. Show schemas and sample data
print("Taxi Data Schema:")
ny_taxi.printSchema()
# ny_taxi.show(5)

print("Zones Data Schema:")
ny_taxi_zones.printSchema()
# ny_taxi_zones.show(5)

print("RTBF Data Schema:")
rtbf_taxies.printSchema()
# rtbf_taxies.show(5)

########################### The Tasks ###########################
# 1. - 3., 6. Load the data as shown above.

# 4. Now we can process the data; include the ny_taxi data between the dates `2015-01-15` to `2015-02-15` using the `tpep_pickup_datetime` column.
# Assuming that the date range is inclusive
print("\nStarting Task 4:")
ny_taxi_date_filtered = ny_taxi.filter(
    (ny_taxi.tpep_pickup_datetime >= "2015-01-15") & 
    (ny_taxi.tpep_pickup_datetime <= "2015-02-15")
)

print("Taxi Data Schema:")
ny_taxi_date_filtered.printSchema()
ny_taxi_date_filtered.show(5)

print("Showing min and max dates:")
ny_taxi_date_filtered_check = ny_taxi_date_filtered.agg(
    F.min("tpep_pickup_datetime").alias("min date"), 
    F.max("tpep_dropoff_datetime").alias("max date"), 
)

ny_taxi_date_filtered_check.show()

# 5. Filter right to be forgotten taxi\_ids. Remove all rows that have a `taxi_id` that is in the ny\_taxi\_rtbf list.
# Using left_anti to remove common taxi_ids
print("\nStarting Task 5:")
ny_taxi_id_filtered = ny_taxi.join(
    rtbf_taxies,
    ny_taxi.taxi_id == rtbf_taxies.taxi_id,
    "left_anti"
)
ny_taxi_id_count_before = ny_taxi.select("taxi_id").distinct().count()
ny_taxi_id_count_after = ny_taxi_id_filtered.select("taxi_id").distinct().count()

print(f"Taxi ID count before filtering: {ny_taxi_id_count_before}")
print(f"Taxi ID count after filtering: {ny_taxi_id_count_after}")
print(f"Number of rows removed: {ny_taxi_id_count_before - ny_taxi_id_count_after}")

# remove to keep seperate
ny_taxi = ny_taxi_id_filtered

# 7. Using the geocoding data (`ny_taxi_zones`) and the appropriate index column in the `ny_taxi` data, geocode each pickup location with zone and borough. We would like 2 new columns: `pickup_zone` and `pickup_borough`.
# Convert the hex string to decimal for comparison
print("\nStarting Task 7:")
def hex_to_dec(hex_str):
    try:
        return str(int(hex_str, 16))
    except Exception:
        return None

# Register the UDF to convert hex to decimal
hex_to_dec_udf = F.udf(hex_to_dec, StringType())

ny_taxi = ny_taxi.alias("taxi").join(
    ny_taxi_zones.alias("pickup_zone"),
    hex_to_dec_udf(F.col("taxi.pickup_h3_index")) == F.col("pickup_zone.h3_index"),
    "left"
).select(
    F.col("taxi.*"),
    F.col("pickup_zone.zone").alias("pickup_zone"),
    F.col("pickup_zone.borough").alias("pickup_borough")
)
print("Geocoded Taxi Data Schema:")
ny_taxi.printSchema()
ny_taxi.show(3)

# 8. Using the geocoding data (`ny_taxi_zones`) and the appropriate index column in the `ny_taxi` data, geocode each dropoff location with zone and borough. We would like 2 new columns: `dropoff_zone` and `dropoff_borough`.
print("\nStarting Task 8:")
ny_taxi = ny_taxi.alias("taxi").join(
    ny_taxi_zones.alias("dropoff_zone"),
    hex_to_dec_udf(F.col("taxi.dropoff_h3_index")) == F.col("dropoff_zone.h3_index"),
    "left"
).select(
    F.col("taxi.*"),
    F.col("dropoff_zone.zone").alias("dropoff_zone"),
    F.col("dropoff_zone.borough").alias("dropoff_borough")
)
print("Geocoded Taxi Data Schema with Dropoff:")
ny_taxi.printSchema()
ny_taxi.show(3)

# 9. **Insight 1:** Calculate the average total fare for each `trip_distance` (trip distance rounded to 0 decimal places) and the number of trips. Order the output by trip\_distance.  
# Write the output as a single csv with headers. The resulting output should have 3 columns: `trip_distance`, `average_total_fare` and `number_of_trips`. 
print("\nStarting Task 9:")
ny_taxi_insight1 = ny_taxi.groupBy(
    F.round("trip_distance", 0).alias("trip_distance")
).agg(
    F.avg("fare_amount").alias("average_total_fare"),
    F.count("*").alias("number_of_trips")
).orderBy("trip_distance")

ny_taxi_insight1.write.csv("./results/ny_taxi_insight1", header=True, mode="overwrite")
print("ny_taxi_insight1 CSV created")

# 10. Looking at the output of step 9, decide what would be an appropriate upper limit of `trip_distance` and rerun with this filtered.
# ./results/ny_taxi_insight1.csv 
# - Show negative distance 
# - Values appear to become statistically insignificant after 65.0 miles. After this point, the average fare becomes erratic and the number of trips is very low.
print("\nStarting Task 10:")
ny_taxi_insight1_filtered = ny_taxi.filter(
    (ny_taxi.trip_distance >= 0) & 
    (ny_taxi.trip_distance <= 65.0)
).groupBy(
    F.round("trip_distance", 0).alias("trip_distance").cast(IntegerType())
).agg(
    F.avg("fare_amount").alias("average_total_fare"),
    F.count("*").alias("number_of_trips")
).orderBy("trip_distance")

ny_taxi_insight1_filtered.write.csv("./results/ny_taxi_insight1_filtered", header=True, mode="overwrite")
print("ny_taxi_insight1_filtered CSV created")

# 11. Filter rows if any of the columns: `pickup_zone`, `pickup_borough`, `dropoff_zone` and `dropoff_borough` are null.
print("\nStarting Task 11:")
ny_taxi_not_null = ny_taxi.filter(
    (ny_taxi.pickup_zone.isNotNull()) &
    (ny_taxi.pickup_borough.isNotNull()) &
    (ny_taxi.dropoff_zone.isNotNull()) &
    (ny_taxi.dropoff_borough.isNotNull())
)

ny_taxi_not_null_count_before = ny_taxi.count()
ny_taxi_not_null_count_after = ny_taxi_not_null.count()

print(f"Count before filtering: {ny_taxi_not_null_count_before}")
print(f"Count after filtering: {ny_taxi_not_null_count_after}")
print(f"Number of rows removed: {ny_taxi_not_null_count_before - ny_taxi_not_null_count_after}")

# remove to keep seperate
ny_taxi = ny_taxi_not_null

# 12. **Insight 2:** Total number of pickups in each zone.  
# Write the output as a single csv with headers. The resulting output should have 2 columns: `zone` and `number_of_pickups`.
print("\nStarting Task 12:")
ny_taxi_zone = ny_taxi.withColumnRenamed("pickup_zone", "zone")
ny_taxi_insight2 = ny_taxi_zone.groupBy("zone").agg(
    F.count("*").alias("number_of_pickups")
).orderBy(F.desc("number_of_pickups"))
ny_taxi_insight2.write.csv("./results/ny_taxi_insight2", header=True, mode="overwrite")
print("ny_taxi_insight2 CSV created")

# 13. **Insight 3:** Total number of pickups in each borough.  
# Write the output as a single csv with headers. The resulting output should have 2 columns: `borough` and `number_of_pickups`.
print("\nStarting Task 13:")
ny_taxi_borough = ny_taxi.withColumnRenamed("pickup_borough", "borough")
ny_taxi_insight3 = ny_taxi_borough.groupBy("borough").agg(
    F.count("*").alias("number_of_pickups")
).orderBy(F.desc("number_of_pickups"))
ny_taxi_insight3.write.csv("./results/ny_taxi_insight3", header=True, mode="overwrite")
print("ny_taxi_insight3 CSV created")

# 14. **Insight 4:** Total number of dropoffs, average total cost and average distance in each zone.  
# Write the output as a single csv with headers. The resulting output should have 4 columns: `zone`, `number_of_dropoffs`, `average_total_fare` and `average_trip_distance`.
print("\nStarting Task 14:")
ny_taxi_insight4 = ny_taxi_zone.groupBy("zone").agg(
    F.count("*").alias("number_of_dropoffs"),
    F.round(F.avg("fare_amount"), 2).alias("average_total_fare"),
    F.round(F.avg("trip_distance"), 1).alias("average_trip_distance")
)
ny_taxi_insight4.write.csv("./results/ny_taxi_insight4", header=True, mode="overwrite")
print("ny_taxi_insight4 CSV created")

# 15. **Insight 5:** Total number of dropoffs, average total cost and average distance in each borough.  
# Write the output as a single csv with headers. The resulting output should have 4 columns: `borough`, `number_of_dropoffs`, `average_total_fare` and `average_trip_distance`.
print("\nStarting Task 14:")
ny_taxi_insight5 = ny_taxi_borough.groupBy("borough").agg(
    F.count("*").alias("number_of_dropoffs"),
    F.round(F.avg("fare_amount"), 2).alias("average_total_fare"),
    F.round(F.avg("trip_distance"), 1).alias("average_trip_distance")
).orderby("borough")
ny_taxi_insight5.write.csv("./results/ny_taxi_insight5", header=True, mode="overwrite")
print("ny_taxi_insight5 CSV created")

# 16. **Insight 6:** For each pickup zone calculate the top 5 dropoff zones ranked by number of trips.  
# Write output as a single csv with headers. The resulting output should have 4 columns: `pickup_zone`, `dropoff_zone`, `number_of_dropoffs` and `rank`.
print("\nStarting Task 16:")
ny_taxi_insight6 = ny_taxi.groupBy("pickup_zone", "dropoff_zone").agg(
    F.count("*").alias("number_of_dropoffs")
).withColumn(
    "rank", F.rank().over(
        Window.partitionBy("pickup_zone").orderBy(F.desc("number_of_dropoffs"))
    )
).filter(F.col("rank") <= 5).orderBy("pickup_zone", "rank")
ny_taxi_insight6.write.csv("./results/ny_taxi_insight6", header=True, mode="overwrite")
print("ny_taxi_insight6 CSV created")

# 17. **Insight 7:** Calculate the average number of trips per hour (using `tpep_pickup_datetime`).  
# The resulting output should have 2 columns: `hour_of_day` and `average_trips`.
print("\nStarting Task 17:")
ny_taxi_insight7 = ny_taxi.groupBy(F.hour("tpep_pickup_datetime")).agg(
    F.hour("tpep_pickup_datetime").alias("hour_of_day"),
    F.count("*").alias("number_of_trips")
).groupBy("hour_of_day").agg(
    F.avg("number_of_trips").alias("average_trips")
).orderBy("hour_of_day")
ny_taxi_insight7.write.csv("./results/ny_taxi_insight7", header=True, mode="overwrite")
print("ny_taxi_insight7 CSV created")


########################### The End ###########################

# Stop Spark session
spark.stop()
