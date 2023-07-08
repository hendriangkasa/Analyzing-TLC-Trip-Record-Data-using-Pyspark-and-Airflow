from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func
from pyspark.sql.functions import col, unix_timestamp, from_unixtime
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import max
import contextlib
import sys

spark = SparkSession.builder.appName("TaxiTripAnalysis").getOrCreate()

# Load the Parquet data
df = spark.read.parquet("/home/hendri/airflow/dataset/fhv_tripdata_2021-02.parquet")

# Task 1 : Count the number of taxi trips on February 15 
feb_15_trips = df.filter(col("pickup_datetime").contains("2021-02-15"))
trip_count = feb_15_trips.count()
print(f"1. Number of taxi trips on February 15: {trip_count}")
    
    
# Task 2: Find the longest trip for each day
longestTripEachDay = df
longestTripEachDay = longestTripEachDay.withColumn("dropoff_time", unix_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss").cast(IntegerType()))
longestTripEachDay = longestTripEachDay.withColumn("pickup_time", unix_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss").cast(IntegerType()))

# Create a column for pickup date instead of datetime
longestTripEachDay = longestTripEachDay.withColumn("pickup_date", from_unixtime(col("pickup_time"), "yyyy-MM-dd"))

# Find the trip duration each trip
longestTripEachDay = longestTripEachDay.withColumn("trip_duration_seconds", col("dropoff_time") - col("pickup_time"))
longestTripEachDay = longestTripEachDay.withColumn("trip_duration_minutes", col("trip_duration_seconds")/60)
longestTripEachDay = longestTripEachDay.withColumn("trip_duration_hours", col("trip_duration_seconds")/3600)

longestTrips = longestTripEachDay.groupBy("pickup_date") \
	.agg(max("trip_duration_seconds").alias("longest_trip_duration (in seconds)"), \
	func.round(func.max("trip_duration_minutes"),2).alias("longest_trip_duration (in minutes)"), \
	func.round(func.max("trip_duration_hours"),2).alias("longest_trip_duration (in hours)") 
	) \
	.orderBy("pickup_date")

print("2. Longest Trip on Each Day\n", longestTrips.toPandas())
print("Note: I have checked the data and there is indeed a data with trip duration 77 days")


# Task 3: Top 5 Most Frequent "dispatching_base_num"
top5DBN = df.groupBy("dispatching_base_num") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(5) \

print("3. Top 5 Most Frequent 'dispatching_base_num':")
top5DBN.show()


# Task 4: Top 5 Most Common Location Pairs (PUlocationID and DOlocationID)
# Null Values will be excluded as there are too many null in the locationID columns
top_location_pairs = df.filter(col("PUlocationID").isNotNull() & col("DOlocationID").isNotNull()) \
    .groupBy("PUlocationID", "DOlocationID") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(5) \

print("4. Top 5 Most Common Location Pairs:")
top_location_pairs.show()



# Bonus section: convert 4 tasks into 4 output .txt files
with open("/home/hendri/airflow/output_taxi_trip_analysis/output_1.txt", "w") as f:
        f.write(f"Number of taxi trips on February 15: {trip_count}")
        
with open("/home/hendri/airflow/output_taxi_trip_analysis/output_2.txt", "w") as f:
        f.write("2. Longest Trip on Each Day\n")
        f.write(longestTrips.toPandas().to_string())
        f.write("\nNote: I have checked the data and there is indeed a data with trip duration 77 days")
        
with open("/home/hendri/airflow/output_taxi_trip_analysis/output_3.txt", "w") as f:
   	f.write("3. Top 5 Most Frequent 'dispatching_base_num':\n")
   	with contextlib.redirect_stdout(f):
        	top5DBN.show()
        	
with open("/home/hendri/airflow/output_taxi_trip_analysis/output_4.txt", "w") as f:
	f.write("4. Top 5 Most Common Location Pairs:\n")
	with contextlib.redirect_stdout(f):
        	top_location_pairs.show()
