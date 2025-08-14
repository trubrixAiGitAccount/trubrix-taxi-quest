# Set the configuration to allow overwriting messy directories with a clean Delta table
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# Import necessary functions from PySpark. We'll use these to manipulate data.
from pyspark.sql.functions import col

# --- 1. DEFINE FILE PATHS ---
# We will read the raw data directly from the source on the internet.
bronze_file_path = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

# Define the path where we will save our clean Silver Delta table.
silver_table_path = "/FileStore/trubrix-taxi-quest/silver/nyc_taxi_clean"


# --- 2. READ THE RAW BRONZE DATA ---
print("Reading raw data from Bronze layer...")
df_bronze = spark.read.load(bronze_file_path)


# --- 3. CLEAN AND TRANSFORM THE DATA ---
# Let's select only the columns we care about for our analysis.
df_selected = df_bronze.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount"
)

# Filter out bad data to improve data quality.
df_filtered = df_selected.filter(
    (col("trip_distance") > 0) & 
    (col("fare_amount") > 0)
)

# Let's assign our final, clean DataFrame to a new variable.
df_silver = df_filtered
print("Data cleaning and transformation complete.")


# --- 4. WRITE THE CLEAN DATA TO THE SILVER LAYER ---
# We save our clean DataFrame as a Delta table.
print(f"Writing clean data to Silver Delta table at: {silver_table_path}")
df_silver
