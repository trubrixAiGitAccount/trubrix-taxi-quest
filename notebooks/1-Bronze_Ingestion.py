# MAGIC %md
# ## Phase 2: Bronze Ingestion
#
# This notebook documents the ingestion of the raw NYC Taxi dataset into the Bronze layer.
# The data was uploaded in its original, untouched Parquet format.
#
# ### Steps Taken
# 1. **Downloaded Data:** The raw Parquet file was downloaded from the official NYC TLC website.
# 2. **Uploaded to Databricks:** The file was uploaded into the `bronze_data` schema.
#
# ### Bronze Layer Path
# The raw data is now located at the following path:
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
