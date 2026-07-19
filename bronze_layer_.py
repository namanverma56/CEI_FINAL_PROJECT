
#1->storing the raw data in a database 
spark.sql("CREATE DATABASE IF NOT EXISTS service_track_bronze")


# creating a dedicated bronze database
spark.sql("CREATE DATABASE IF NOT EXISTS service_track_bronze")

# Load the raw tables you uploaded via the Catalog UI
df_customers_raw = spark.table("customers")
df_devices_raw = spark.table("devices")
df_jobs_raw = spark.table("service_jobs")

#  writing the data into bronze database without any transformation
df_customers_raw.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("service_track_bronze.customers")

df_devices_raw.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("service_track_bronze.devices")

df_jobs_raw.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("service_track_bronze.service_jobs")

#bronze layer had been created
print("Bronze layer created")

