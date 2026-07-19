from pyspark.sql.functions import col, count, when, to_date, datediff

# setup data from bronze layer
spark.sql('CREATE DATABASE IF NOT EXISTS service_track_silver')

df_customers_bronze = spark.table("service_track_bronze.customers")
df_devices_bronze = spark.table("service_track_bronze.devices")
df_jobs_bronze = spark.table("service_track_bronze.service_jobs")


print("Checking initial nulls in service_jobs table:")
df_jobs_bronze.select([
    count(when(col(c).isNull(), c)).alias(c) 
    for c in df_jobs_bronze.columns
]).show()


df_jobs_clean=df_jobs_bronze\
    .dropDuplicates(["job_id"]) \
    .withColumn("technician_name", 
    when(col("technician_name").isNull() |(col("technician_name")==""),col("technician_id"))
    .otherwise(col("technician_name"))).fillna({"repair_notes":"No notes provided"}) \
    .withColumn("received_date",to_date(col("received_date"))) \
    .withColumn("promised_date", to_date(col("promised_date"))) \
    .withColumn("completed_date",to_date(col("completed_date")))

#Clean Customers
df_customers_clean = df_customers_bronze \
    .withColumn("registration_date", to_date(col("registration_date")))


df_silver_enriched = df_jobs_clean.join(df_devices_bronze,on="device_id", how="left") \
    .join(df_customers_clean,on="customer_id", how="left")\
    .withColumn("repair_duration_days",datediff(col("completed_date"), col("received_date")))

#load data to silver layer (cleaned data)
df_silver_enriched.write.format("delta") \
    .mode("overwrite").saveAsTable("service_track_silver.enriched_service_jobs")

#print final schema
print("Silver layer processing complete. Final schema:")
df_silver_enriched.printSchema()