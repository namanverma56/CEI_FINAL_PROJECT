from pyspark.sql.functions import col, count, avg, when, round, max

# ==========================================
# 1. SETUP (Load from Silver)
# ==========================================
spark.sql('CREATE DATABASE IF NOT EXISTS service_track_gold')

# Read the clean, joined data from the Silver layer
df_silver = spark.table("service_track_silver.enriched_service_jobs")

#time taken for job
df_gold_delays = df_silver.withColumn("delivery_status",when(col("completed_date")>col("promised_date"), "Delayed")
    .when(col("completed_date").isNull(),"In Progress/Pending").otherwise("On Time"))

#Technician Performanc
df_gold_tech_perf = df_silver.filter(col("job_status")=="Completed").groupBy("technician_id", "technician_name")\
    .agg(count("job_id").alias("total_jobs_completed"),round(avg("repair_duration_days"), 2).alias("avg_repair_days")
    ).orderBy("avg_repair_days")


#Repeat Customer Detection
df_gold_repeat_customers = df_silver.groupBy("customer_id", "customer_name").agg(count("job_id").alias("total_visits"))\
    .filter(col("total_visits") > 1).orderBy(col("total_visits").desc())


#Device & Fault Trend Analysis
df_gold_device_trends = df_silver.groupBy("brand", "device_type", "issue_type") \
    .agg(count("job_id").alias("job_volume")) \
    .orderBy(col("job_volume").desc())

#Customer Visit History
df_latest_dates = df_silver.groupBy("customer_id") \
    .agg(max("received_date").alias("latest_date"))


df_gold_latest_visit = df_silver.alias("main").join(
    df_latest_dates.alias("latest"),
    (col("main.customer_id") == col("latest.customer_id")) & 
    (col("main.received_date") == col("latest.latest_date")),
    "inner").select("main.*")

#load the data
tables= {
    "gold_delay_analysis": df_gold_delays,
    "gold_technician_performance": df_gold_tech_perf,
    "gold_repeat_customers": df_gold_repeat_customers,
    "gold_device_trends": df_gold_device_trends,
    "gold_latest_customer_visit": df_gold_latest_visit
}

for table_name,df in tables.items():
    df.write.format("delta").mode("overwrite")\
        .saveAsTable(f"service_track_gold.{table_name}")
    
    print(f"Successfully saved: {table_name}")