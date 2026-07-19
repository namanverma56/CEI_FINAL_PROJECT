
-- SERVICETRACK ANALYTICS QUERIES
-- Database: service_track_gold


--clasify jobs based on time based completion or pending
SELECT job_id,customer_name,received_date,promised_date,completed_date,
    CASE 
        WHEN completed_date > promised_date THEN 'Delayed'
        WHEN completed_date IS NULL THEN 'In Progress / Pending'
        ELSE 'On Time'
    END AS delivery_status
FROM service_track_silver.enriched_service_jobs;


--repeated customed detect
SELECT customer_id,customer_name,COUNT(job_id) as total_visits
FROM service_track_silver.enriched_service_jobs
GROUP BY customer_id,customer_name
HAVING COUNT(job_id)>1
ORDER BY total_visits DESC;

--technician s average repair time
SELECT job_id,technician_name,issue_type,
    DATEDIFF(completed_date, received_date) as repair_duration_days,
    ROUND(
        AVG(DATEDIFF(completed_date, received_date)) OVER(PARTITION BY technician_id), 2
    )AS avg_tech_repair_time
FROM service_track_silver.enriched_service_jobs
WHERE job_status='Completed'
ORDER BY technician_id, repair_duration_days;

---most recent jobs percustomer
WITH RankedCustomerVisits AS (
    SELECT customer_id,customer_name,job_id,issue_type,received_date,
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY received_date DESC) as visit_rank
    FROM service_track_silver.enriched_service_jobs
)
SELECT 
    customer_id,customer_name,job_id AS most_recent_job_id,issue_type,received_date AS last_visit_date
FROM RankedCustomerVisits
WHERE visit_rank = 1;



SELECT sj.job_id,sj.job_status,c.customer_name,c.phone_number,d.brand,d.device_type,sj.issue_type,sj.technician_name
FROM service_track_bronze.service_jobs sj
INNER JOIN service_track_bronze.customers c 
    ON sj.customer_id = c.customer_id
INNER JOIN service_track_bronze.devices d 
    ON sj.device_id = d.device_id;