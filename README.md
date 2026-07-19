# CEI Final Project - Medallion Architecture

## Project Overview
This repository contains the implementation of a robust data pipeline utilizing the Medallion Architecture within Databricks. The project is designed to incrementally process data from its raw state to a highly refined state, making it ready for advanced SQL analytics and business intelligence.

## Architecture Layers
This project relies on a multi-hop Delta Lake architecture:
* **Bronze Layer:** The landing zone for raw, unprocessed data ingested directly from the source. 
* **Silver Layer:** The enterprise view. Data is cleansed, filtered, transformed, and validated.
* **Gold Layer:** The reporting layer. Data is aggregated and business-level metrics are calculated using complex SQL queries (Window functions, CTEs, multi-table joins).

## Tech Stack
* **Platform:** Databricks
* **Storage:** Delta Lake
* **Languages:** SQL, PySpark

## Author
* Naman Verma
