# NYC Taxi Pipeline with Bruin & DuckDB

This project is an end-to-end NYC Taxi data pipeline built with Bruin and DuckDB.

## What this pipeline does

1. **Ingest NYC Taxi trips**  
   - Downloads yellow taxi Parquet files from the public NYC Taxi dataset (CloudFront / TLC source).
   - Loads them into DuckDB as a raw `ingestion.trips` table with some basic normalization.

2. **Seed lookup tables**  
   - Loads a local `payment_lookup.csv` as a DuckDB seed table (`ingestion.payment_lookup`) for payment type descriptions.

3. **Build a cleaned staging layer**  
   - Creates `staging.trips` from the raw trips.
   - Cleans timestamps, filters out invalid trips (e.g. negative distance or amount), and removes duplicates.

4. **Create monthly reports**  
   - Aggregates trips into `reports.report_trips_monthly` using Bruin’s `time_interval` incremental strategy.
   - Computes monthly metrics like total trips, total and average fare, tips, and duration by taxi type.

5. **Run everything with Bruin**  
   - All steps are defined as Bruin assets (Python, SQL, and seed) and orchestrated via the Bruin CLI on top of a local DuckDB database.
