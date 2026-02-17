/* @bruin
name: staging.trips
type: duckdb.sql
description: |
  Transforms and cleans raw trip data from ingestion layer.
  Normalizes column names, computes duration, joins with payment lookup,
  and filters out obvious data quality issues.

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_time
  time_granularity: timestamp

columns:
  - name: pickup_time
    type: TIMESTAMP
    description: The date and time when the meter was engaged
    primary_key: true
    nullable: false
  - name: dropoff_time
    type: TIMESTAMP
    description: The date and time when the meter was disengaged
    primary_key: true
    nullable: false
  - name: pickup_location_id
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was engaged
    primary_key: true
    nullable: false
  - name: dropoff_location_id
    type: INTEGER
    description: TLC Taxi Zone in which the taximeter was disengaged
    primary_key: true
    nullable: false
  - name: taxi_type
    type: VARCHAR
    description: Type of taxi (yellow or green)
    primary_key: true
    nullable: false
  - name: trip_distance
    type: DOUBLE
    description: The elapsed trip distance in miles reported by the taximeter
  - name: passenger_count
    type: DOUBLE
    description: The number of passengers in the vehicle
  - name: fare_amount
    type: DOUBLE
    description: The time-and-distance fare calculated by the meter
  - name: tip_amount
    type: DOUBLE
    description: Tip amount
  - name: total_amount
    type: DOUBLE
    description: The total amount charged to passengers (does not include cash tips)
    checks:
      - name: non_negative
  - name: trip_duration_seconds
    type: DOUBLE
    description: Calculated trip duration in seconds (dropoff time - pickup time)
    checks:
      - name: positive
      - name: max
        value: 28800
  - name: payment_type
    type: DOUBLE
    description: Numeric code signifying how the passenger paid for the trip
  - name: payment_description
    type: VARCHAR
    description: Human-readable description of the payment type
  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the data was extracted from the source
  - name: updated_at
    type: TIMESTAMP
    description: Timestamp when the data was last updated in staging

@bruin */

WITH normalized_trips AS (
  SELECT
    vendorid,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_time,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_time,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    pulocationid AS pickup_location_id,
    dolocationid AS dropoff_location_id,
    CAST(payment_type AS INTEGER) AS payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    taxi_type,
    extracted_at
  FROM ingestion.trips
  WHERE 1 = 1
    AND tpep_pickup_datetime BETWEEN CAST('{{ start_datetime }}' AS TIMESTAMP)
                                 AND CAST('{{ end_datetime }}' AS TIMESTAMP)
    AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND pulocationid IS NOT NULL
    AND dolocationid IS NOT NULL
    AND taxi_type IS NOT NULL
),

enriched_trips AS (
  SELECT
    nt.pickup_time,
    nt.dropoff_time,
    EXTRACT(EPOCH FROM (nt.dropoff_time - nt.pickup_time)) AS trip_duration_seconds,
    nt.pickup_location_id,
    nt.dropoff_location_id,
    nt.taxi_type,
    nt.trip_distance,
    nt.passenger_count,
    nt.fare_amount,
    nt.tip_amount,
    nt.total_amount,
    nt.payment_type,
    pmt.payment_description,
    nt.extracted_at,
    CURRENT_TIMESTAMP AS updated_at
  FROM normalized_trips AS nt
  LEFT JOIN ingestion.payment_lookup AS pmt
    ON nt.payment_type = pmt.payment_type_id
  WHERE 1 = 1
    -- positive, reasonable durations (0 < dur < 8h)
    AND EXTRACT(EPOCH FROM (nt.dropoff_time - nt.pickup_time)) > 0
    AND EXTRACT(EPOCH FROM (nt.dropoff_time - nt.pickup_time)) < 28800
    -- non-negative amounts and distances
    AND nt.total_amount >= 0
    AND nt.trip_distance >= 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      nt.pickup_time,
      nt.dropoff_time,
      nt.pickup_location_id,
      nt.dropoff_location_id,
      nt.taxi_type,
      nt.trip_distance,
      nt.passenger_count,
      nt.fare_amount,
      nt.tip_amount,
      nt.total_amount,
      nt.payment_type
    ORDER BY nt.extracted_at DESC
  ) = 1
)

SELECT
  pickup_time,
  dropoff_time,
  pickup_location_id,
  dropoff_location_id,
  taxi_type,
  trip_distance,
  passenger_count,
  fare_amount,
  tip_amount,
  total_amount,
  trip_duration_seconds,
  payment_type,
  payment_description,
  extracted_at,
  updated_at
FROM enriched_trips;