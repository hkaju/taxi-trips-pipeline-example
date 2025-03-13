-- Set up pg_partman for automated partitioning
CREATE SCHEMA partman;
CREATE EXTENSION pg_partman SCHEMA partman;

CREATE SCHEMA pipeline;

-- Create a staging table template.
-- The schema should have as few constraints as possible to
-- facilitate COPYing CSV data into the table without errors.
-- Staged data will be cleaned, converted and moved into the partitioned
-- main table (pipeline.trips) as a separate step.
CREATE UNLOGGED TABLE pipeline.trips_staging_template(
    trip_id VARCHAR,
    taxi_id VARCHAR,
    trip_start VARCHAR,
    trip_end VARCHAR,
    trip_seconds VARCHAR,
    trip_miles VARCHAR,
    pickup_census_tract VARCHAR,
    dropoff_census_tract VARCHAR,
    pickup_community_area VARCHAR,
    dropoff_community_area VARCHAR,
    fare VARCHAR,
    tips VARCHAR,
    tolls VARCHAR,
    extras VARCHAR,
    trip_total VARCHAR,
    payment_type VARCHAR,
    company VARCHAR,
    pickup_centroid_latitude VARCHAR,
    pickup_centroid_longitude VARCHAR,
    pickup_centroid_location VARCHAR,
    dropoff_centroid_latitude VARCHAR,
    dropoff_centroid_longitude VARCHAR,
    dropoff_centroid_location VARCHAR,
    source VARCHAR
);

-- Create a table for data that we remove during cleaning
CREATE UNLOGGED TABLE pipeline.bad_trips (LIKE pipeline.trips_staging_template);

-- Create the main data table.
-- This should have all the constraints we need to ensure data quality.
CREATE TABLE pipeline.trips(
    trip_id VARCHAR NOT NULL,
    taxi_id VARCHAR NOT NULL,
    trip_start TIMESTAMP NOT NULL,
    trip_end TIMESTAMP NOT NULL,
    trip_seconds INTEGER,
    trip_miles FLOAT,
    pickup_census_tract BIGINT,
    dropoff_census_tract BIGINT,
    pickup_community_area INT,
    dropoff_community_area INT,
    fare FLOAT,
    tips FLOAT,
    tolls FLOAT,
    extras FLOAT,
    trip_total FLOAT,
    payment_type VARCHAR,
    company VARCHAR,
    pickup_centroid_latitude FLOAT,
    pickup_centroid_longitude FLOAT,
    pickup_centroid_location VARCHAR,
    dropoff_centroid_latitude FLOAT,
    dropoff_centroid_longitude FLOAT,
    dropoff_centroid_location VARCHAR
) PARTITION BY RANGE (trip_start);

-- Create indexes on columns we expect to use more heavily
CREATE INDEX ON pipeline.trips (taxi_id);
CREATE INDEX ON pipeline.trips (trip_start);
CREATE INDEX ON pipeline.trips (trip_end);

-- Create a template table for new partitions
CREATE TABLE pipeline.trips_template (LIKE pipeline.trips);
-- Primary keys should be added to the template table, not parent
-- https://github.com/pgpartman/pg_partman/blob/development/doc/pg_partman.md#child-table-property-inheritance
ALTER TABLE pipeline.trips_template ADD PRIMARY KEY (trip_id);

-- Set up automated partitioning for pipeline.trips
SELECT partman.create_parent(
    p_parent_table := 'pipeline.trips'
    -- Partition by trip_start
    , p_control := 'trip_start'
    , p_interval := '1 week'
    , p_template_table := 'pipeline.trips_template'
    , p_type := 'native'
    -- Start precreating partitions from this date until CURRENT_TIMESTAMP.
    -- Slightly suboptimal since it results in a large number of unused partitions
    -- if this is far from CURRENT_TIMESTAMP but it avoids extra manual or scripting work.
    , p_start_partition := '2024-01-01'
);