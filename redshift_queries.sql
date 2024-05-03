-- Create Schema
CREATE SCHEMA airlines;

-- Create Dimension Table
CREATE TABLE airlines.dim_airports (
    airport_id BIGINT,
    city VARCHAR(100),
    state VARCHAR(100),
    name VARCHAR(200)
);

-- Create Fact Table
CREATE TABLE airlines.fact_daily_flights (
    carrier VARCHAR(10),
    dep_airport VARCHAR(200),
    arr_airport VARCHAR(200),
    dep_city VARCHAR(100),
    arr_city VARCHAR(100),
    dep_state VARCHAR(100),
    arr_state VARCHAR(100),
    dep_delay BIGINT,
    arr_delay BIGINT
);

-- Load data from S3 to Dimension Table (dim_airports); Use S3 path, role with S3, Redshift access
COPY airlines.dim_airports
FROM 's3://pf-airlines-data/dim/airports/airports.csv' 
IAM_ROLE 'arn:aws:iam::654654491149:role/redshift-role'
DELIMITER ','
IGNOREHEADER 1
REGION 'us-east-1';

--------------

