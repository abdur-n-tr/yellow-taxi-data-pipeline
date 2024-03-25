DROP TABLE location_dim;

CREATE TABLE location_dim(
    location_id INTEGER NOT NULL,
    borough VARCHAR(20),
    zone VARCHAR(50),
    service_zone VARCHAR(15),
    PRIMARY KEY(location_id)
)
DISTSTYLE ALL;

COPY location_dim 
FROM 's3://yellow-taxi-pipeline-data/dimension_data/location/location_dim.csv'
REGION 'us-east-1' 
IAM_ROLE 'arn:aws:iam::471112663332:role/role-s3-to-redshift-vice-versa'
IGNOREHEADER 1
DELIMITER ','
FORMAT CSV
;

-- ===========================================

DROP TABLE payment_type_dim;

CREATE TABLE payment_type_dim(
    payment_type_id INTEGER NOT NULL,
    payment_type_name VARCHAR(15),
    PRIMARY KEY(payment_type_id)
)
DISTSTYLE ALL;

COPY payment_type_dim 
FROM 's3://yellow-taxi-pipeline-data/dimension_data/payment_type/payment_type_dim.csv'
REGION 'us-east-1' 
IAM_ROLE 'arn:aws:iam::471112663332:role/role-s3-to-redshift-vice-versa'
IGNOREHEADER 1
DELIMITER ','
FORMAT CSV
;

-- =============================================

DROP TABLE rate_code_dim;

CREATE TABLE rate_code_dim(
    rate_code_id INTEGER NOT NULL,
    rate_code_name VARCHAR(25),
    PRIMARY KEY(rate_code_id)
)
DISTSTYLE ALL;

COPY rate_code_dim 
FROM 's3://yellow-taxi-pipeline-data/dimension_data/rate_code/rate_code_dim.csv'
REGION 'us-east-1' 
IAM_ROLE 'arn:aws:iam::471112663332:role/role-s3-to-redshift-vice-versa'
IGNOREHEADER 1
DELIMITER ','
FORMAT CSV
;

-- ============================================

DROP TABLE datetime_dim;

CREATE TABLE datetime_dim(
    datetime_id INTEGER NOT NULL DISTKEY SORTKEY,
    datetime TIMESTAMP,
    year SMALLINT,
    month SMALLINT,
    day SMALLINT,
    hour SMALLINT,
    weekday SMALLINT,
    PRIMARY KEY(datetime_id)
);

COPY datetime_dim 
FROM 's3://yellow-taxi-pipeline-data/dimension_data/datetime/datetime_dim.csv'
REGION 'us-east-1' 
IAM_ROLE 'arn:aws:iam::471112663332:role/role-s3-to-redshift-vice-versa'
IGNOREHEADER 1
DELIMITER ','
FORMAT CSV
;

-- ================== Encodings have been selected by running aws redshift analyzer ==========================

DROP TABLE rides_fact;

CREATE TABLE rides_fact(
    vendor_id INTEGER ENCODE az64,
    pickup_datetime_id INTEGER ENCODE az64 NOT NULL DISTKEY SORTKEY,
    dropoff_datetime_id INTEGER ENCODE az64 NOT NULL,
    pickup_location_id INTEGER ENCODE az64 NOT NULL,
    dropoff_location_id INTEGER ENCODE az64 NOT NULL,
    rate_code_id INTEGER ENCODE az64 NOT NULL,
    payment_type_id INTEGER ENCODE az64 NOT NULL,
    airport_fee REAL ENCODE zstd,
    congestion_surcharge REAL ENCODE zstd,
    total_amount REAL ENCODE zstd,
    improvement_surcharge REAL ENCODE zstd,
    tolls_amount REAL ENCODE zstd, 
    tip_amount REAL ENCODE zstd,
    mta_tax REAL ENCODE zstd,
    extra REAL ENCODE zstd,
    fare_amount REAL ENCODE bytedict,
    trip_distance REAL ENCODE zstd,
    passenger_count INTEGER ENCODE az64,
    FOREIGN KEY (pickup_datetime_id) REFERENCES datetime_dim (datetime_id),
    FOREIGN KEY (dropoff_datetime_id) REFERENCES datetime_dim (datetime_id),
    FOREIGN KEY (pickup_location_id) REFERENCES location_dim (location_id),
    FOREIGN KEY (dropoff_location_id) REFERENCES location_dim (location_id),
    FOREIGN KEY (rate_code_id) REFERENCES rate_code_dim (rate_code_id),
    FOREIGN KEY (payment_type_id) REFERENCES payment_type_dim (payment_type_id)
);
