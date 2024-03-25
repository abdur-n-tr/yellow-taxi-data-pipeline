-- https://docs.aws.amazon.com/redshift/latest/dg/c_Compression_encodings.html
-- https://docs.aws.amazon.com/redshift/latest/dg/Examples__compression_encodings_in_CREATE_TABLE_statements.html
-- https://docs.aws.amazon.com/redshift/latest/dg/t_Verifying_data_compression.html
-- https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html
-- https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE.html


-- 1- Use the smallest possible column size. Don't make it a practice to use the maximum column size for convenience.
-- Instead, consider the largest values you are likely to store in your columns and size them accordingly. 
-- For instance, a CHAR column for storing chemical symbols from the periodic table would only need to be CHAR(2). 

-- 2- If table size is small, use the default compression which redshift automatically applies. If table size is large,
-- try out different encodings for every column and choose the best one. Also, run `analyze` on table to get the best
-- suggested econding and reduction percentage from redshift.

-- 3- Initially, set the distribution style ALL for dimension tables and distribution key for fact table on the FK.
-- Foreign key should be choosen based on the frequency of join and also the size of the dimension table after filter.
-- Now, also set the same primary key as distribution key in the dimension table. We are doing this since we can not
-- set multiple distribution keys in the fact table otherwise we could create multiple dist key in fact table against 
-- every dimension table primary key as distribution key. After than analyze the query plan, and optimize further.
-- Refer to the official docs for optimization: https://docs.aws.amazon.com/redshift/latest/dg/t_explain_plan_example.htmlssss

-- 4- A compound key is made up of all of the columns listed in the sort key definition, in the order they are listed. 
-- A compound sort key is most useful when a query's filter applies conditions, such as filters and joins, that use a 
-- prefix of the sort keys. The performance benefits of compound sorting decrease when queries depend only on secondary
-- sort columns, without referencing the primary columns. COMPOUND is the default sort type.
-- For more detail info on sort key: https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html


DROP TABLE location_dim;

CREATE TABLE location_dim(
    location_id SMALLINT,
    borough VARCHAR(20),
    zone VARCHAR(50),
    service_zone VARCHAR(15),
    PRIMARY KEY(location_id)
);

COPY location_dim 
FROM 's3://yellow-taxi-pipeline-data/dimension_data/location_dim.csv'
REGION 'us-east-1' 
IAM_ROLE 'arn:aws:iam::471112663332:role/role-s3-to-redshift-vice-versa'
IGNOREHEADER 1
DELIMITER ','
FORMAT CSV
;

DELETE FROM location_dim;

SELECT count(*) FROM public.location_dim;

select * from stl_load_errors;

analyze compression public.location_dim;

select "column", type, encoding, distkey, sortkey, "notnull" 
from pg_table_def
where tablename = 'location_dim';

select * from SVV_TABLE_INFO;

select * from SYS_QUERY_DETAIL;

select * from SVV_ALTER_TABLE_RECOMMENDATIONS;

show search_path;


SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public' -- Replace 'public' with the schema name of your table
  AND table_name = 'payment_type_dim';


ALTER TABLE datetime_dim ALTER DISTSTYLE KEY DISTKEY datetime_id; 
ALTER TABLE rides_fact ALTER DISTSTYLE EVEN;