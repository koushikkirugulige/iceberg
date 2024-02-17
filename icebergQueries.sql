-- use this command to run the spark-sql shell
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions     --conf spark.sql.catalog.spark_catalog=iceberg     --conf spark.sql.catalog.spark_catalog.type=hive     --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog     --conf spark.sql.catalog.local.type=hadoop     --conf spark.sql.catalog.local.warehouse=$PWD/warehouse     --conf spark.sql.defaultCatalog=local

----------------------------------------------------------------

CREATE TABLE prod.db.table.branch_audit
(
id bigint,
val string
)
USING iceberg;


INSERT INTO prod.db.table.branch_audit VALUES (1, 'a'), (2, 'b');

UPDATE prod.db.table.branch_audit AS t1
SET val = 'c';

CREATE TABLE prod.my_app.logs (
uuid string NOT NULL,
level string NOT NULL,
ts timestamp NOT NULL,
message string)
USING iceberg
PARTITIONED BY (level, hours(ts));


DELETE FROM prod.db.table.branch_audit WHERE id = 2;

----------------------------------------------------------------


CREATE OR REPLACE TABLE taxi_db.taxi_tbl (
ride_id STRING,
ride_distance DOUBLE,
ride_fare DOUBLE,
city STRING
)
USING iceberg
PARTITIONED BY (city)


INSERT INTO taxi_db.taxi_tbl (ride_id, ride_distance, ride_fare, city)
VALUES ('ride123', 5.7, 23.5, 'Bengaluru'),
('ride456', 8.2, 31.0, 'Mumbai'),
('ride789', 3.1, 15.8, 'Chennai');

--Update in iceberg
UPDATE taxi_db.taxi_tbl
SET ride_fare = ride_fare * 1.1  -- Increase fare by 10%
WHERE city = 'Bengaluru';

UPDATE taxi_db.taxi_tbl
set city = 'Bengaluru' where city = 'Chennai';

---------------------------
---Merge
---------------------------

CREATE OR REPLACE TABLE mergedb.master_tbl (
product_id STRING,
product_name STRING,
price DOUBLE,
category STRING
)
USING iceberg
PARTITIONED BY (category)
;
-------------
CREATE OR REPLACE TABLE mergedb.delta_tbl (
product_id STRING,
product_name STRING,
price DOUBLE,
category STRING,
stock INT
)
USING iceberg
PARTITIONED BY (category)
;

-- Create table done
---------------------------------------
-- Master Table
INSERT INTO mergedb.master_tbl (product_id, product_name, price, category)
VALUES ('p123', 'Headphones', 49.99, 'Electronics'),
('p456', 'T-shirt', 19.99, 'Clothing'),
('p789', 'Laptop', 799.99, 'Electronics');

-- Delta Table
INSERT INTO mergedb.delta_tbl (product_id, product_name, price, category, stock)
VALUES ('p123', 'Headphones', 49.99, 'Electronics', 100),
('p456', 'T-shirt', 22.99, 'Clothing', 50),
('p901', 'Mouse', 29.99, 'Electronics', 20);


MERGE INTO mergedb.master_tbl USING mergedb.delta_tbl
ON master_tbl.product_id = delta_tbl.product_id
WHEN MATCHED AND master_tbl.price <> delta_tbl.price THEN
UPDATE SET master_tbl.price = delta_tbl.price
WHEN NOT MATCHED THEN INSERT *; 

delta.product_id,delta.product_name,delta.price,delta.category;



