CREATE TABLE `data-analytics-bk.bq_demo_ds.demo_table_10c`
(
  cust_identifier STRING,
  tstamp TIMESTAMP,
  country_code STRING,
  attr1 INT64,
  attr2 INT64,
  attr3 INT64,
  attr4 INT64,
  attr5 INT64,
  attr6 INT64,
  attr7 INT64,
  attr8 INT64,
  attr9 INT64,
  attr10 INT64
)
PARTITION BY DATE(tstamp)
CLUSTER BY cust_identifier;

CREATE TABLE bq_demo_ds.demo_table_10c_cust_part_date_cluster
PARTITION BY
   RANGE_BUCKET(cust_identifier_bucket, GENERATE_ARRAY(1,4000,1))
CLUSTER BY tstamp
as
SELECT ABS(MOD(FARM_FINGERPRINT(a.cust_identifier),4000)) cust_identifier_bucket, a.*
FROM `data-analytics-bk.bq_demo_ds.demo_table_10c` a;

CREATE TABLE bq_demo_ds.demo_table_10c_date_part_cust_cluster
PARTITION BY DATE(tstamp)
CLUSTER BY cust_identifier
as
SELECT *
FROM `data-analytics-bk.bq_demo_ds.demo_table_10c`;

