#! /usr/bin/env python3

# Code used for the blog post comparing relative performance gain when using partitioning and / or clustering
# link to the blog post - TBD
#Python script to run  queries on BigQuery for a set of different input values to measure and compare
#bytes scanned and query latency across a set of partitioning and clustering schemes

import random

from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# Get a list of values for tstamp and cust identifiers

cust_id = []
dates = []

# Create an ARRAY of Cust Identifier
# Change LIMIT to increase or decrease number of runs for each queries

cust_id_array_query = """
    SELECT cust_identifier FROM bq_demo_ds.cust_ids LIMIT 10;
"""

query_job = client.query(cust_id_array_query)
for row in query_job:
    cust_id.append(row.cust_identifier)

# Create an ARRAY of DATE

dates_array_query = """
    SELECT dates from bq_demo_ds.tstamps LIMIT 10;
"""

query_job = client.query(dates_array_query)
for row in query_job:
    dates.append(row.dates)

# Define SQL queries to test all 9 scenarios

c1q1 = """
    SELECT /*CASE#1 QUERY#1 */ cust_identifier , tstamp, (attr1 + attr2 -attr3) some_random_calc
    FROM bq_demo_ds.demo_table_10c_date_part_cust_cluster
    WHERE cust_identifier = @cust_identifier;
"""

c1q2 = """
    SELECT /*CASE#1 QUERY#2 */ cust_identifier , tstamp, (attr1 + attr2 -attr3) some_random_calc
    FROM bq_demo_ds.demo_table_10c_cust_part_date_cluster
    WHERE cust_identifier_bucket = ABS(MOD(FARM_FINGERPRINT(@cust_identifier1),4000))
    AND cust_identifier = @cust_identifier2;
"""

c1q3 = """
    SELECT /*CASE#1 QUERY#3 */ cust_identifier , tstamp, (attr1 + attr2 -attr3) some_random_calc
    FROM bq_demo_ds.demo_table_10c_date_part_no_cluster
    WHERE cust_identifier = @cust_identifier;
"""

c2q1 = """
    SELECT /*CASE#2 QUERY#1 */ cust_identifier , tstamp, (attr1 + attr2 -attr3) some_random_calc
    FROM bq_demo_ds.demo_table_10c_date_part_cust_cluster
    WHERE EXTRACT(DATE FROM tstamp) = @dt;
"""

c2q2 = """
    SELECT /*CASE#2 QUERY#2 */ cust_identifier , tstamp, (attr1 + attr2 -attr3) some_random_calc
    FROM bq_demo_ds.demo_table_10c_cust_part_date_cluster
    WHERE EXTRACT(DATE FROM tstamp) = @dt;
"""

c2q3 = """
    SELECT /*CASE#2 QUERY#3 */ cust_identifier , tstamp, (attr1 + attr2 -attr3) some_random_calc
    FROM bq_demo_ds.demo_table_10c_date_part_no_cluster
    WHERE EXTRACT(DATE FROM tstamp) = @dt;
"""

c3q1 = """
    SELECT /*CASE#3 QUERY#1*/ cust_identifier , tstamp, (attr1 + attr2 -attr3) some_random_calc
    FROM bq_demo_ds.demo_table_10c_date_part_cust_cluster
    WHERE EXTRACT(DATE FROM tstamp) = @dt
    AND  cust_identifier = @cust_identifier;
"""

c3q2 = """
    SELECT /*CASE#3 QUERY#2*/ cust_identifier , tstamp, (attr1 + attr2 -attr3) some_random_calc
    FROM bq_demo_ds.demo_table_10c_cust_part_date_cluster
    WHERE cust_identifier_bucket = ABS(MOD(FARM_FINGERPRINT(@cust_identifier1),4000))
    AND cust_identifier = @cust_identifier2
    AND EXTRACT(DATE FROM tstamp) = @dt;
"""

c3q3 = """
    SELECT /*CASE#3 QUERY#3*/ cust_identifier , tstamp, (attr1 + attr2 -attr3) some_random_calc
    FROM bq_demo_ds.demo_table_10c_date_part_no_cluster
    WHERE EXTRACT(DATE FROM tstamp) = @dt
    AND  cust_identifier = @cust_identifier;
"""

# Run customer search queries
# Include a job_id_prefix to help with retrieving performance data from INFORMATION_SCHEMA

for cust in cust_id:
    job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                    bigquery.ScalarQueryParameter("cust_identifier", "STRING", cust)
            ]
    )

    query_job = client.query(c1q1, job_config=job_config, job_id_prefix= "cpperf_c1q1_")  # Make an API request.
    results = query_job.result()

    print("Query CASE#1 QUERY#1 - Got {} rows for cust_identifier {} .".format(results.total_rows, cust))

    job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                    bigquery.ScalarQueryParameter("cust_identifier1", "STRING", cust),
                    bigquery.ScalarQueryParameter("cust_identifier2", "STRING", cust)
            ]
    )
    query_job = client.query(c1q2, job_config=job_config, job_id_prefix= "cpperf_c1q2_")  # Make an API request.
    results = query_job.result()

    print("Query CASE#1 QUERY#2 - Got {} rows for cust_identifier {} .".format(results.total_rows, cust))

# Run date search queries

for dt in dates:
    job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                    bigquery.ScalarQueryParameter("dt", "DATE", dt)
            ]
    )

    query_job = client.query(c2q1, job_config=job_config, job_id_prefix= "cpperf_c2q1_")  # Make an API request.
    results = query_job.result()

    print("Query CASE#2 QUERY#1 - Got {} rows for TSTAMP {} .".format(results.total_rows, dt))

    query_job = client.query(c2q2, job_config=job_config, job_id_prefix= "cpperf_c2q2_")  # Make an API request.
    results = query_job.result()

    print("Query CASE#2 QUERY#2 - Got {} rows for TSTAMP {} .".format(results.total_rows, dt))

    query_job = client.query(c2q3, job_config=job_config, job_id_prefix="cpperf_c2q3_")  # Make an API request.
    results = query_job.result()

    print("Query CASE#2 QUERY#3 - Got {} rows for TSTAMP {} .".format(results.total_rows, dt))

# Run queries which filter on customer identifier and date
for i in range(10):

    # Pick a tstamp and cust indentifier from the arrays
    dt = random.choice(dates)
    cust_identifier = random.choice(cust_id)

    job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                    bigquery.ScalarQueryParameter("dt", "DATE", dt),
                    bigquery.ScalarQueryParameter("cust_identifier", "STRING", cust_identifier)
            ]
    )

    query_job = client.query(c3q1, job_config=job_config, job_id_prefix= "cpperf_c3q1_")  # Make an API request.
    results = query_job.result()

    print("Query CASE#3 QUERY#1 - Got {} rows for Cust_ID {} and TSTAMP {} .".format(results.total_rows, cust_identifier, dt))

    query_job = client.query(c3q3, job_config=job_config, job_id_prefix= "cpperf_c3q3_")  # Make an API request.
    results = query_job.result()

    print("Query CASE#3 QUERY#3 - Got {} rows for Cust_ID {} and TSTAMP {} .".format(results.total_rows, cust_identifier, dt))

    job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                    bigquery.ScalarQueryParameter("cust_identifier1", "STRING", cust_identifier),
                    bigquery.ScalarQueryParameter("cust_identifier2", "STRING", cust_identifier),
                    bigquery.ScalarQueryParameter("dt", "DATE", dt)
            ]
    )

    query_job = client.query(c3q2, job_config=job_config, job_id_prefix= "cpperf_c3q2_")  # Make an API request.
    results = query_job.result()

    print("Query CASE#3 QUERY#2 - Got {} rows for Cust_ID {} and TSTAMP {} .".format(results.total_rows, cust_identifier, dt))

# Run query which is searching a customer identifier on a date partitioned table without date filtering.
# Kept this as separate block as it will do a full table scan and hence may not want to run it as many time as some of the otehr queries.

for i in range(10):
    cust_identifier = random.choice(cust_id)

    job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                    bigquery.ScalarQueryParameter("cust_identifier", "STRING", cust_identifier)
            ]
    )

    query_job = client.query(c1q3, job_config=job_config, job_id_prefix= "cpperf_c1q3_")  # Make an API request.
    results = query_job.result()

    print("Query CASE#1 QUERY#3 - Got {} rows for cust_identifier {} .".format(results.total_rows, cust_identifier))
