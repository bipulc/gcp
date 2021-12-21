WITH jqr as (
SELECT
  SUBSTR(job_id,8,4) as job_group,
  job_id,
  start_time,
  end_time,
  total_bytes_processed,
  total_bytes_billed,
  TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS elasped_millisecond
FROM
  `region-europe-west1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  creation_time BETWEEN TIMESTAMP('2021-12-19 13:41:00') AND TIMESTAMP('2021-12-19 14:41:00')
  AND job_type = "QUERY"
  AND job_id LIKE 'cpperf%'
  AND end_time BETWEEN TIMESTAMP('2021-12-19 13:41:00') AND TIMESTAMP('2021-12-19 14:41:00')
  AND project_id = 'data-analytics-bk')
SELECT job_group, 
       count(*) num_exec, 
       ceiling(avg(total_bytes_processed)/1024/1024) avg_mb_processed_per_run, 
       ceiling(avg(total_bytes_billed)/1024/1024) avg_mb_billed_per_run,
       ceiling(avg(elasped_millisecond)) avg_millisecond_per_run
FROM jqr GROUP BY job_group ORDER by job_group;
