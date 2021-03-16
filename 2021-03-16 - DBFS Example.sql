-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC ## Overview
-- MAGIC 
-- MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
-- MAGIC 
-- MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/lead_lag_input.csv"
-- MAGIC file_type = "csv"
-- MAGIC 
-- MAGIC # CSV options
-- MAGIC infer_schema = "false"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create a view or table
-- MAGIC 
-- MAGIC temp_table_name = "lead_lag_input_csv"
-- MAGIC 
-- MAGIC df.createOrReplaceTempView(temp_table_name)

-- COMMAND ----------



/* Query the created temp table in a SQL cell */
DROP TABLE lead_lag_table1;
CREATE TABLE lead_lag_table1 AS
select Medicare_number
    , CAST(start_date AS DATE) AS start_date
    , CAST((CASE WHEN end_date = '2099-12-31' THEN '2021-03-31' ELSE end_date END) AS DATE) AS end_date
from(
select Medicare_number
    , from_unixtime(unix_timestamp(eff_date, 'M/d/yy H:mm'),'yyyy-MM-dd') AS start_date
    , from_unixtime(unix_timestamp(term_date, 'M/d/yy H:mm'),'yyyy-MM-dd') AS end_date
   , enroll_status
from `lead_lag_input_csv`
) z;

-- COMMAND ----------

DESC FORMATTED lead_lag_table1

-- COMMAND ----------

select Medicare_number
    , sum(gap)
from(
    select Medicare_number
       , start_date_new
       , end_date
       , NVL(datediff(start_date_new,end_date) - 1,0) AS gap
  from(
    select Medicare_number
         , lead(start_date) over(partition by Medicare_number order by start_date asc) AS start_date_new
         , start_date
         , end_date
    from lead_lag_table1
     ) a
   ) b
group by Medicare_number

-- COMMAND ----------

select a.Medicare_number
     , DATEDIFF(MAX(end_date),MIN(start_date)) + 1 AS tenure_total_withgap
from lead_lag_table1 a
GROUP BY Medicare_number


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
-- MAGIC # Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
-- MAGIC # To do so, choose your table name and uncomment the bottom line.
-- MAGIC 
-- MAGIC permanent_table_name = "lead_lag_input_csv"
-- MAGIC 
-- MAGIC # df.write.format("parquet").saveAsTable(permanent_table_name)
