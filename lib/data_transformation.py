from pyspark.sql.functions import current_timestamp, regexp_replace, col, when, coalesce, lit, length
import os

# A. Clean customers.csv

customers_df_ingested = customers_df_renamed.withColumn("ingest_date", current_timestamp())

customers_df_distinct = customers_df_ingested.distinct()

customers_df_distinct.createOrReplaceTempView("customers")

customers_income_filtered = spark.sql("select * from customers where annual_income is not null")

customers_income_filtered.createOrReplaceTempView("customers")

customers_emplength_cleaned = customers_income_filtered.withColumn("emp_length", regexp_replace(col("emp_length"), "[^0-9]",""))

customers_emplength_int = customers_emplength_cleaned.withColumn("emp_length", when(col("emp_length") == "", None).otherwise(col("emp_length")).cast('int'))

customers_emplength_int.createOrReplaceTempView("customers")

avg_emp_length = spark.sql("select floor(avg(emp_length)) as avg_emp_length from customers").collect()

avg_emp_duration = avg_emp_length[0][0]

customers_emplength_replaced = customers_emplength_int.na.fill(avg_emp_duration, subset=['emp_length'])

customers_emplength_replaced.createOrReplaceTempView("customers")

customers_state_cleaned = customers_emplength_replaced.withColumn(
    "address_state", when(length(col("address_state"))> 2, "NA")
    .otherwise(col("address_state")))

customers_state_cleaned.write \
.option("header", True) \
.format("csv") \
.mode("overwrite") \
.option("path", "../data/cleaned/customers.csv") \
.save()