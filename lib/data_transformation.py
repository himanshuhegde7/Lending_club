from pyspark.sql.functions import current_timestamp, regexp_replace, col, when, coalesce, lit, length
import os

# A. Clean customers.csv
def clean_customers(spark, customers_df):
    # Unnecessary?: customers_df = customers_df.withColumnRenamed("member_id", "customer_id")
    # Remove duplicate records
    customers_df_distinct = customers_df.distinct()
    
    # Filter out records with null or empty annual_income
    customers_df_distinct.createOrReplaceTempView("customers")
    customers_income_filtered = spark.sql("select * from customers where annual_income is not null")

    # Extract numeric part from emp_length and convert it to integer. Eg: "9 years" to 9, n/a to null
    customers_emplength_cleaned = customers_income_filtered.withColumn(
        "emp_length", regexp_replace(col("emp_length"), "[^0-9]",""))
    customers_emplength_int = customers_emplength_cleaned.withColumn(
        "emp_length", when(col("emp_length") == "", None).otherwise(col("emp_length")).cast('int'))

    # Replace nulls in emp_length with average of the column
    customers_emplength_int.createOrReplaceTempView("customers")
    avg_emp_length = spark.sql("select floor(avg(emp_length)) as avg_emp_length from customers").collect()
    avg_emp_duration = avg_emp_length[0][0]
    customers_emplength_replaced = customers_emplength_int.na.fill(avg_emp_duration, subset=['emp_length'])
    
    # Clean address_state: if length > 2, replace with 'NA'
    customers_state_cleaned = customers_emplength_replaced.withColumn(
        "address_state", when(length(col("address_state"))> 2, "NA")
        .otherwise(col("address_state")))
    
    return customers_state_cleaned

    

def clean_loans(spark, loans_df):
    return

def clean_repayments(spark, repayments_df):
    return

def clean_delinquencies(spark, defaulters_df):
    return

def final_cleaning(spark):
    return
