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
    loans_df.createOrReplaceTempView("loans")
    # Filter out records with null or empty values in critical columns
    columns_with_na = ["loan_amount", "funded_amount", "loan_term_months", "interest_rate",
                       "monthly_installment", "issue_date", "loan_status", "loan_purpose", 
                       "loan_title"]
    loans_filtered_df = loans_df.na.drop(subset = columns_with_na)
    
    # Convert loan_term_months to loan_term_years by removing " months" and dividing by 12
    # Convert to integer 
    loans_filtered_df.createOrReplaceTempView("loans")
    loans_term_modified_df = loans_filtered_df.withColumn("loan_term_months", 
        (regexp_replace(col("loan_term_months"), " months", "").cast("int") / 12).cast("int")).withColumnRenamed("loan_term_months","loan_term_years")
    
    # Standardize loan_purpose: if not in valid list, replace with "other"
    loan_purposes_valid = ["debt_consolidation", "credit_card", "home_improvement", "other",
                           "major_purchase", "medical", "small_business", "car", "vacation",
                           "moving", "house", "wedding", "renewable_energy","educational"]
    loans_purpose_modified = loans_term_modified_df.withColumn("loan_purpose", 
                            when(col("loan_purpose").isin(loan_purposes_valid), col("loan_purpose")).otherwise("other"))
    
    return loans_purpose_modified

def clean_repayments(spark, repayments_df):
    
    # Filter out records with null or empty values in critical columns
    columns_to_check_na = ["total_principal_received", "total_interest_received", 
                        "total_late_fee_received", "total_payment_received"]
    repayments_filtered_df = repayments_df.na.drop(subset=columns_to_check_na)
    
    # If total_payment_received is 0 but total_principal_received is not 0, 
    # then calculate total_payment_received as sum of total_principal_received, total_interest_received and total_late_fee_received
    loans_payments_fixed_df = repayments_filtered_df.withColumn("total_payment_received",
    when((col("total_principal_received") != 0.0) & (col("total_payment_received") == 0.0),
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")
    ).otherwise(col("total_payment_received")))
    
    # Filter out records where total_payment_received is still 0 after the above correction
    loans_payments_fixed2_df = loans_payments_fixed_df.filter("total_payment_received != 0.0")
    
    # If last_payment_date and next_payment_date are numeric, replace with null
    loans_payments_ldate_fixed_df = loans_payments_fixed2_df.withColumn("next_payment_date", 
        when(col("last_payment_date").rlike(r'^\d+(\.\d+)?$'), None).otherwise(col("last_payment_date")))

    loans_payments_ndate_fixed_df = loans_payments_ldate_fixed_df.withColumn("last_payment_date",
        when((col("next_payment_date").rlike(r'^\d+(\.\d+)?$')),None).otherwise(col("next_payment_date")))
    
    return loans_payments_ndate_fixed_df

def clean_delinquencies(spark, defaulters_df):
    return

def final_cleaning(spark):
    return
