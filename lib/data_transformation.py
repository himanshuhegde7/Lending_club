from pyspark.sql.functions import (
    current_timestamp, 
    regexp_replace, 
    col, 
    when, 
    coalesce, 
    lit, 
    length
)
import os

# A. Clean customers.csv
def clean_customers(spark, customers_df):
    # Remove duplicate records
    customers_df_distinct = customers_df.distinct()
    
    # Filter out records with null or empty annual_income
    customers_df_distinct.createOrReplaceTempView("customers")
    customers_income_filtered = spark.sql("SELECT * FROM customers WHERE annual_income IS NOT null")

    # Extract numeric part from emp_length and convert it to integer. Eg: "9 years" to 9, n/a to null
    customers_emplength_cleaned = customers_income_filtered.withColumn(
        "emp_length", regexp_replace(col("emp_length"), "[^0-9]",""))
    customers_emplength_int = customers_emplength_cleaned.withColumn(
        "emp_length", when(col("emp_length") == "", None).otherwise(col("emp_length")).cast('int'))

    # Replace nulls in emp_length with average of the column
    customers_emplength_int.createOrReplaceTempView("customers")
    avg_emp_length = spark.sql("SELECT FLOOR(AVG(emp_length)) AS avg_emp_length FROM customers").collect()
    avg_emp_duration = avg_emp_length[0][0]
    customers_emplength_replaced = customers_emplength_int.na.fill(avg_emp_duration, subset=['emp_length'])
    
    # Clean address_state: if length > 2, replace with 'NA'
    customers_state_cleaned = customers_emplength_replaced.withColumn(
        "address_state", when(length(col("address_state"))> 2, "NA")
        .otherwise(col("address_state")))
    
    bad_data_customers = spark.sql("""SELECT * FROM customers 
        WHERE member_id IN (
            SELECT member_id
            FROM customers
            GROUP BY member_id
            HAVING COUNT(*) > 1)
        """)
    
    customers_memberid_cleaned = spark.sql("""SELECT * FROM customers 
    WHERE member_id IN (
        SELECT member_id
        FROM customers
        GROUP BY member_id
        HAVING COUNT(*) = 1)
    """)
    
    return customers_memberid_cleaned, bad_data_customers


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

def clean_delinquencies(spark, delinquencies_df):
    delinquencies_df.createOrReplaceTempView("delinquencies")
    bad_data_delinquencies = spark.sql("""SELECT * FROM delinquencies 
        WHERE member_id IN (
            SELECT member_id
            FROM delinquencies
            GROUP BY member_id
            HAVING COUNT(*) > 1)
        """)
    
    delinquencies_memberid_cleaned = spark.sql("""SELECT * FROM delinquencies 
        WHERE member_id IN (
            SELECT member_id
            FROM delinquencies
            GROUP BY member_id
            HAVING COUNT(*) = 1)
        """)
    
    # Convert the delinq_2yrs column to integer and replaces nulls with 0
    delinquencies_processed_df = delinquencies_memberid_cleaned.withColumn(
        "delinq_2yrs", col("delinq_2yrs").cast("integer")).fillna(0, subset = ["delinq_2yrs"])
    
    # Create a dataframe with members having at least one delinquency
    delinquencies_processed_df.createOrReplaceTempView("delinquencies")
    delinquencies_cleaned = spark.sql("""
        SELECT member_id, delinq_2yrs, delinq_amnt, int(mths_since_last_delinq) 
        FROM delinquencies 
        WHERE delinq_2yrs > 0 OR mths_since_last_delinq > 0
        """)
    
    # Creating defaulters details - public records
    loans_def_p_pub_rec_df = delinquencies_processed_df.withColumn(
    "pub_rec", col("pub_rec").cast("integer")).fillna(0, subset = ["pub_rec"])
    
    loans_def_p_pub_rec_bankruptcies_df = loans_def_p_pub_rec_df.withColumn(
    "pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("integer")).fillna(0, subset = ["pub_rec_bankruptcies"])
    
    loans_def_p_inq_last_6mths_df = loans_def_p_pub_rec_bankruptcies_df.withColumn(
    "inq_last_6mths", col("inq_last_6mths").cast("integer")).fillna(0, subset = ["inq_last_6mths"])
    
    loans_def_p_inq_last_6mths_df.createOrReplaceTempView("loan_defaulters")

    delinquencies_public_records = spark.sql("""
    SELECT member_id, pub_rec, pub_rec_bankruptcies, inq_last_6mths 
    FROM loan_defaulters
    """)
    
    return delinquencies_cleaned, delinquencies_public_records, bad_data_delinquencies

def calculate_credit_score(spark):
    credit_score_tiers = {
    "unacceptable": 0,
    "very_bad": 100,
    "bad": 250,
    "good": 500,
    "very_good": 650,
    "excellent": 800
    }
    
    # Criterion 1 - Payment history lph
    ph_df = spark.sql(f"""
    SELECT
        c.member_id,
        CASE
            WHEN p.last_payment_amount >= (c.monthly_installment * 0.5)
                AND p.last_payment_amount < c.monthly_installment
                THEN {credit_score_tiers["very_bad"]}
            WHEN p.last_payment_amount = c.monthly_installment
                THEN {credit_score_tiers["good"]}
            WHEN p.last_payment_amount > c.monthly_installment
                AND p.last_payment_amount <= (c.monthly_installment * 1.5)
                THEN {credit_score_tiers["very_good"]}
            WHEN p.last_payment_amount > (c.monthly_installment * 1.5)
                THEN {credit_score_tiers["excellent"]}
            ELSE {credit_score_tiers["unacceptable"]}
        END AS last_payment_pts,

        CASE
            WHEN p.total_payment_received >= (c.funded_amount * 0.5)
                THEN {credit_score_tiers["very_good"]}
            WHEN p.total_payment_received < (c.funded_amount * 0.5)
                AND p.total_payment_received > 0
                THEN {credit_score_tiers["good"]}
            WHEN p.total_payment_received = 0
                OR p.total_payment_received IS NULL
                THEN {credit_score_tiers["unacceptable"]}
        END AS total_payment_pts

    FROM lending_club.repayments p
    JOIN lending_club.loans c
        ON c.loan_id = p.loan_id
    """)
    ph_df.createOrReplaceTempView("ph_pts")
    
    # Criterion 2 - loan defaulters history ldh
    ldh_ph_df = spark.sql(f"""
        select p.*,
        CASE 
        WHEN d.delinq_2yrs = 0 
            THEN {credit_score_tiers["excellent"]} 
        WHEN d.delinq_2yrs BETWEEN 1 AND 2 
            THEN {credit_score_tiers["bad"]} 
        WHEN d.delinq_2yrs BETWEEN 3 AND 5 
            THEN {credit_score_tiers["very_bad"]} 
        WHEN d.delinq_2yrs > 5 OR d.delinq_2yrs IS NULL 
            THEN {credit_score_tiers["unacceptable"]} 
        END AS delinq_pts, 
        CASE 
        WHEN l.pub_rec = 0 
            THEN {credit_score_tiers["excellent"]} 
        WHEN l.pub_rec BETWEEN 1 AND 2 
            THEN {credit_score_tiers["bad"]} 
        WHEN l.pub_rec BETWEEN 3 AND 5 
            THEN {credit_score_tiers["very_bad"]} 
        WHEN l.pub_rec > 5 OR l.pub_rec IS NULL 
            THEN {credit_score_tiers["very_bad"]} 
        END AS public_records_pts, 
        CASE 
        WHEN l.pub_rec_bankruptcies = 0 
            THEN {credit_score_tiers["excellent"]} 
        WHEN l.pub_rec_bankruptcies BETWEEN 1 AND 2 
            THEN {credit_score_tiers["bad"]} 
        WHEN l.pub_rec_bankruptcies BETWEEN 3 AND 5 
            THEN {credit_score_tiers["very_bad"]} 
        WHEN l.pub_rec_bankruptcies > 5 OR l.pub_rec_bankruptcies IS NULL 
            THEN {credit_score_tiers["very_bad"]}
        END as public_bankruptcies_pts, 
        CASE 
        WHEN l.inq_last_6mths = 0 
            THEN {credit_score_tiers["excellent"]} 
        WHEN l.inq_last_6mths BETWEEN 1 AND 2 
            THEN {credit_score_tiers["bad"]} 
        WHEN l.inq_last_6mths BETWEEN 3 AND 5 
            THEN {credit_score_tiers["very_bad"]} 
        WHEN l.inq_last_6mths > 5 OR l.inq_last_6mths IS NULL 
            THEN {credit_score_tiers["unacceptable"]} 
        END AS enq_pts 
        FROM lending_club.delinquencies_public_records l 
        INNER JOIN lending_club.delinquencies d ON d.member_id = l.member_id  
        INNER JOIN ph_pts p ON p.member_id = l.member_id
        """)
    ldh_ph_df.createOrReplaceTempView("ldh_ph_pts")
    
    # Criterion 3 - financial health fh
    fh_ldh_ph_df = spark.sql(f"""select ldef.*, 
    CASE 
    WHEN LOWER(l.loan_status) LIKE '%fully paid%'
        THEN {credit_score_tiers["excellent"]} 
    WHEN LOWER(l.loan_status) LIKE '%current%' 
        THEN {credit_score_tiers["good"]} 
    WHEN LOWER(l.loan_status) LIKE '%in grace period%' 
        THEN {credit_score_tiers["bad"]} 
    WHEN LOWER(l.loan_status) LIKE '%late (16-30 days)%' OR LOWER(l.loan_status) LIKE '%late (31-120 days)%' 
        THEN {credit_score_tiers["very_bad"]} 
    WHEN LOWER(l.loan_status) LIKE '%charged off%' 
        THEN {credit_score_tiers["unacceptable"]}
    else {credit_score_tiers["unacceptable"]} 
    END AS loan_status_pts, 
    CASE 
    WHEN LOWER(a.home_ownership) LIKE '%own' 
        THEN {credit_score_tiers["excellent"]} 
    WHEN LOWER(a.home_ownership) LIKE '%rent' 
        THEN {credit_score_tiers["good"]} 
    WHEN LOWER(a.home_ownership) LIKE '%mortgage' 
        THEN {credit_score_tiers["bad"]} 
    WHEN LOWER(a.home_ownership) LIKE '%any' OR LOWER(a.home_ownership) IS NULL 
        THEN {credit_score_tiers["very_bad"]} 
    END AS home_pts, 
    CASE 
    WHEN l.funded_amount <= (a.total_high_credit_limit * 0.10) 
        THEN {credit_score_tiers["excellent"]} 
    WHEN l.funded_amount > (a.total_high_credit_limit * 0.10) AND l.funded_amount <= (a.total_high_credit_limit * 0.20) 
        THEN {credit_score_tiers["very_good"]} 
    WHEN l.funded_amount > (a.total_high_credit_limit * 0.20) AND l.funded_amount <= (a.total_high_credit_limit * 0.30) 
        THEN {credit_score_tiers["good"]} 
    WHEN l.funded_amount > (a.total_high_credit_limit * 0.30) AND l.funded_amount <= (a.total_high_credit_limit * 0.50) 
        THEN {credit_score_tiers["bad"]} 
    WHEN l.funded_amount > (a.total_high_credit_limit * 0.50) AND l.funded_amount <= (a.total_high_credit_limit * 0.70) 
        THEN {credit_score_tiers["very_bad"]} 
    WHEN l.funded_amount > (a.total_high_credit_limit * 0.70) 
        THEN {credit_score_tiers["unacceptable"]}
    else {credit_score_tiers["unacceptable"]} 
    END AS credit_limit_pts, 
    CASE 
    WHEN (a.grade) = 'A' and (a.sub_grade)='A1' 
        THEN {credit_score_tiers["excellent"]} 
    WHEN (a.grade) = 'A' and (a.sub_grade)='A2' 
        THEN ({credit_score_tiers["excellent"]} * 0.95) 
    WHEN (a.grade) = 'A' and (a.sub_grade)='A3' 
        THEN ({credit_score_tiers["excellent"]} * 0.90) 
    WHEN (a.grade) = 'A' and (a.sub_grade)='A4' 
        THEN ({credit_score_tiers["excellent"]} * 0.85) 
    WHEN (a.grade) = 'A' and (a.sub_grade)='A5' 
        THEN ({credit_score_tiers["excellent"]} * 0.80) 
    WHEN (a.grade) = 'B' and (a.sub_grade)='B1' 
        THEN {credit_score_tiers["very_good"]} 
    WHEN (a.grade) = 'B' and (a.sub_grade)='B2' 
        THEN ({credit_score_tiers["very_good"]} * 0.95) 
    WHEN (a.grade) = 'B' and (a.sub_grade)='B3' 
        THEN ({credit_score_tiers["very_good"]} * 0.90) 
    WHEN (a.grade) = 'B' and (a.sub_grade)='B4' 
        THEN ({credit_score_tiers["very_good"]} * 0.85) 
    WHEN (a.grade) = 'B' and (a.sub_grade)='B5' 
        THEN ({credit_score_tiers["very_good"]} * 0.80) 
    WHEN (a.grade) = 'C' and (a.sub_grade)='C1' 
        THEN {credit_score_tiers["good"]} 
    WHEN (a.grade) = 'C' and (a.sub_grade)='C2' 
        THEN ({credit_score_tiers["good"]} * 0.95) 
    WHEN (a.grade) = 'C' and (a.sub_grade)='C3' 
        THEN ({credit_score_tiers["good"]} * 0.90) 
    WHEN (a.grade) = 'C' and (a.sub_grade)='C4' 
        THEN ({credit_score_tiers["good"]} * 0.85) 
    WHEN (a.grade) = 'C' and (a.sub_grade)='C5' 
        THEN ({credit_score_tiers["good"]} * 0.80) 
    WHEN (a.grade) = 'D' and (a.sub_grade)='D1' THEN ({credit_score_tiers["bad"]}) 
    WHEN (a.grade) = 'D' and (a.sub_grade)='D2' THEN ({credit_score_tiers["bad"]} * 0.95) 
    WHEN (a.grade) = 'D' and (a.sub_grade)='D3' THEN ({credit_score_tiers["bad"]} * 0.90) 
    WHEN (a.grade) = 'D' and (a.sub_grade)='D4' THEN ({credit_score_tiers["bad"]} * 0.85) 
    WHEN (a.grade) = 'D' and (a.sub_grade)='D5' THEN ({credit_score_tiers["bad"]} * 0.80) 
    WHEN (a.grade) = 'E' and (a.sub_grade)='E1' THEN {credit_score_tiers["very_bad"]} 
    WHEN (a.grade) = 'E' and (a.sub_grade)='E2' THEN ({credit_score_tiers["very_bad"]} * 0.95) 
    WHEN (a.grade) = 'E' and (a.sub_grade)='E3' THEN ({credit_score_tiers["very_bad"]} * 0.90) 
    WHEN (a.grade) = 'E' and (a.sub_grade)='E4' THEN ({credit_score_tiers["very_bad"]} * 0.85) 
    WHEN (a.grade) = 'E' and (a.sub_grade)='E5' THEN ({credit_score_tiers["very_bad"]} * 0.80) 
    WHEN (a.grade) in ('F', 'G') THEN {credit_score_tiers["unacceptable"]} 
    END AS grade_pts 
    FROM ldh_ph_pts ldef 
    INNER JOIN lending_club.loans l ON ldef.member_id = l.member_id 
    INNER JOIN lending_club.customers a ON a.member_id = ldef.member_id
   """) 
    fh_ldh_ph_df.createOrReplaceTempView("fh_ldh_ph_pts")

    # Final Credit Score Calculation
    # 1. Payment History = 20%
    # 2. Loan Defaults = 45%
    # 3. Financial Health = 35%

    credit_score = spark.sql("""SELECT member_id, 
        ((last_payment_pts+total_payment_pts)*0.20) as payment_history_pts, 
        ((delinq_pts + public_records_pts + public_bankruptcies_pts + enq_pts) * 0.45) as defaulters_history_pts, 
        ((loan_status_pts + home_pts + credit_limit_pts + grade_pts)*0.35) as financial_health_pts 
        FROM fh_ldh_ph_pts""")
    
    final_credit_score = credit_score.withColumn(
    'credit_score', credit_score.payment_history_pts + credit_score.defaulters_history_pts + credit_score.financial_health_pts)
    
    return final_credit_score
