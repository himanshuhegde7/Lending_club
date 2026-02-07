from pyspark.sql.functions import current_timestamp

def get_customers_schema():
    schema = """
        member_id STRING,
        emp_title STRING,
        emp_length STRING,
        home_ownership STRING,
        annual_income STRING,
        address_state STRING,
        address_zipcode STRING,
        address_country STRING,
        grade STRING,
        sub_grade STRING,
        verification_status STRING,
        total_high_credit_limit INTEGER,
        application_type STRING,
        join_annual_income STRING,
        verification_status_joint STRING
    """
    return schema

def get_loans_schema():
    schema = """
        loan_id STRING,
        member_id STRING,
        loan_amount FLOAT,
        funded_amount FLOAT,
        loan_term_months STRING,
        interest_rate FLOAT,
        monthly_installment FLOAT,
        issue_date string,
        loan_status STRING,
        loan_purpose STRING,
        loan_title STRING
    """
    return schema

def get_repayments_schema():
    schema = """
        loan_id STRING,
        total_principal_received FLOAT,
        total_interest_received FLOAT,
        total_late_fee_received FLOAT,
        total_payment_received FLOAT,
        last_payment_amount FLOAT,
        last_payment_date STRING,
        next_payment_date STRING
    """
    return schema

def get_delinquencies_schema():
    schema = """
        member_id STRING,
        delinq_2yrs FLOAT,
        delinq_amnt FLOAT,
        pub_rec FLOAT,
        pub_rec_bankruptcies FLOAT,
        inq_last_6mths FLOAT,
        total_rec_late_fee FLOAT,
        mths_since_last_delinq FLOAT,
        mths_since_last_record FLOAT
    """
    return schema   

def read_customers(spark, file_path):
    customers_df = spark.read.csv(file_path, header=True, schema = get_customers_schema())
    customers_df_ingested = customers_df.withColumn("ingest_date", current_timestamp())
    return customers_df_ingested

def read_loans(spark, file_path):
    loans_df = spark.read.csv(file_path, header=True, schema = get_loans_schema())
    loans_df_ingested = loans_df.withColumn("ingest_date", current_timestamp())
    return loans_df_ingested

def read_repayments(spark, file_path):
    repayments_df = spark.read.csv(file_path, header=True, schema = get_repayments_schema())
    repayments_df_ingested = repayments_df.withColumn("ingest_date", current_timestamp())
    return repayments_df_ingested

def read_delinquencies(spark, file_path):
    delinquencies_df = spark.read.csv(file_path, header=True, schema = get_delinquencies_schema())
    delinquencies_df_ingested = delinquencies_df.withColumn("ingest_date", current_timestamp())
    return delinquencies_df_ingested