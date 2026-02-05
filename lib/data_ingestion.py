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
        verification_status_joint STRING,
        ingest_date TIMESTAMP
    """
    return schema

def get_loans_schema():
    schema = [
        {"name": "loan_id", "type": "integer"},
        {"name": "customer_id", "type": "integer"},
        {"name": "loan_amount", "type": "float"},
        {"name": "loan_start_date", "type": "date"},
        {"name": "loan_end_date", "type": "date"},
        {"name": "interest_rate", "type": "float"}
    ]
    return schema

def get_repayments_schema():
    schema = [
        {"name": "repayment_id", "type": "integer"},
        {"name": "loan_id", "type": "integer"},
        {"name": "repayment_amount", "type": "float"},
        {"name": "repayment_date", "type": "date"}
    ]
    return schema

def get_defaulters_schema():
    schema = [
        {"name": "defaulter_id", "type": "integer"},
        {"name": "customer_id", "type": "integer"},
        {"name": "loan_id", "type": "integer"},
        {"name": "default_date", "type": "date"},
        {"name": "amount_due", "type": "float"}
    ]
    return schema   

def read_customers(spark, file_path):
    customers_df = spark.read.csv(file_path, header=True, schema = get_customers_schema())
    customers_df_ingested = customers_df.withColumn("ingest_date", current_timestamp())
    return customers_df_ingested

def read_loans(spark, file_path):
    loans_df = spark.read.csv(file_path, header=True, schema = get_loans_schema())
    return loans_df

def read_repayments(spark, file_path):
    repayments_df = spark.read.csv(file_path, header=True, schema = get_repayments_schema())
    return repayments_df

def read_defaulters(spark, file_path):
    defaulters_df = spark.read.csv(file_path, header=True, schema = get_defaulters_schema())
    return defaulters_df