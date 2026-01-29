

def get_customers_schema():
    schema = [
        {"name": "member_id", "type": "string"},
        {"name": "emp_title", "type": "string"},
        {"name": "emp_length", "type": "string"},
        {"name": "home_ownership", "type": "string"},
        {"name": "annual_income", "type": "string"}
        {"name": "address_state", "type": "string"},
        {"name": "address_zipcode", "type": "string"}
        {"name": "address_country", "type": "string"},
        {"name": "grade", "type": "string"},
        {"name": "sub_grade", "type": "string"},
        {"name": "verification_status", "type": "string"},
        {"name": "total_high_credit_limit", "type": "integer"}
        {"name": "application_type", "type": "string"}
        {"name": "join_annual_income", "type": "string"}
        {"name": "verification_status_joint", "type": "string"}
        {"name": "ingest_date", "type": "string"}
    ]
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
    return customers_df

def read_loans(spark, file_path):
    loans_df = spark.read.csv(file_path, header=True, schema = get_loans_schema())
    return loans_df

def read_repayments(spark, file_path):
    repayments_df = spark.read.csv(file_path, header=True, schema = get_repayments_schema())
    return repayments_df

def read_defaulters(spark, file_path):
    defaulters_df = spark.read.csv(file_path, header=True, schema = get_defaulters_schema())
    return defaulters_df