import os
from pyspark.sql import SparkSession
import lib.data_ingestion as di, lib.data_transformation as dt

# Set JAVA_HOME for PySpark
os.environ['JAVA_HOME'] = '/opt/homebrew/Cellar/openjdk@21/21.0.9/libexec/openjdk.jdk/Contents/Home'

# Stop any existing spark sessions, create a new one
SparkSession._instantiatedSession = None if SparkSession._instantiatedSession else None
spark = SparkSession.builder.appName("LendingClub").master("local[2]").getOrCreate()

"""
# Load, clean and save customers.csv
customers_path = "../Lending_club/data/raw/customers.csv"
customers_raw = di.read_customers(spark, customers_path)
customers_cleaned, customers_bad_data = dt.clean_customers(spark, customers_raw)

(
customers_cleaned.write
    .option("header", True)
    .format("csv")
    .mode("overwrite")
    .option("path", "../Lending_club/data/cleaned/delete_customers")
    .save()
)

(
customers_bad_data.write
    .option("header", True)
    .format("csv")
    .mode("overwrite")
    .option("path", "../Lending_club/data/cleaned/delete_customers_bad_data")
    .save()
)


# Load, clean and save loans.csv
loans_path = "../Lending_club/data/raw/loans.csv"
loans_raw = di.read_loans(spark, loans_path)
loans_cleaned = dt.clean_loans(spark, loans_raw)

(
loans_cleaned.write
    .option("header", True)
    .format("csv")
    .mode("overwrite")
    .option("path", "../Lending_club/data/cleaned/delete_loans")
    .save()
)

# Load, clean and save repayments.csv
repayments_path = "../Lending_club/data/raw/repayments.csv"
repayments_raw = di.read_repayments(spark, repayments_path)
repayments_cleaned = dt.clean_repayments(spark, repayments_raw)

(
repayments_cleaned.write
    .option("header", True)
    .format("csv")
    .mode("overwrite")
    .option("path", "../Lending_club/data/cleaned/delete_repayments")
    .save()
)

# Load, clean and save delinquencies.csv
delinquencies_path = "../Lending_club/data/raw/delinquencies.csv"
delinquencies_raw = di.read_delinquencies(spark, delinquencies_path)
delinquencies_cleaned, delinquencies_public_records, delinquencies_bad_data = dt.clean_delinquencies(spark, delinquencies_raw)

(
delinquencies_cleaned.write
    .option("header", True)
    .format("csv")
    .mode("overwrite")
    .option("path", "../Lending_club/data/cleaned/delete_delinquencies")
    .save()
)

(   
delinquencies_bad_data.write
    .option("header", True)
    .format("csv")
    .mode("overwrite")
    .option("path", "../Lending_club/data/cleaned/delete_delinquencies_bad_data")
    .save()
)

(   
delinquencies_public_records.write
    .option("header", True)
    .format("csv")
    .mode("overwrite")
    .option("path", "../Lending_club/data/cleaned/delete_delinquencies_public_records")
    .save()
)

"""
# Create permanent tables on the above cleaned data

spark.sql("create database lending_club")

# Delete previously existing tables during re-execution of this file
spark.sql("drop table if exists lending_club.customers")
spark.sql("drop table if exists lending_club.loans")
spark.sql("drop table if exists lending_club.repayments")
spark.sql("drop table if exists lending_club.delinquencies")
spark.sql("drop table if exists lending_club.delinquencies_public_records")


# Create 5 external tables
spark.sql("""CREATE EXTERNAL TABLE lending_club.customers(
    member_id STRING, emp_title STRING, emp_length INTEGER, 
    home_ownership STRING, annual_income FLOAT, address_state STRING, 
    address_zipcode STRING, address_country STRING, grade STRING, 
    sub_grade STRING, verification_status STRING, total_high_credit_limit FLOAT,
    application_type STRING, join_annual_income FLOAT, 
    verification_status_joint STRING, ingest_date TIMESTAMP)
    using csv location  '/Users/himanshuhegde/Desktop/Lending_club/data/cleaned/delete_customers'
    options('header'='true', 'inferSchema'='false')
    """)

spark.sql("""CREATE EXTERNAL TABLE lending_club.loans(
    loan_id STRING, member_id STRING, loan_amount FLOAT, funded_amount FLOAT,
    loan_term_years INTEGER, interest_rate FLOAT, monthly_installment FLOAT, issue_date STRING,
    loan_status STRING, loan_purpose STRING, loan_title STRING, ingest_date TIMESTAMP)
    using csv location  '/Users/himanshuhegde/Desktop/Lending_club/data/cleaned/delete_loans'
    options('header'='true', 'inferSchema'='false')
    """)

spark.sql("""CREATE EXTERNAL TABLE lending_club.repayments(
    loan_id string, total_principal_received FLOAT, 
    total_interest_received FLOAT,total_late_fee_received FLOAT,
    total_payment_received FLOAT,last_payment_amount FLOAT,
    last_payment_date string,next_payment_date string,
    ingest_date TIMESTAMP)
    using csv location  '/Users/himanshuhegde/Desktop/Lending_club/data/cleaned/delete_repayments'
    options('header'='true', 'inferSchema'='false')
    """)

spark.sql("""CREATE EXTERNAL TABLE lending_club.delinquencies(
    member_id string, delinq_2yrs INTEGER, delinq_amnt FLOAT,
    mths_since_last_delinq INTEGER)
    using csv location  '/Users/himanshuhegde/Desktop/Lending_club/data/cleaned/delete_delinquencies'
    options('header'='true', 'inferSchema'='false')
    """)

spark.sql("""CREATE EXTERNAL TABLE lending_club.delinquencies_public_records(
    member_id string, pub_rec INTEGER, pub_rec_bankruptcies INTEGER, 
    inq_last_6mths INTEGER)
    using csv location  '/Users/himanshuhegde/Desktop/Lending_club/data/cleaned/delete_delinquencies_public_records'
    options('header'='true', 'inferSchema'='false')
    """)



#   7. Remove duplicate member ids in each file
#	a. Save the duplicate member ids as a separate csv in 'Bad data folder'
#	b. Re-save the above permanent tables after removing duplicate member ids, as parquet format
#	c. Save in cleaned_new. Customers, loan defaulters delinq, loan defaulters detail rec enq
#	8. Create external tables for the three new parquets in 7
#	9. Calculating credit score
#	a. Calculate points for payments history
#	b. Calculate points for defaulters history
#	c. Calculate points for financial health
#	d. Final credit score calculate, add to database, save as parquet