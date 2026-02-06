import os
from pyspark.sql import SparkSession
import lib.data_ingestion as di, lib.data_transformation as dt

# Set JAVA_HOME for PySpark
os.environ['JAVA_HOME'] = '/opt/homebrew/Cellar/openjdk@21/21.0.9/libexec/openjdk.jdk/Contents/Home'

# Stop any existing spark sessions
SparkSession._instantiatedSession = None if SparkSession._instantiatedSession else None

spark = SparkSession.builder.appName("LendingClub").master("local[2]").getOrCreate()

"""
# Load, clean and save customers.csv
customers_path = "../Lending_club/data/raw/customers.csv"
customers_raw = di.read_customers(spark, customers_path)
customers_cleaned = dt.clean_customers(spark, customers_raw)
customers_cleaned.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "../Lending_club/data/cleaned/delete_customers") \
    .save()
"""

"""
# Load, clean and save loans.csv
loans_path = "../Lending_club/data/raw/loans.csv"
loans_raw = di.read_loans(spark, loans_path)
loans_cleaned = dt.clean_loans(spark, loans_raw)
loans_cleaned.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "../Lending_club/data/cleaned/delete_loans") \
    .save()
"""

"""
# Load, clean and save repayments.csv
repayments_path = "../Lending_club/data/raw/repayments.csv"
repayments_raw = di.read_repayments(spark, repayments_path)
repayments_cleaned = dt.clean_repayments(spark, repayments_raw)
repayments_cleaned.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "../Lending_club/data/cleaned/delete_repayments") \
    .save()
"""

# Load, clean and save delinquencies.csv
delinquencies_path = "../Lending_club/data/raw/delinquencies.csv"
delinquencies_raw = di.read_defaulters(spark, delinquencies_path)
delinquencies_cleaned, delinquencies_public_records = dt.clean_delinquencies(spark, delinquencies_raw)
delinquencies_cleaned.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "../Lending_club/data/cleaned/delete_delinquencies") \
    .save()
    
delinquencies_public_records.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "../Lending_club/data/cleaned/delete_delinquencies_public_records") \
    .save()

#	5. Create defaulters details
#	a. Write defaulters detail records enquiry csv
#	6. Create a database named lending club
#	a. Create permanent external tables for the above 5 cleaned and created csv files
#	7. Remove duplicate member ids in each file
#	a. Save the duplicate member ids as a separate csv in 'Bad data folder'
#	b. Re-save the above permanent tables after removing duplicate member ids, as parquet format
#	c. Save in cleaned_new. Customers, loan defaulters delinq, loan defaulters detail rec enq
#	8. Create external tables for the three new parquets in 7
#	9. Calculating credit score
#	a. Calculate points for payments history
#	b. Calculate points for defaulters history
#	c. Calculate points for financial health
#	d. Final credit score calculate, add to database, save as parquet