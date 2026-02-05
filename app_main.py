import os
from pyspark.sql import SparkSession
import lib.data_ingestion as di, lib.data_transformation as dt

# Set JAVA_HOME for PySpark
os.environ['JAVA_HOME'] = '/opt/homebrew/Cellar/openjdk@21/21.0.9/libexec/openjdk.jdk/Contents/Home'

# Stop any existing spark sessions
SparkSession._instantiatedSession = None if SparkSession._instantiatedSession else None

spark = SparkSession.builder.appName("LendingClub").master("local[2]").getOrCreate()

customers_path = "../Lending_club/data/raw/customers.csv"

customers_raw = di.read_customers(spark, customers_path)

customers_cleaned = dt.clean_customers(spark, customers_raw)

customers_cleaned.write \
    .option("header", True) \
    .format("csv") \
    .mode("overwrite") \
    .option("path", "../Lending_club/data/cleaned/customers_del.csv") \
    .save()

#	2. Cleaning loans.csv
#	a. Write back loans.csv in cleaned folder
#	3. Cleaning repayments.csv
#	4. Cleaning delinquencies.csv
#	a. Write delinquencies.csv
#	b. Write delinquncies public records.csv
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