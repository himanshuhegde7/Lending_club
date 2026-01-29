import os
from pyspark.sql import SparkSession

# Set JAVA_HOME for PySpark
os.environ['JAVA_HOME'] = '/opt/homebrew/Cellar/openjdk@21/21.0.9/libexec/openjdk.jdk/Contents/Home'

# Stop any existing spark sessions
SparkSession._instantiatedSession = None if SparkSession._instantiatedSession else None

spark = SparkSession.builder \
    .appName("LendingClub") \
    .master("local[2]") \
    .getOrCreate()

file_path = "../data/raw/customers.csv"