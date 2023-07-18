"""
Question: Given a text file containing paragraphs, count the total number of sentences in the file.

Input:  Text file content:
Apache Spark is a fast and general-purpose cluster computing system.
It provides high-level APIs in Java, Scala, Python, and R.
Apache Spark is widely used for big data processing and analytics.

Expected Output: Total sentence count: 3
"""
# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Count Sentences").getOrCreate()
sc = spark.sparkContext

# Reading the file, and converting it to RDD using textFile function
rdd1 = sc.textFile("/user/cloudera/spark.txt")

# Split the file with the new line break to calculate the total number of sentences
rdd2 = rdd1.map(lambda x:x.split())

# After the line split, using count function to display the total number of sentences in the file
print(f"Total sentence count: {rdd2.count()}")
# Total sentence count: 3
