"""
Question: Given a text file containing sentences, count the total number of words in the file.

Input: Text file content: Apache Spark is a fast and general-purpose cluster computing system.

Expected Output: Total word count: 8
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Counting words").getOrCreate()
sc = spark.sparkContext

# Reading the file from the HDFS Location (Content of file: Apache Spark is a fast and general-purpose cluster computing system.)

rdd1 = sc.textFile("/user/cloudera/words.txt")

#Printing the total count of words in the given file

print(rdd1.flatMap(lambda x:x.split(" ").count())
