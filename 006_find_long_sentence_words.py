"""
Question: Given a text file containing sentences, find the longest sentence in terms of the number of words.

Input: Text file content:
Apache Spark is a fast and general-purpose cluster computing system.
It provides high-level APIs in Java, Scala, Python, and R.

Expected Output:
Longest sentence: It provides high-level APIs in Java, Scala, Python, and R.
"""
# Initializing SparkSession and SparkContext objects

from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Counting words").getOrCreate()
sc = spark.sparkContext

# Reading the file, and converting it to RDD using textFile function
rdd1 = sc.textFile("/user/cloudera/spark.txt")

# Reading the RDD and splitting them based on the new line and finding the sentence maximum number of words
rdd2 = rdd1.filter(lambda x: x.split()).max()

# Printing the sentence which contains the higher number of words
print(f"Longest sentence: {rdd2.collect()}")
