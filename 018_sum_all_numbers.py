"""
Question: Given a text file containing numbers, find the sum of all the numbers.

Input: Text file content:
10
15
6
20
8
43
32
89
41
66
Expected Output: Sum of numbers: 330 
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Count Sentences").getOrCreate()
sc = spark.sparkContext

# Reading the file, and converting it to RDD using textFile function
rdd1 = sc.textFile("/user/cloudera/spark.txt")

# Iterate through each value using MAP function, and convert it into INT value and then perform SUM operation
rdd2 = rdd1.map(lambda x: int(x))
print(f"Sum of numbers: {rdd2.sum()}")
# Output: Sum of numbers: 330
