"""
Question:
Given a list of numbers, filter out the even numbers and return the result as a list.

Input:
[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

Expected Output:
[1, 3, 5, 7, 9]
"""

# Initializing SparkSession and SparkContext objects

from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Filter Even Numbers").getOrCreate()
sc = spark.sparkContext

# creating a python list with list of values
lst = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Converting the python list into RDD using parallelize function

rdd1 = sc.parallelize(lst)
rdd2 = rdd1.map(lambda x: x %2 == 0)

# Printing only the even numbers from the list
print(f"Even numbers: {rdd2.collect()}")
