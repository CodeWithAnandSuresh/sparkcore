"""
Question: Given a list of numbers, find the maximum and minimum numbers.

Input: [5, 9, 3, 2, 7]

Expected Output:
Maximum: 9
Minimum: 2
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Max and Min numbers").getOrCreate()
sc = spark.sparkContext

# Creating a Python list with the list of values.
lst = [5, 9, 3, 2, 7]

# Converting the Python list to RDD using parallelize function
rdd1 = sc.parallelize(lst)

#Finding the minimum and maximum numbers using the in-built functions
print(f"The minimum number in the given list is: {rdd1.min()}")
# The minimum number in the given list is: 2

print(f"The minimum number in the given list is: {rdd1.max()}")
# The minimum number in the given list is: 9
