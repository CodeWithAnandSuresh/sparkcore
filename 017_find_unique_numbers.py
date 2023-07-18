"""
Quesiton: Given a list of strings, convert all the strings to uppercase and return the modified list.

Input: ["apple", "banana", "kiwi", "strawberry", "orange"]

Expected Output: Uppercase strings: ["APPLE", "BANANA", "KIWI", "STRAWBERRY", "ORANGE"]
"""
# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Finding Unique Numbers").getOrCreate()
sc = spark.sparkContext

# Creating a Python list with the list of string values.
lst = ["apple", "banana", "kiwi", "strawberry", "orange"]

# Converting the Python list to RDD using parallelize function
rdd1 = sc.parallelize(lst)

# Displaying the list of string values in the uppercase
print(f"Uppercase strings: {rdd2.collect()}")
# Output: Uppercase strings: ['APPLE', 'BANANA', 'KIWI', 'STRAWBERRY', 'ORANGE']
