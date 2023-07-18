"""
Question: Given a list of numbers, remove duplicates and return a new list with unique numbers.

Input: [2, 5, 7, 2, 9, 5, 3, 7,3,8]

Expected Output: Unique numbers: [2, 8, 5, 7, 9, 3]
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Finding Unique Numbers").getOrCreate()
sc = spark.sparkContext

# Creating a Python list with the list of values.
lst = [2, 5, 7, 2, 9, 5, 3, 7,3,8]

# Converting the Python list to RDD using parallelize function
rdd1 = sc.parallelize(lst)

# Extracting the unique values using the built-in distinct function
rdd2 = rdd1.distinct()

# Displaying only the unique values from the list
print(f"Unique Numbers: {rdd2.collect()}")
# Unique Numbers: [2, 8, 5, 7, 9, 3]
