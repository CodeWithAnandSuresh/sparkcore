"""
Question: Given a text file containing numbers, find the average of the even numbers.

Input: [10, 15, 6, 20, 8, 42, 40, 23, 41, 92]

Expected Output: Average of even numbers: 31.14
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Counting words").getOrCreate()
sc = spark.sparkContext

# Creating a Python list with the list of values.
lst = [10, 15, 6, 20, 8]

# Converting the Python list to RDD using parallelize function
rdd1 = sc.parallelize(lst)

# Extracting only the even numbers from the list, and storing it in the new RDD
rdd2 = rdd1.filter(lambda x: x%2 == 0)

# Finding the mean of the remaining even number from the list
print(f"The mean of the even numbers in the list: {rdd2.mean()}")
