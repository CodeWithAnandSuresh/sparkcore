"""
Question: Given a list of numbers, calculate the sum of all the numbers.

Input: [10, 5, 7, 3, 17]

Expected Output: Sum: 42

"""
# Initializing SparkSession and SparkContext objects

from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Counting words").getOrCreate()
sc = spark.sparkContext

# Create a list of values
lst = [10, 5, 7, 3, 17]

# Converting the python list into Spark RDD by using Parallelize function
rdd1 = sc.parallelize(lst)

# Adding all the numbers by using the sum function

print(f"The sum of the given list: {rdd1.sum()}")
