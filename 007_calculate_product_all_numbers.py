"""
Question: Given a list of numbers, calculate the product of all the numbers.

Input: [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

Expected Output: Product: 3715891200
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Product of all numbers").getOrCreate()
sc = spark.sparkContext

# Creating a python list with given values
lst_values = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

# Converting the Python list into RDD by using the spark parallelize function
rdd1 = sc.parallelize(lst_values)

# Calculating the product of all the values by using the reduce function and storing it in a Python variable
product_result = rdd1.reduce(lambda x,y:x*y)

# Displaying the result of the product of the given values
print(f"The product of all the given values are: {product_result}")
