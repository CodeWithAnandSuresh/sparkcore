"""
Question: Given a list of strings, sort the strings in descending order based on their lengths.

Input: ["apple", "banana", "kiwi", "strawberry", "orange"]

Expected Output: ["strawberry", "banana", "orange", "apple", "kiwi"]
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Sorting String Length").getOrCreate()
sc = spark.sparkContext

# Creating a Python list with the list of string.
lst = ["apple", "banana", "kiwi", "strawberry", "orange"]

# Converting the Python list to RDD using parallelize function
rdd1 = sc.parallelize(lst)

# Sorting the RDD with the descending order using the sortBy function with descending order
rdd2 = rdd1.sortBy(lambda x:len(x),ascending=False)

#Display the sorted list in the descending order
print(f"The list of strings in descending order: {rdd2}")
