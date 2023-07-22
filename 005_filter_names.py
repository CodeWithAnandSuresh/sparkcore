"""
Question:
Given a list of names, create a new list that contains only the names starting with the letter 'S'.

Input:
["John", "Sarah", "Steven", "Emily", "Sam", "Sophia"]

Expected Output:
["Sarah", "Steven", "Sam", "Sophia"]
"""
# Initializing SparkSession and SparkContext objects

from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Filtering Names").getOrCreate()
sc = spark.sparkContext

# creating a python list with list of values
lst = ["John", "Sarah", "Steven", "Emily", "Sam", "Sophia"]

# Converting the python list into RDD using parallelize function
rdd1 = sc.parallelize(lst)

# Filtering out the values that starts with the letter 'S'
rdd2 = rdd1.filter(lambda x: x[0][0] == 'S')

# Printing only the filtered names from the RDD
print(f"The list of names starts with 'S' are: {rdd2.collect()}")
