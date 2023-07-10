"""
Question: Given a text file containing student records in the format "Name,Score", calculate the average score of all the students.

Input: Text file content:
John,85
Emily,92
Michael,78
Sophia,90

Expected Output: Average score: 86.25
"""

# Initializing SparkSession and SparkContext objects

from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Counting words").getOrCreate()
sc = spark.sparkContext

# Reading the file, and converting it to RDD using textFile function

rdd1 = sc.textFile("/user/cloudera/students_score.txt")

# Iterating through the RDD using Map function, and calculate the average score using Mean function

rdd2 = rdd1.map(lambda x: x.split(",")).map(lambda x: int(x[1])).mean()

# Reading sample calculated average data 

print(ff" Average score: {rdd2.take(2)}")
