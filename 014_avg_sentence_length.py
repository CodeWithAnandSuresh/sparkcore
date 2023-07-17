"""
Question: Given a text file containing sentences, find the average length of the sentences.

Input(Text file content):
Apache Spark is a fast and general-purpose cluster computing system.
It provides high-level APIs in Java, Scala, Python, and R and many more

Expected Output: 
Average sentence length: 8.5
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Average Sentence Length").getOrCreate()
sc = spark.sparkContext

# Reading the file, and converting it to RDD using textFile function
rdd1 = sc.textFile("/user/cloudera/spark.txt")

# Split the file with the new line break to calculate the total number of sentences
rdd2 = rdd1.map(lambda x:x.split())
# rdd2.collect()
# [['Apache', 'Spark', 'is', 'a', 'fast', 'and', 'general-purpose', 'cluster', 'computing', 'system.'], ['It', 'provides', 'high-level', 'APIs', 'in', 'Java,', 'Scala,', 'Python,', 'and', 'R', 'and', 'many', 'more']]

# After the line split, using map and len function to calculate the number of words and finding the average using mean function
rdd3 = rdd2.map(lambda x:len(x)).mean()

# Displaying the average/mean of the sentences in the given file
print(f"The average length of the sentences in the given file is: {rdd3}")
# Output: The average length of the sentences in the given file is: 11.5
