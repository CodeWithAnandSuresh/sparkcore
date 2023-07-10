"""
Question: Given a text file containing log entries, count the number of occurrences of each unique log entry.

Input: Text file content:
Error: File not found
Warning: Connection timed out
Error: Invalid input
Info: Process completed successfully

Expected Output:
Log count:
Error: 2
Warning: 1
Info: 1
"""
# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Counting words").getOrCreate()
sc = spark.sparkContext

# Reading the file, and converting it to RDD using textFile function
rdd1 = sc.textFile("/user/cloudera/spark.txt")

# Applying map, reduceByKey function to extract the log type and reduce it by the key.
rdd2 = rdd1.map(lambda x:x.split(":")).map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y)

# Displaying the log count by the type of the log as expected
print(f"The number of log type occurrences in the given file is: {rdd2.collect()}")
