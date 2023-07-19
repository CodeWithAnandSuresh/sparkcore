"""
Question: Given a text file containing log entries, find the top 5 most frequently occurring words.

Input: Text file content: (Sample data)
Timestamp: 2023-07-19 10:00:00
LogLevel: INFO
Message: Data processing started.
Component: DataProcessor
ExecutionID: dp-20230719100000
Server: 192.168.1.100
User: admin
Timestamp: 2023-07-19 10:00:10
LogLevel: INFO
Message: Data ingestion initiated.
Component: DataIngestor
ExecutionID: di-20230719100000
Source: /mnt/data/input
Destination: /tmp/data_stage

Expected Output: Top 5 frequent words: (Sample output based on sample data)
[(' INFO', 19), (' WARNING', 3), (' ERROR', 1)]
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("LogCount Analysis").getOrCreate()
sc = spark.sparkContext

# Reading the file, and converting it to RDD using textFile function
rdd1 = sc.textFile("/user/cloudera/spark.txt")

# Iterate through the values using MAP function, and split it based on semi-colon
rdd2 = rdd1.map(lambda x: x.split(":"))

# Filter only the LogLevel using the Filter Function
rdd3 = rdd2.filter(lambda x:x[0]=='LogLevel')

# Adding a value, and convert it into key-value pair and use reduceByKey function to perform the count
rdd4 = rdd3.map(lambda x:(x[1],1)).reduceByKey(lambda x,y:x+y)

# Displaying the count of log types with the count
print(f"Log Count Results: {rdd4.collect()}")
