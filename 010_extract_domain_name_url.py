"""
Question: Given a text file containing URLs, extract the domain names from the URLs.

Input:
------
Text file content:
https://www.example.com
http://subdomain.example.com/page
https://www.google.com/search?q=spark

Expected Output:
----------------
Domain names:
example.com
subdomain.example.com
google.com
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Counting words").getOrCreate()
sc = spark.sparkContext

# Reading the file, and converting it to RDD using textFile function
rdd1 = sc.textFile("/user/cloudera/spark.txt")

# Splitting the values with comma separator using the map function
rdd2 = rdd1.map(lambda x:x.split("://")[1])
# ['www.example.com', 'subdomain.example.com/page', 'www.google.com/search?q=spark']

# Extracting only the domain name and then adding the .com to the domain name.
rdd3 = rdd2.map(lambda x:x.split(".")[1]+'.com')

# Displaying the list of extracted domain names from the given URLs
print(f"The domains names from the URL list: {rdd3.collect()}")

# Output: The domains names from the URL list: ['example.com', 'example.com', 'google.com']
