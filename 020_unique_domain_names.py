"""
Question: Given a text file containing URLs, count the number of occurrences of each unique domain name.

Input: Text file content: (Sample)
https://www.example.com
http://subdomain.example.com/page
https://www.google.com/search?q=spark

Expected Output: (Sample)
Domain count:
example.com: 2
subdomain.example.com: 1
google.com: 1
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Unique Domain Names").getOrCreate()
sc = spark.sparkContext

Reading the file, and converting it to RDD using textFile function
rdd1 = sc.textFile("/user/cloudera/spark.txt")

# Extract URL without protocol
rdd2 = rdd1.map(lambda x: x.split(":")[1])

# Extract URL without protocol and leading "www."
rdd3 = rdd2.map(lambda x: x.split("//")[1])

# Extract domain name without trailing paths or query parameters
rdd4 = rdd3.map(lambda x: x.split("/")[0])

# Extract second-level domain and top-level domain
rdd5 = rdd4.map(lambda x: x.split(".")[-2] + "." + x.split(".")[-1])

# Create key-value pairs with domain name as key and count as value
rdd6 = rdd5.map(lambda x: (x, 1))

# Count the occurrences of each domain name
rdd7 = rdd6.reduceByKey(lambda x, y: x + y)

# Sort by count in descending order
rdd8 = rdd7.sortBy(lambda x: x[1], ascending=False)

# Collect the final result
result = rdd8.collect()
print(f"Domain count: {result}")

# Output: 
# Domain count: [('example.com', 8), ('google.com', 4), ('wikipedia.org', 4), ('amazon.com', 4), ('twitter.com', 3), ('nytimes.com', 3), 
# ('reddit.com', 3), ('instagram.com', 3), ('netflix.com', 3), ('apple.com', 3), ('facebook.com', 3), ('youtube.com', 3), 
# ('linkedin.com', 3), ('ebay.com', 3)]
