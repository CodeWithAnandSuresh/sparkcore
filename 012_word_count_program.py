"""
Question: Given a list of sentences, count the number of occurrences of each word across all the sentences.

Input: ["Hello world", "Spark is awesome", "Hello again", "World is great"]

Expected Output: Word count: Hello: 2 world: 2 Spark: 1 is: 2 awesome: 1 again: 1 great: 1
"""

# Initializing SparkSession and SparkContext objects
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.master("local").appName("Counting words").getOrCreate()
sc = spark.sparkContext

# Creating a Python list with the list of string.
lst =  ["Hello world", "Spark is awesome", "Hello again", "World is great"]

# Converting the Python list to RDD using parallelize function
rdd1 = sc.parallelize(lst)

# Using flatMap function to flatten the data
rdd2 = rdd1.flatMap(lambda x:x.split(" "))
rdd2.collect()
# Output: ['Hello', 'world', 'Spark', 'is', 'awesome', 'Hello', 'again', 'World', 'is', 'great']

# Converting the list into a tuple key-value pair to perform the count
rdd3 = rdd2.map(lambda x:(x,1))
rdd3.collect()
# Output: [('Hello', 1), ('world', 1), ('Spark', 1), ('is', 1), ('awesome', 1), ('Hello', 1), ('again', 1), ('World', 1), ('is', 1), ('great', 1)]

# Reduce the key-value pair by using reduceByKey function to get the final output
rdd4 = rdd3.reduceByKey(lambda x,y:x+y)
print(f"The word count program result: {rdd4.collect()}")
# Output: The word count program result: [('Hello', 2), ('world', 1), ('Spark', 1), ('is', 2), ('awesome', 1), ('again', 1), ('World', 1), ('great', 1)]

# Sorting the word count using the count of the words with the help of sortBy function
rdd5 = rdd4.sortBy(lambda x:x[1],ascending=False)

# Displaying the output with the word count program in descending count
print(f"The sorted word count: {rdd5.collect()}")
# The sorted word count: [('Hello', 2), ('is', 2), ('world', 1), ('Spark', 1), ('awesome', 1), ('again', 1), ('World', 1), ('great', 1)]
