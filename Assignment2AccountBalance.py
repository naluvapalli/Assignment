from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import sum, when, col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Account Balance") \
    .getOrCreate()

# Load dataset
Loaddataframe = spark.read.xlsm("C:\Users\Desktop\Data-asessment-dataset.xlsm", header=True, inferSchema=True)

# Partitioning by AccountNumber and ordered by TransactionType
 
windowSpec = Window.partitionBy("AccountNumber").orderBy("TransactionType")

# Calculate running total balance using cumulative sum over the window
Loaddataframe = Loaddataframe.withColumn("balance",
                   sum(when(col("TransactionType") == "credit", col("Amount"))
                       .otherwise(-col("Amount"))).over(windowSpec))

# Show the results
Loaddataframe.select("TransactionDate","AccountNumber","TransactionType","Amount", "balance").show()

# Stop the Spark session
spark.stop()