from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean
spark = SparkSession.builder.appName('InMemoryAnalytics').getOrCreate()
df = spark.read.csv('data/processed', header=True, inferSchema=True)
# cache the DataFrame (in-memory)
df.cache()
print('Row count:', df.count())
df.groupBy('region').agg(mean(col('num_transactions')).alias('avg_txn')).show()
spark.stop()
