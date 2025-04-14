print("SCRIPT STARTED")

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").getOrCreate()

try:
    print("Spark version:", spark.version)
    test_df = spark.range(5)
    print("Count:", test_df.count())  # Should print 5
finally:
    spark.stop()
    print("SCRIPT ENDED")