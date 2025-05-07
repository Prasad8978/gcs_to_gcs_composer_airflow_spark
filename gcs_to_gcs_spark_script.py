from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyJob").getOrCreate()

df = spark.read.csv("gs://comp-airflow-bucket/emp8_data.csv", header = True)

df = df.drop("age", "disgnation", "address")

df.write.mode("overwrite").csv("gs://comp-airflow-bucket/data/output/", header = True)

spark.stop()
