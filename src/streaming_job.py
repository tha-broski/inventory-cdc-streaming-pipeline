from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inventory-cdc-streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "inventory.public.products")
    .load()
)

kafka_df.printSchema()
value_df = kafka_df.selectExpr("CAST(value AS STRING) AS value")
query = (
    value_df.writeStream.format("console")
    .option("truncate", "false")
    .outputMode("append")
    .start()
)

query.awaitTermination()
