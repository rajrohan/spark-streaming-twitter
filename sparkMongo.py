from pyspark.sql import SparkSession,SQLContext



my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sparkkafka.stream") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sparkkafka.stream") \
    .getOrCreate()

sqlContext = SQLContext(my_spark)
df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.printSchema()
df.createOrReplaceTempView("temp")
top10Keywords = sqlContext.sql("SELECT tag, SUM(count) as frequency FROM temp GROUP BY tag ORDER BY frequency desc")
top10Keywords.show(10)

