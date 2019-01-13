from pyspark.sql import SparkSession,SQLContext
from collections import namedtuple
from bson.son import SON
from pyspark import SparkContext
from pyspark.sql.functions import desc
from kafka import KafkaConsumer
from pyspark.streaming import StreamingContext



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
some_fruit = sqlContext.sql("SELECT tag, SUM(count) as frequency FROM temp GROUP BY tag ORDER BY frequency desc")
some_fruit.show(10)

#df.registerTempTable("tempTable")
#SQLContext.registerDataFrameAsTable(df, "tempTable")
#SQLContext.sql()
#sqlContext = SQLContext(my_spark)
#result= sqlContext.sql('SELECT tweet,count(*) as frequency FROM tempTable GROUP BY tweet ORDER BY tweet')



#result = sqlContext.sql('SELECT tag, count from tweets')
#result.show()
# df.printSchema()
# df['tweet']= df['tweet'].str.split(" ", n = 1, expand = True)
# df.show()
# df.filter( lambda word: word.lower().startswith("#") ) \
# .map( lambda word: ( word.lower(), 1 ) ) \
# .reduceByKey( lambda a, b: a + b ) \
# .map( lambda rec: Tweet( rec[0], rec[1] ) ) \
# .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") ))\
# .limit(10).registerTempTable("tweets")

#top_10_tweets = SQLContext.sql( 'Select tag, count from tweets' )
#top_10_df = top_10_tweets.toPandas()
#top_10_df.head()