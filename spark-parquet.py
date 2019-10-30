import findspark 
findspark.init()
import pyspark from pyspark.sql 
import SparkSession 
spark = SparkSession \ 
.builder \
.appName("Python Spark SQL basic example") \
.config("spark.sql.parquet.compression.codec", "gzip") \
.getOrCreate() 

df = spark.read.load(r"C:\Users\rraj\python_projects\data \onsite_kinesis_complete.parquet.gzip") 
df.printSchema()
