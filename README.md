# spark-streaming-twitter
Building pipeline to process the real-time data using Spark and Mongodb.
Twitter real-time data are pulling using an API and then processing it using Apache-Spark. The “tweepy” API pulls the data from twitter which is stored as JSON Objects. This JSON object contains the tweets, user-details, re-tweets, ip address of the tweets, geo-coordinates etc. But for our processing we are taking into consideration only the actual tweet(tweeted or retweeted by user) and the timestamp when it was created. This data is then staged in MongoDb and some processing is done on the run-time. 

Data Flow Process:
The Twitter dataset is real-time stream data. We can access stream data using twitter API. In order to get tweets from Twitter, authentication is required which can available after creating a Twitter application (in developer mode to get access tokens). Once access token is available Authentication can be done with tweepy API. 
Now for Building pipeline, First get the data to the StreamListener instance with the help of tweepy package later process the data and send it to the Mongodb. 
Now the streaming data is continuously flowing to spark streaming instance. The transformation will be performed once the data is available to spark instance later on the data will be available in spark temporary table and this will be used to return top trending hashtags and represents this data on a real-time dashboard.

Data Ingestion: 
Fetching Twitter live streaming  data requires following steps 
• Creating a Twitter application- In order to get the tweets from Twitter, it is needed to create a Twitter application and filling the information. After accepting the developer agreement,  we'll be able to create access tokens. 
• Connectivity with Spark- To setup a pipeline for streaming data we need to authenticate with Twitter API and send the data locally to Spark.  
• Processing with Spark- As the data will be live streaming we need to setup the process how we're processing the data and representing on the fly. Once the whole channel is establish and data flow will start we cannot intervene in transformation of the stream data.   
• Global Schema- Once the data start flowing in a pipeline,  we need to declare the schema for cleansing the data and storing in the local machine. 

Processing and Visualisation:(Rohan) 
• For Speed layer Data 
o The stream data we are getting from Twitter is in JSON object format. Before sending the data to Spark, it needs to be encoded (used UTF-8 encoding). The checkpoint directory is created with Streaming Context which will save the messages in case of streaming components fail. 
o Spark Streaming context is created with a batch interval of 10 seconds. The messages would accumulate for 10 seconds and then get processed. The RDD will be created for every 10 seconds, but the data in RDD will be for the last 20 seconds. 
o Streaming Context receives tweet text, Splits to a list, filters all the words which start with a hashtag(#), converts the words to lowercase, maps each tag to (word, 1), then reduces and counts the occurrence of each hashtag.
o Finally, it converts the resulting tags and their counts into a data frame, sorts the tags in descending order and takes only the first 10 records and this data frame will be stored in Temp Table which is In-memory table. Created a SQL context, which will be used to query the trends from the results. 
o Snippet for the processing the tweets-
( lines.flatMap( lambda text: text.split( " " ) ) .filter( lambda word: word.lower().startswith("#") ).map( lambda word: ( word.lower(), 1 ) ) .reduceByKey( lambda a, b: a + b ) .map( lambda rec: Tweet( rec[0], rec[1] ) ) .foreachRDD( lambda rdd: rdd.toDF().sort( desc("count") ) .limit(10).registerTempTable("tweets") ) ). 

• For Batch layer Data-   
o The Data is fetched from Mongodb with Aggregation query for trending keywords with the frequency. 
query- [{"$group": {"_id": "$tag", "count": {"$sum": "$count"}}},{"$sort": SON([("count", 1)])}].Later this data stored in data frame and used for visualization.

Challenges Faced and decisions made to mitigate:(Rohan) Challenges faced during building the data engineering model. 
• Twitter-Spark connectivity issue- 
o  Spark 2.4.x version is not supported with pyspark Spark Streaming. So, we decided to downgrade the version to 2.3.x as it was supported earlier by this version and this is working as expected.  
• Spark-MongoDB connectivity issue- 
o Spark-MongoDB has jars issue and it was solved by externally finding the compatible jars. The connection could not be established easily and it failed repeatedly due to Class Dependency errors. This was eliminated using jars(mongo-javadriver-3.9.0 ,org.mongodb.spark_mongo-spark-connector_2.112.4.0) downloaded from Maven repositories and adding it to the classpath. 
