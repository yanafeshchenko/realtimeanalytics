import re
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType
from textblob import TextBlob
import findspark



if __name__ == "__main__":
    findspark.init()

def clean_text(text):
    return re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', text.lower().strip())

def Subjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity

# TextBlob Polarity function
def Polarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity

# Assign sentiment to elements
def Sentiment(polarityValue: int) -> str:
    if polarityValue <= -0.1:
        return 'Negative'
    elif polarityValue >= 0.1:
        return 'Positive'
    else:
        return 'Neutral'

# spark object without localhost
spark = SparkSession \
    .builder \
    .appName("TwitterSentimentAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4") \
    .config("spark.local.dir", "D:/PROJECTS/local/temp") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets-iphone14") \
    .option("startingOffsets", "earliest") \
    .option("header", "true") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING)")


schema = StructType() \
    .add("date_time", StringType(), True) \
    .add("username", StringType(), True) \
    .add("user_location", StringType(), True) \
    .add("user_description", StringType(), True) \
    .add("verified", StringType(), True) \
    .add("followers_count", StringType(), True) \
    .add("following_count", StringType(), True) \
    .add("tweet_like_count", StringType(), True) \
    .add("tweet_retweet_count", StringType(), True) \
    .add("tweet_reply_count", StringType(), True) \
    .add("source", StringType(), True) \
    .add("tweet_text", StringType(), True)

values = df.select(from_json(df.value.cast("string"), schema).alias("tweets"))
df1 = values.select("tweets.*")

clean_tweets_udf = udf(clean_text, StringType())
subjectivity_func_udf = udf(Subjectivity, FloatType())
polarity_func_udf = udf(Polarity, FloatType())
sentiment_func_udf = udf(Sentiment, StringType())

cl_tweets = df1.withColumn('processed_text', clean_tweets_udf(col("tweet_text")))
subjectivity_tw = cl_tweets.withColumn('subjectivity', subjectivity_func_udf(col("processed_text")))
polarity_tw = subjectivity_tw.withColumn("polarity", polarity_func_udf(col("processed_text")))
sentiment_tw = polarity_tw.withColumn("sentiment", sentiment_func_udf(col("polarity")))

#sentiment_tw.printSchema()

url = "jdbc:postgresql://localhost:5432/postgres"
user = "postgres"
password = "postgresql2024"
driver = "org.postgresql.Driver"
table = "tweets_iphone14"


def _write_streaming(
        df,
        epoch_id
) -> None:
    df.write \
        .mode('append') \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", 'tweets_iphone14') \
        .option("user", 'postgres') \
        .option("password", 'postgresql2024') \
        .save()


sentiment_tw.writeStream \
    .foreachBatch(_write_streaming) \
    .option("checkpointLocation", "chk-point-dir") \
    .start() \
    .awaitTermination()


'''postgres_properties = {
    "driver": "C:\Software\Java\jdk-17\lib\postgresql-42.7.4.jar",
    "url": "jdbc:postgresql://localhost:5432/postgres",
    "dbtable": "twitter_test",  # Table where data will be written
    "user": "postgres",  # Your PostgreSQL username
    "password": "postgresql2024"  # Your PostgreSQL password
}'''

# Writing to PostgreSQL
'''sentiment_tw.write \
    .outputMode("append") \
    .format("jdbc") \
    .options(**postgres_properties) \  # Required for streaming write
    .save()'''


'''
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("TwitterSentimentAnalysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()


#spark.sparkContext.setLogLevel('ERROR')






df = df.withColumn("data", from_json(col("value"), schema))
df = df.select(col("data.*"))

pre_process = udf(clean_text, StringType())
#sent_analysis = udf(sentiment_analysis, DoubleType())
#sent_analysis_text = udf(sentiment_analysis_text, StringType())



#df = df.withColumn("clean_data", pre_process(col("tweet_text")))



#df.printSchema()
query = df.select("tweet_text") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
'''











'''
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from pyspark.sql.functions import col, from_json, expr, udf
from pyspark.sql.types import StructType, StringType, DoubleType, ArrayType
import nltk
import re

nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)

# Initialize a Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Define the schema for the incoming Kafka data (assume tweets are in JSON format)
schema = StructType() \
    .add("date_time", StringType()) \
    .add("username", StringType()) \
    .add("user_location", StringType()) \
    .add("user_description", StringType()) \
    .add("verified", StringType()) \
    .add("followers_count", StringType()) \
    .add("following_count", StringType()) \
    .add("tweet_like_count", StringType()) \
    .add("tweet_retweet_count", StringType()) \
    .add("tweet_reply_count", StringType()) \
    .add("source", StringType()) \
    .add("tweet_text", StringType())
'''



'''
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter_test") \
    .option("startingOffsets", "earliest") \
    .option("header", "true") \
    .load() \
    .selectExpr("CAST(value AS STRING)")
'''













'''schema = StructType() \
    .add("date_time", StringType(), True) \
    .add("username", StringType(), True) \
    .add("user_location", StringType(), True) \
    .add("user_description", StringType(), True) \
    .add("verified", StringType(), True) \
    .add("followers_count", StringType(), True) \
    .add("following_count", StringType(), True) \
    .add("tweet_like_count", StringType(), True) \
    .add("tweet_retweet_count", StringType(), True) \
    .add("tweet_reply_count", StringType(), True) \
    .add("source", StringType(), True) \
    .add("tweet_text", StringType(), True)

values = df.select(from_json(df.value.cast("string"), schema).alias("tweets"))
df1 = values.select("tweets.*")

query = df1.select("username","tweet_text") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()'''












# Read from Kafka topic
'''kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka messages are in key-value pairs, where value is your message
# Decode Kafka messages from binary to string
tweets_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Parse the JSON messages using the schema
parsed_df = tweets_df.withColumn("json_data", from_json(col("value"), schema)) \
    .select("json_data.*")

pre_process = udf(
    lambda x: re.sub(r'[^A-Za-z\n ]|(http\S+)|(www.\S+)', '', x.lower().strip()).split(), ArrayType(StringType())
)
parsed_df = parsed_df.withColumn("cleaned_data", pre_process(parsed_df.tweet_text)).dropna()

query = parsed_df \
    .select("date_time", "username", "tweet_text", "sentiment_score") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
'''


'''query = sentiment_tw.select("tweet_text", "polarity").writeStream.queryName("final_tweets_reg") \
        .outputMode("append").format("console") \
        .option("truncate", False) \
        .start().awaitTermination(60)'''

'''def write_in_postgres(df, batch_id):
    df.write \
        .format('jdbc') \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .mode("overwrite") \
        .save()

query = sentiment_tw.writeStream.queryName("final_tweets_reg") \
    .foreachBatch(write_in_postgres)\
    .start()\
    .awaitTermination()'''

'''sentiment_tw.select("date_time","username","user_location",)\
    .writeStream.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "tweets_test") \
    .option("user", "postgres").option("password", "postgresql2024").start().awaitTermination(25)'''

