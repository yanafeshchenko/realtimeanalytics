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
