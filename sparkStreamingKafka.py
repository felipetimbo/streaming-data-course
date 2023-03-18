from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("wordCountStructured") \
        .getOrCreate()

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "wctopic") \
        .load()

    words_df = kafka_df.select(expr("explode(split(value,' ')) as word"))

    counts_df = words_df.groupBy("word").count()

    word_count_query = counts_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .start()
    
    word_count_query.awaitTermination()
