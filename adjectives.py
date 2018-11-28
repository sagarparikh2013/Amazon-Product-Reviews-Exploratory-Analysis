from pyspark import SparkConf, SparkContext
import sys
import random
import operator
from pyspark.sql import SparkSession, functions, types
import json
from textblob import TextBlob
from pyspark.ml.feature import Tokenizer
import utilities


@functions.udf(returnType=types.StringType())
def textblob(word):
    blob = TextBlob(word)
    return blob.tags

def main(inputs):
    # reviews_df = utilities.get_completereviews_dataframe(spark)
    reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA)
    reviews_df = reviews_df.filter(reviews_df.product_id.isNotNull())

    positive_reviews_df = reviews_df.filter(reviews_df.star_rating > 3).dropna()
    negative_reviews_df = reviews_df.filter(reviews_df.star_rating < 3).dropna()
    neutral_reviews_df = reviews_df.filter(reviews_df.star_rating == 3).dropna()

    positive_reviews_df = positive_reviews_df.select('review_headline', 'review_body').withColumn('review_concat', functions.concat('review_headline', functions.lit(" "),'review_body')).select('review_concat').dropna()

    tokenizer = Tokenizer(inputCol="review_concat", outputCol="words")
    positive_reviews_df = tokenizer.transform(positive_reviews_df)

    positive_reviews_df = positive_reviews_df.withColumn("words_explode",functions.explode(positive_reviews_df.words)).select('words_explode')
    positive_reviews_df = positive_reviews_df.withColumn('pos', textblob(positive_reviews_df.words_explode))


if __name__ == '__main__':

    #inputs = utilities.COMPLETE_PARQUET_DATAPATH
    inputs = "D:\\development\\bigdata\\amzn\\sampledata"
    spark = SparkSession.builder.appName('Spark Cassandra load logs').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)
