from pyspark import SparkConf, SparkContext
import sys
import random
import operator
from pyspark.sql import SparkSession, functions, types
import json
import utilities


def tokenize_pos(rdd_element):
    sys.path.append('/home/cahuja/.local/lib/python3.5/site-packages')
    import nltk
    nltk.data.path.append('/home/cahuja/nltk_data')
    lst = nltk.pos_tag(nltk.word_tokenize(rdd_element))
    return lst


def save_adjectives(df, filename):
    df = df.select('review_headline', 'review_body').withColumn('review_concat',
                                                                functions.concat('review_headline', functions.lit(" "),
                                                                                 'review_body')).select(
        'review_concat').dropna()

    rd = df.rdd
    rd = rd.map(lambda x: x[0])
    rd = rd.map(lambda x: x.lower())
    rd = rd.map(tokenize_pos).flatMap(lambda x: x).filter(lambda x: x[1] == 'JJ')

    df = spark.createDataFrame(rd, ['adjectives', 'pos']).select('adjectives')
    df = df.groupBy('adjectives').agg(functions.count('adjectives'))
    df.show()
    df.repartition(1).write.mode('overwrite').csv(filename)


def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark)
    # nltk.data.path.append('/home/youruserid/nltk_data')
    # reviews_df = spark.read.parquet(inputs)
    reviews_df = reviews_df.filter(reviews_df.product_id.isNotNull())

    positive_reviews_df = reviews_df.filter(reviews_df.star_rating > 3)
    negative_reviews_df = reviews_df.filter(reviews_df.star_rating < 3)
    neutral_reviews_df = reviews_df.filter(reviews_df.star_rating == 3)

    save_adjectives(positive_reviews_df, 'positive_adjectives')
    save_adjectives(negative_reviews_df, 'negative_adjectives')
    save_adjectives(neutral_reviews_df, 'neutral_adjectivees')


if __name__ == '__main__':
    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    # inputs = "/user/parikh/etl_final_1995_2000"
    spark = SparkSession.builder.appName('Spark Cassandra load logs').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)


