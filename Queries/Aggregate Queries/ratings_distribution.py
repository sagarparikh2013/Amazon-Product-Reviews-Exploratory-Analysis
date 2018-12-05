import sys
from pyspark.sql import SparkSession, functions
import utilities
import matplotlib.pyplot as plt

assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+


def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark)
    #reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA)
    reviews_df = reviews_df.filter(reviews_df.product_id.isNotNull()).select(reviews_df.star_rating).cache()
    ratings_dict = reviews_df.groupBy('star_rating').agg(functions.count('star_rating').alias('count')).rdd.collectAsMap()

    x_array = list(ratings_dict.keys())
    y_array = list(ratings_dict.values())

    plt.bar(x_array, y_array)
    plt.xlabel('Ratings of products on Amazon')
    plt.ylabel('count')
    plt.title('Count of each rating')
    plt.show()
    plt.savefig('ratings_distribution.png')


if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata\\sample_us.tsv"
    spark = SparkSession.builder.appName('Spark Cassandra load logs').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)
