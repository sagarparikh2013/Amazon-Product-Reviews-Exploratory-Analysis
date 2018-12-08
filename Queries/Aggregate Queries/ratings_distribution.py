import sys
from pyspark.sql import SparkSession, functions
import utilities
import matplotlib.pyplot as plt


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
    plt.savefig('../../figures/ratings_distribution.png')


if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata\\sample_us.tsv"
    spark = SparkSession.builder.appName('Ratings Distribution').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()

    main(inputs)
