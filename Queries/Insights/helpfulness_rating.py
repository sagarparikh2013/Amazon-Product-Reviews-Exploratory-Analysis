import sys
from pyspark.sql import SparkSession, functions
import utilities
import matplotlib.pyplot as plt

assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+


def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark)
    #reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA)
    reviews_df.cache()
    helpful_df = reviews_df.filter(reviews_df.product_id.isNotNull()).filter(reviews_df.total_votes > 1).filter(reviews_df.helpful_votes > 0).cache()

    helpful_df = helpful_df.withColumn('helpfulness_percentage', (helpful_df.helpful_votes / helpful_df.total_votes) * 100)
    helpful_df = helpful_df.select('helpfulness_percentage','star_rating')

    helpful_df = helpful_df.groupBy('star_rating').agg(functions.mean(helpful_df.helpfulness_percentage).alias('mean_helpfulness_percent')).select('star_rating', 'mean_helpfulness_percent').cache()
    helpful_df.repartition(1).write.mode('overwrite').csv('helpfulness_vs_rating')

    avg_helpfulness_dict = helpful_df.coalesce(1).rdd.collectAsMap()

    x_array = list(avg_helpfulness_dict.keys())
    y_array = list(avg_helpfulness_dict.values())

    plt.bar(x_array, y_array, color='g')
    plt.xlabel('Ratings')
    plt.ylabel('Average helpfulness')
    plt.title('Avgerage helpfuless vs rating')
    plt.show()

if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata"
    spark = SparkSession.builder.appName('Helpfulness query').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)
