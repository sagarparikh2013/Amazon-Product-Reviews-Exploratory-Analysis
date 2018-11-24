import sys
from pyspark.sql import SparkSession, functions
import utilities
import matplotlib.pyplot as plt

assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+


def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark)
    # reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA)
    reviews_df = reviews_df.filter(reviews_df.product_id.isNotNull())

    aggregated_df = reviews_df.groupBy(reviews_df.product_category).agg(functions.avg('star_rating').alias('average')).cache()
    aggregated_df.repartition(1).write.mode('overwrite').csv('category_wise_average')

    aggregated_dict_high = aggregated_df.sort(aggregated_df.average, ascending=False).rdd.collectAsMap()
    x_values_high = list(aggregated_dict_high.keys())
    labels_high = list(aggregated_dict_high.values())
    y_values_high = range(len(labels_high))

    plt.barh(y_values_high, labels_high)
    plt.yticks(y_values_high, x_values_high, rotation='90')
    plt.xlabel('Star Rating of an average product')
    plt.ylabel('Categories')
    plt.title('Categories wise average ratings')

    # aggregated_dict_low = aggregated_df.sort(aggregated_df.average).rdd.collectAsMap()
    # x_values_low = list(aggregated_dict_low.keys())
    # labels_low = list(aggregated_dict_low.values())
    # y_values_low = range(len(labels_low))
    #
    # plt.subplot(2, 1, 2)
    # plt.barh(y_values_low, labels_low)
    # plt.yticks(y_values_low, x_values_low, rotation='30')
    # plt.xlabel('Star Rating of an average product')
    # plt.ylabel('Categories')
    # plt.title('Categories with lowest average rating')

    plt.show()


if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata"
    spark = SparkSession.builder.appName('Spark Cassandra load logs').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)
