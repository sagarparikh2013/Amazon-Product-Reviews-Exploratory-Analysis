import sys
from pyspark.sql import SparkSession, functions, types
import utilities
import matplotlib.pyplot as plt
import re, string
import collections

assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+


def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark)
    #reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA)
    reviews_df.cache()

    product_count_df = reviews_df.filter(reviews_df.customer_id.isNotNull()).groupBy('product_category').agg(functions.countDistinct('product_id'))
    product_count_df.repartition(1).write.mode('overwrite').csv('product_count_categorywise')
    product_count = product_count_df.rdd.collectAsMap()
    aggregated_dict = collections.OrderedDict(sorted(product_count.items(), reverse=True))

    x_values_high = list(aggregated_dict.keys())
    labels_high = list(aggregated_dict.values())
    y_values_high = range(len(labels_high))

    plt.barh(y_values_high, labels_high, color='g')
    plt.yticks(y_values_high, x_values_high)

    plt.xlabel('Number of distinct products')
    plt.ylabel('Category')
    plt.title('Product count across categories')
    plt.show()

if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata"
    spark = SparkSession.builder.appName('Helpfulness query').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+
    spark.sparkContext.setLogLevel('WARN')

    main(inputs)
