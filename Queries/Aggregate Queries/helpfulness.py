import sys
from pyspark.sql import SparkSession, functions
import utilities
import matplotlib.pyplot as plt

assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+


def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark)
    #reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA)
    #Removing rows which have total votes or helpful votes 0 to avoid divide by zero error
    helpful_df = reviews_df.filter(reviews_df.product_id.isNotNull()).filter(reviews_df.total_votes > 1).filter(reviews_df.helpful_votes > 0).cache()

    helpful_df = helpful_df.withColumn('helpfulness_percentage', (helpful_df.helpful_votes / helpful_df.total_votes) * 100)
    helpful_df = helpful_df.select('helpfulness_percentage','product_category')

    helpful_df = helpful_df.groupBy('product_category').agg(functions.mean(helpful_df.helpfulness_percentage).alias('mean_helpfulness_percent'))
    helpful_df.write.mode('overwrite').csv('category_mean_helpfulness')

    #Collecting is safe since we know the result is a very small dataset - rows = no of product categories
    aggregated_dict_high = helpful_df.sort(helpful_df.mean_helpfulness_percent, ascending=False).rdd.collectAsMap()

    x_values_high = list(aggregated_dict_high.keys())
    labels_high = list(aggregated_dict_high.values())
    y_values_high = range(len(labels_high))

    plt.barh(y_values_high, labels_high)
    plt.yticks(y_values_high, x_values_high)
    plt.xlabel('Helpfulness percentage of an average review')
    plt.ylabel('Categories')
    plt.title('Categories with most helpful reviews')

    plt.show()

if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata"
    spark = SparkSession.builder.appName('Helpfulness query').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    main(inputs)
