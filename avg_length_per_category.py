import sys
from pyspark.sql import SparkSession, functions, types
import utilities
import matplotlib.pyplot as plt
import re, string
import collections


# command to run : spark-submit --conf spark.rpc.askTimeout=800 misc_queries.py
assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

@functions.udf(returnType=types.LongType())
def word_count(review):
    count = len(re.findall(wordsep, str(review)))
    return count

def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark).dropna()
    #reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA).dropna()
    reviews_df.cache()

    reviews_df = reviews_df.filter(reviews_df.product_id.isNotNull()).withColumn('length_words', word_count(reviews_df.review_body))
    avg_length_df = reviews_df.groupBy('product_category').agg(functions.avg('length_words').alias('avg_length_words')).select('product_category','avg_length_words').cache()
    avg_length_df.repartition(1).write.mode('overwrite').csv('avg_length_per_category')

    avg_length_dict = avg_length_df.rdd.collectAsMap()
    avg_length_ddict = collections.OrderedDict(sorted(avg_length_dict.items(), reverse=True))

    x_values_high = list(avg_length_ddict.keys())
    labels_high = list(avg_length_ddict.values())
    y_values_high = range(len(labels_high))

    plt.barh(y_values_high, labels_high, color='g')
    plt.yticks(y_values_high, x_values_high)

    plt.xlabel('Average Review Length in words')
    plt.ylabel('Category')
    plt.title('Average Length across categories')
    plt.show()


if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata"
    spark = SparkSession.builder.appName('Helpfulness query').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)

