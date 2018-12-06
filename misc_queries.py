import sys
from pyspark.sql import SparkSession, functions, types
import utilities
import matplotlib.pyplot as plt
import re, string


# command to run : spark-submit --conf spark.rpc.askTimeout=800 misc_queries.py
assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

@functions.udf(returnType=types.LongType())
def word_count(review):
    count = len(re.findall(wordsep, str(review)))
    return count

def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark).dropna()
    #reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA)
    reviews_df.cache()
    helpful_df = reviews_df.filter(reviews_df.product_id.isNotNull()).filter(reviews_df.total_votes > 1).filter(reviews_df.helpful_votes > 0).cache()
    helpful_df = helpful_df.withColumn('helpfulness_percentage',(helpful_df.helpful_votes / helpful_df.total_votes) * 100)
    helpful_df = helpful_df.filter(helpful_df.helpfulness_percentage > 90).select('review_body')            # take most helpful reviews

    helpful_df = helpful_df.withColumn('word_count', word_count(helpful_df.review_body)).cache()

    ## median of most helpful reviews
    median = helpful_df.approxQuantile('word_count', [0.5], 0.25)
    sc.parallelize(median).repartition(1).saveAsTextFile('median_words_in_helpfulreviews.csv')

    ## average, std of most helpful reviews
    aggregations = [functions.avg('word_count').alias('average_wordcount'), functions.stddev('word_count').alias('std_wordcount')]

    stats_df = helpful_df.agg(*aggregations)
    stats_df.repartition(1).write.mode('overwrite').csv('average_stddev_wordcount_mosthelpful')


    count = reviews_df.filter(reviews_df.customer_id.isNotNull()).select('customer_id').agg(functions.countDistinct('customer_id'))
    count.repartition(1).write.mode('overwrite').csv('total_customersin_dataset')

    product_count = reviews_df.filter(reviews_df.product_id.isNotNull()).select('product_id').agg(functions.countDistinct('product_id'))
    product_count.repartition(1).write.mode('overwrite').csv('total_productsin_dataset')

if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata\\sample_us.tsv"
    spark = SparkSession.builder.appName('Helpfulness query').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)

