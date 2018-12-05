import sys
from pyspark.sql import SparkSession, functions, types
import utilities
import matplotlib.pyplot as plt
import re
import string

assert sys.version_info >= (3, 5)   # make sure we have Python 3.5+

wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))

@functions.udf(returnType=types.LongType())
def word_count(review):
    count = len(re.findall(wordsep, str(review)))
    return count

def main(inputs):
    reviews_df = utilities.get_completereviews_dataframe(spark)
    #reviews_df = spark.read.csv(sep='\t', path=inputs, schema=utilities.REVIEWS_SCHEMA, encoding='utf-8')
    reviews_df.cache()

    reviews_filtered = reviews_df.filter(reviews_df.product_id.isNotNull()).filter(reviews_df.total_votes > 1).filter(reviews_df.helpful_votes > 0)

    helpful_df = reviews_filtered.withColumn('helpfulness_percentage', (reviews_filtered.helpful_votes / reviews_filtered.total_votes) * 100).select('customer_id','helpfulness_percentage').cache()

    aggregations = [functions.count('*').alias('num_reviews'), functions.mean('helpfulness_percentage').alias('mean_helpfulness_percentage')]

    reviews_count_df = helpful_df.groupBy('customer_id').agg(*aggregations)

    reviews_count_df = reviews_count_df.filter(reviews_count_df.num_reviews > 150).filter(reviews_count_df.mean_helpfulness_percentage > 90)

    reviews_count_df.sort('num_reviews', ascending = False).repartition(1).write.mode('overwrite').csv('influential_reviewrs')

if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata"
    spark = SparkSession.builder.appName('Helpfulness query').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)
