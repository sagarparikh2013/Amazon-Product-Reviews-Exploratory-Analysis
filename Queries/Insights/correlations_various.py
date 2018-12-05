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

    helpful_df = reviews_df.filter(reviews_df.product_id.isNotNull()).filter(reviews_df.total_votes > 1).filter(reviews_df.helpful_votes > 0).cache()

    helpful_df = helpful_df.withColumn('helpfulness_percentage', (helpful_df.helpful_votes / helpful_df.total_votes) * 100)
    helpful_df = helpful_df.withColumn('length_words', word_count(reviews_df.review_body)).select('helpfulness_percentage','star_rating', 'length_words').cache()

    helpful_star_rating_corr = helpful_df.corr('helpfulness_percentage','star_rating')
    length_rating_corr = helpful_df.corr('length_words', 'star_rating')
    star_length_corr = helpful_df.corr('helpfulness_percentage', 'length_words')

    print(helpful_star_rating_corr)
    print(length_rating_corr)
    print(star_length_corr)




if __name__ == '__main__':

    inputs = utilities.COMPLETE_PARQUET_DATAPATH
    #inputs = "D:\\development\\bigdata\\amzn\\sampledata\\sample_us.tsv"
    spark = SparkSession.builder.appName('Helpfulness query').getOrCreate()
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    assert spark.version >= '2.3'  # make sure we have Spark 2.3+

    main(inputs)
