import sys
import requests
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, from_unixtime,broadcast,udf,year
spark = SparkSession.builder.appName('Sentiment Review').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')


def main(inputs):
    """
    amazon_schema = types.StructType([
    types.StructField('marketplace',types.StringType()),
    types.StructField('customer_id',types.IntegerType()),
    types.StructField('review_id',types.StringType()),
    types.StructField('product_id',types.StringType()),
    types.StructField('product_parent',types.LongType()),
    types.StructField('product_title',types.StringType()),
    types.StructField('product_category',types.StringType()),
    types.StructField('star_rating',types.IntegerType()),
    types.StructField('helpful_votes',types.IntegerType()),
    types.StructField('total_votes',types.IntegerType()),
    types.StructField('vine',types.StringType()),
    types.StructField('verified_purchase',types.StringType()),
    types.StructField('review_headline',types.StringType()),
    types.StructField('review_body',types.StringType()),
    types.StructField('review_date',types.DateType())])
    """
    input_df = spark.read.parquet(inputs)
    input_df.show()
    print("No of rows in input_dataset:",input_df.count())
    url = "http://text-processing.com/api/sentiment/"
    """
    curl -d "text=terrible" http://text-processing.com/api/sentiment/
    """
    sentimentUDF = udf(lambda review: requests.post(url,data={"text":review}).json()["label"],types.StringType())
    df_with_sentiments = input_df.withColumn("sentiment",sentimentUDF(col('review_body')))
    df_with_sentiments.show()
    print("No of rows in sntiment_df:",df_with_sentiments.count())
if __name__ == '__main__':
    inputs = sys.argv[1]
    # output = sys.argv[2]
    # start_year = sys.argv[3]
    # end_year = sys.argv[4]
    main(inputs)
