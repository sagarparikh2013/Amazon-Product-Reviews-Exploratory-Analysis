import sys
import os
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, from_unixtime,broadcast,udf,year,countDistinct,date_format,count
spark = SparkSession.builder.appName('Read Parquets S3 Categories ').getOrCreate()
#assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext
sc.setLogLevel('WARN')



def main(inputs):
    
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

    input_df = spark.read.parquet(inputs).cache()
    #input_df.show()
    print("No of rows in input dataset:",inputs," is:",input_df.count())

    #Total number of reviewers over all these years:
    print("Total number of unique reviewers: ",input_df.select('customer_id').distinct().count())
    
    #Total number of products over all these years:
    print("Total number of unique products: ",input_df.select('product_id').distinct().count())
    
    #Total number of products in each category:
    products_per_category = input_df.groupBy('product_category').agg(countDistinct(col('product_id')).alias('Unique Products Count'))
    products_per_category.show()

    ratings_count_vs_years = input_df.groupBy(year('review_date').alias('year')).count()
    ratings_count_vs_years.show()


if __name__ == '__main__':
    
    inputs = sys.argv[1]
    main(inputs)

