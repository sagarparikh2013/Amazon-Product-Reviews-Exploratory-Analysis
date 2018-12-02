import sys
import os
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, from_unixtime,broadcast,udf,year
spark = SparkSession.builder.appName('Read Parquets/CSVs').getOrCreate()
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

    #For reading CSV input files partitioned on Product Categories
    #input_df = spark.read.option('sep','\t').csv(inputs,header='true')

    #For reading Parquet input files partitioned on Product Categories
    input_df = spark.read.parquet(inputs)
    input_df = input_df.repartition(96) 
    #input_df.write.partitionBy('product_category').option("header","true").csv("csv_1995_2000")
    

    input_df.show()
    #final_df = input_df.withColumn("product_category",functions.lit('Electronics'))
    #final_df.show()
    #final_df.write.option('header','true').csv('electronics_csv')
    #print("No of rows in input dataset:",inputs," is:",input_df.count())

if __name__ == '__main__':
    inputs = sys.argv[1]
    # output = sys.argv[2]
    # start_year = sys.argv[3]
    # end_year = sys.argv[4]
    main(inputs)

