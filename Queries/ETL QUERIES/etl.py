import sys
import os
import time
from datetime import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, from_unixtime,broadcast,udf,year
#Broadcast timeout given to prevent timeout of large joins - currently 10 hrs but should be increased if data increases exponentially to make it scalable
spark = SparkSession.builder.appName('Amazon Product Categories ETL').config("spark.sql.broadcastTimeout", 36000).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')



def main(inputs,output,start_year,end_year):
    
    #Specifying the schema for the dataframe
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

    #Loading the data into dataframe
    raw_dataset = spark.read.option('sep','\t').csv(inputs,schema=amazon_schema,header='true')
    raw_dataset = raw_dataset.repartition(96)
    #print("No of rows in raw_dataset:",raw_dataset.count())

    #Keeping only those rows which are verified purchases
    verified_purchases_df = raw_dataset.filter(col('verified_purchase')=="Y").cache()
    #print("No of rows in verified_purchases_df:",verified_purchases_df.count())

    
    #10-core products only - Keeping only the products which have more than 10 reviews
    product_count = verified_purchases_df.groupby('product_id').count().filter(col('count')>10)
    ten_core_dataset = verified_purchases_df.join(broadcast(product_count.select('product_id')),on='product_id')
    ten_core_dataset.registerTempTable('ten_core_dataset')
    #print("No of rows in ten_core_dataset:",ten_core_dataset.count())
    #Selecting data in the given time range
    sliced_data = spark.sql("SELECT * from ten_core_dataset WHERE year(review_date) BETWEEN "+start_year+" AND "+end_year)
    #sliced_data = spark.sql("SELECT * from ten_core_dataset WHERE year(review_date) BETWEEN "+str(2010)+" AND "+str(2015))
    #sliced_data.registerTempTable("sliced_data")
    print("No of rows in sliced_dataset:",sliced_data.count())

    #splitting the datasets year-wise
    # years_year=[2010,2011,2012,2013,2014,2015]
    # for i in years_year:
    #     split_to_years = spark.sql("SELECT * from sliced_data WHERE year(review_date)="+str(i))
    #     print("No of rows in",i,split_to_years.count())
    #     split_to_years.write.partitionBy('product_category').parquet(output+"_"+str(i))

    #Storing the data partitioned on product categories for easy access later on
    sliced_data.write.partitionBy('product_category').parquet(output)
    
    
if __name__ == '__main__':

    inputs = sys.argv[1]
    output = sys.argv[2]
    start_year = sys.argv[3]
    end_year = sys.argv[4]
    main(inputs,output,start_year,end_year)

