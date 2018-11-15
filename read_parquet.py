import sys
import os
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, from_unixtime,broadcast,udf,year
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

    gift_cards_df = spark.read.parquet(inputs)
    gift_cards_df.show()

    """

    raw_dataset = spark.read.option('sep','\t').csv(inputs,schema=amazon_schema,header='true')
    #raw_dataset = raw_dataset.repartition(40).cache()
    
    raw_dataset.show()
    
    print("No of rows in raw_dataset:",raw_dataset.count())

    #Keeping only those rows which are verified purchases
    verified_purchases_df = raw_dataset.filter(col('verified_purchase')=="Y")
    verified_purchases_df.show()
    print("No of rows in verified_purchases_df:",verified_purchases_df.count())

    
    #10-core products only - Keeping only the products which have more than 10 reviews
    product_count = verified_purchases_df.groupby('product_id').count().filter(col('count')>10)
    #product_count.show()
    #print("No of Unique Products:",product_count.count())
    ten_core_dataset = verified_purchases_df.join(broadcast(product_count.select('product_id')),on='product_id')
    ten_core_dataset.registerTempTable('ten_core_dataset')
    ten_core_dataset.show()

    print("No of rows in ten_core_dataset:",ten_core_dataset.count())

    
    # #Converting given time to datetime format
    # dateUDF = udf(lambda reviewTime: datetime.strptime(reviewTime,"%m %d, %Y"),types.DateType())
    # transformed_date_df = raw_dataset.select('reviewerID','asin','reviewerName','helpful','reviewText','overall','summary',dateUDF(col('reviewTime')).alias('date_time'))
    # transformed_date_df.registerTempTable('transformed_data')
    # print("No of rows in transformed_date_df:",transformed_date_df.count())

    #Selecting data in the given time range
    sliced_data = spark.sql("SELECT * from ten_core_dataset WHERE year(review_date) BETWEEN "+start_year+" AND "+end_year)
    sliced_data.show()

    print("No of rows in sliced_dataset:",sliced_data.count())

    sliced_data.write.partitionBy('product_category').parquet(output)
    #final_df.write.json(output)
    """
if __name__ == '__main__':
    inputs = sys.argv[1]
    # output = sys.argv[2]
    # start_year = sys.argv[3]
    # end_year = sys.argv[4]
    main(inputs)

