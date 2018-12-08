import sys
import os
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, from_unixtime,broadcast,udf,year,countDistinct,date_format,count
import pandas as pd
# %matplotlib inline
import random
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName('Queries on Reviews').getOrCreate()
#assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext
sc.setLogLevel('WARN')



def main(inputs):
    

    input_df = spark.read.parquet(inputs)
    input_df = input_df.repartition(96).cache()
    #input_df.show()
    #print("No of rows in input dataset:",inputs," is:",input_df.count())

    #Total number of reviewers over all these years:
    #print("Total number of unique reviewers: ",input_df.select('customer_id').distinct().count())
    
    #Total number of products over all these years:
    #print("Total number of unique products: ",input_df.select('product_id').distinct().count())
    
    #Total number of products in each category:
    products_per_category = input_df.groupBy('product_category').agg(countDistinct(col('product_id')).alias('Unique Products Count'))
    #products_per_category.show()

    #Reviews count per year
    reviews_count_vs_years = input_df.groupBy(year('review_date').alias('year')).count().orderBy('year')
    #reviews_count_vs_years.show()
    reviews_count_vs_years.write.csv('reviews_count_vs_years_'+inputs)
    reviews_count_vs_years_plot = reviews_count_vs_years.toPandas().plot(x='year',y='count').get_figure()
    #reviews_count_vs_years_plot.savefig('figures/reviews_count_vs_years_'+inputs+'.png')

    #Reviews on each day of the week
    counts_per_day_of_week = input_df.groupBy(date_format('review_date','EEEE').alias('dayOfWeek')).count()
    counts_per_day_of_week.show()
    day_with_max_reviews = counts_per_day_of_week.first()
    file_name = 'day_with_max_views'
    counts_per_day_of_week.write.csv(file_name) 
    
if __name__ == '__main__':
    
    inputs = sys.argv[1]
    main(inputs)

