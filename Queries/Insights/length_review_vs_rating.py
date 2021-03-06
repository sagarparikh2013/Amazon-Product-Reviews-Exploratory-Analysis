import sys
import os
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from datetime import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col,count,length
import pandas as pd
# %matplotlib inline
import random
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName('Length of Review vs Ratings').getOrCreate()
#assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext
sc.setLogLevel('WARN')



def main(inputs):
    
    input_df = spark.read.parquet(inputs)
    input_df = input_df.repartition(96).cache()

    length_review_vs_rating_df = input_df.groupBy('star_rating').\
    agg(functions.avg(length(col('review_body'))).alias('avg_length')).orderBy('star_rating')

    
    #length_review_vs_rating_df.write.csv('length_of_review_vs_rating.csv')

    length_review_vs_rating_dict = length_review_vs_rating_df.rdd.collectAsMap()
    
    x_array = list(length_review_vs_rating_dict.keys())
    y_array = list(length_review_vs_rating_dict.values())

    plt.bar(x_array, y_array)
    plt.xlabel('Ratings of products on Amazon')
    plt.ylabel('Average length of review')
    plt.title('Average length of review vs Rating on Amazon')
    plt.savefig('avg_review_length_vs_rating.png')
    plt.show()
    
if __name__ == '__main__':
    
    inputs = sys.argv[1]
    main(inputs)

