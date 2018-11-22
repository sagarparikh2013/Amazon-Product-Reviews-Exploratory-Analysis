import sys
import os



#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
# from datetime import datetime
import numpy as np
import pandas as pd
from os import path
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt


from dateutil.parser import parse
import datetime
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, from_unixtime,broadcast,udf,year,countDistinct,date_format,count
spark = SparkSession.builder.appName('Read Parquets S3 Categories ').getOrCreate()
#assert spark.version >= '2.3' # make sure we have Spark 2.3+
sc = spark.sparkContext
sc.setLogLevel('WARN')

def main(inputs,start_year,end_year):
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
	input_df.registerTempTable("input_df")
	input_df.show()




	get_reviews= spark.sql("SELECT review_body FROM input_df")
	get_reviews.show()
	print(get_reviews.count())
	text_review=get_reviews.collect()

	
	wordcloud = WordCloud(max_font_size=50, max_words=100, background_color="black").generate(str(text_review))
	plt.figure()
	plt.imshow(wordcloud, interpolation="bilinear")
	plt.axis("off")
	plt.show()
	wordcloud.to_file("img/first_review.png")


if __name__ == '__main__':
	inputs = sys.argv[1]
	start_year=int(sys.argv[2])
	end_year=int(sys.argv[3])
	main(inputs,start_year,end_year)