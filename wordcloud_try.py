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
	text_review=get_reviews.collect()

	stopwords = set(STOPWORDS)
	stopwords.update(["take","Row","review_body","br","quot","although","now","say","well","new","made","much","make","got","each","take","use","may","without","part","want","makes","even","many","used","yet","going","set","come","go","found","another","seem"])

	stopwords.update(['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"])

	

	# transformed_cloud_mask = np.ndarray((cloud_mask.shape[0],cloud_mask.shape[1],cloud_mask.shape[2]), np.int32)

	# for i in range(len(cloud_mask)):
	# 	transformed_cloud_mask[i] = list(map(transform_format, cloud_mask[i]))

	#Add Details for Cloud
	font_path="fonts/forbes_bold.ttf"
	cloud_mask = np.array(Image.open("cloud.png"))
	#Canvas Width
	width=1000
	height=800
	max_words=75
	background_color="black"
	contour_width=40
	contour_color="white"




	wordcloud = WordCloud(font_path=font_path,mask=cloud_mask,width=width,height=height,stopwords=stopwords, max_words=max_words, background_color=background_color,contour_width=contour_width,contour_color=contour_color).generate(str(text_review))
	plt.figure()
	plt.imshow(wordcloud, interpolation="bilinear")
	plt.axis("off")
	plt.show()
	wordcloud.to_file("img/first_review.png")


# def transform_format(val):
# 	if val.all() == 0:
# 		return 255
# 	else:
# 		return val

if __name__ == '__main__':
	inputs = sys.argv[1]
	start_year=int(sys.argv[2])
	end_year=int(sys.argv[3])
	main(inputs,start_year,end_year)