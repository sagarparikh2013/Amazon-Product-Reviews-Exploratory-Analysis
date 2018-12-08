import sys
import os
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
spark = SparkSession.builder.appName('WordCloud').getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')

def main(inputs,category):
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
	# input_df = input_df.repartition(96).cache()
	input_df.registerTempTable("input_df")
	get_reviews= spark.sql("SELECT review_body FROM input_df")

	text_review=get_reviews.collect()

	stopwords = set(STOPWORDS)
	stopwords.update(["take","look","put","around","Row","review_body","br","quot","although","now","one","give","saying","say","well","new","made","much","make","got","each","take","use","may","without","part","want","makes","even","many","used","yet","going","set","come","go","found","another","seem"])
	stopwords.update(['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're", "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's", 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn', "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"])

	#Add Details for Cloud
	font_path = "fonts/forbes_bold.ttf"
	image_name = category+"_wordcloud.png"
	#cloud_mask = np.array(Image.open(image_name))
	width=1500
	height=1000
	max_words=150
	background_color="white"
	contour_width=0
	min_font_size=4
	contour_color="black"
	relative_scaling=0
	mode="RGB"
	save_as_name="figures/"+image_name

	wordcloud = WordCloud(relative_scaling=relative_scaling,min_font_size=min_font_size,mode=mode,font_path=font_path,width=width,height=height,stopwords=stopwords, max_words=max_words, background_color=background_color,contour_width=contour_width,contour_color=contour_color).generate(str(text_review))
	plt.figure()
	plt.imshow(wordcloud, interpolation="bilinear")
	plt.axis("off")
	plt.show()
	wordcloud.to_file(save_as_name)

	# #Provides new image similar to the background image
	# image_colors = ImageColorGenerator(cloud_mask)
	# plt.imshow(wordcloud.recolor(color_func=image_colors), interpolation="bilinear")
	# plt.axis("off")
	# plt.savefig("figures/.png", format="png")
	# plt.show()


if __name__ == '__main__':
	inputs = sys.argv[1]
	category = sys.argv[2]
	main(inputs,category)