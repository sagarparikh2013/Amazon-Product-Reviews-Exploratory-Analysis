import sys
import os
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
# from datetime import datetime
from dateutil.parser import parse
import datetime
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
	input_df.registerTempTable("input_df")
	input_df.show()

	black_friday=["2000-11-24",
				  "2001-11-23",
				  "2002-11-29",
				  "2003-11-28",
				  "2004-11-26",
				  "2005-11-25",
				  "2006-11-24",
				  "2007-11-23",
				  "2008-11-28",
				  "2009-11-27",
				  "2010-11-26",
				  "2011-11-25",
				  "2012-11-23",
				  "2013-11-29",
				  "2014-11-28",
				  "2015-11-27"]

	cyber_monday=["2000-11-27",
				  "2001-11-26",
				  "2002-12-01",
				  "2003-12-01",
				  "2004-11-29",
				  "2005-11-28",
				  "2006-11-27",
				  "2007-11-26",
				  "2008-12-01",
				  "2009-11-30",
				  "2010-11-29",
				  "2011-11-28",
				  "2012-11-26",
				  "2013-12-02",
				  "2014-12-01",
				  "2015-11-30"]

	xmas="-12-25"
	christmas=[]
	for i in range(2000,2016):
		christmas.append(str(i)+xmas)

	rem_day="-11-11"
	remembrance_day=[]
	for i in range(2000,2016):
		remembrance_day.append(str(i)+rem_day)

	new_year_date=["-01-01","-12-31"]
	new_year=[]
	for j in new_year_date:
		for i in range(2000,2016):
			new_year.append(str(i)+j)

	thanksgiving=["2000-10-09",
				  "2001-10-08",
				  "2002-10-14",
				  "2003-10-13",
				  "2004-10-11",
				  "2005-10-10",
				  "2006-10-09",
				  "2007-10-08",
				  "2008-10-13",
				  "2009-10-12",
				  "2010-10-11",
				  "2011-10-10",
				  "2012-10-08",
				  "2013-10-14",
				  "2014-10-13",
				  "2015-10-12"]

	days_name=[black_friday,cyber_monday,christmas,remembrance_day,new_year,thanksgiving]
	days_name_string=["Black Friday","Cyber Monday","Christmas Eve","Remembrance Day","New Year","Thanksgiving Day"]
	y=0	

	for x in days_name:
		query=spark.sql("SELECT * from input_df where {} IN {}".format("review_date",tuple(x)))
		print(days_name_string[y],": ",query.count())
		y=y+1
		query.show()



if __name__ == '__main__':
	
	inputs = sys.argv[1]
	main(inputs)