To get the actual dataset:
Run the following code in the directory that contains dataset_list.txt:

```
wget -i datasets_list.txt
```

The following commands were entered to get the programs running and achieve the intended output.

**_Queries:-_**

**Queries/ETL QUERIES:**

1) *etl.py*
This script loads the raw dataset from the cluster, performs **ETL** on it, and stores it on the cluster in Parquet format partitioning on product_category. The given configuration enables the script to fully utilize the compute power of the cluster (given that it is not used by other programs) and it should be changed as per cluster specifications: 

```
time spark-submit --conf spark.dynamicAllocation.enabled=false --num-executors=96 --executor-cores=16 --executor-memory=100G etl.py /user/parikh/amazon_raw_datasets /user/parikh/etl_final_1995_2015 1995 2015 >> final_logs_with_broadcast.txt
```
Considering this will be a long running process, it is a good idea to store the logs in a text file to analyse later.

2) *parquet_to_csv.py*
Script to convert processed dataset stored in parquet format to tsv format for visualization in Tableau. 
```
spark-submit parquet_to_csv.py /user/parikh/etl_final_1995_2015 /user/parikh/csv_all_1995_2015  
```
There is no direct api call in spark to write dataframes in tsv, so the dataframes must be written in csv mode with tab delimeter and then the extension of the files need to be manually changed to tsv. Concatenating parts of files in a single file might also be preffered through a command like this
```
hdfs dfs -cat csv_all_1995_2015/product_category=Books/part* > books.csv
```

3) *parquet_to_csv_single_category.py*
Same as **2**, but works for single categories. This way specific categories can be converted to visualize trends and save time.
```
spark-submit parquet_to_csv.py "/user/parikh/etl_final_1995_2015/product_category=Watches" /user/parikh/csv_Watches_1995_2015 "Watches"
```
As partitionBy was used in ETL process, loading a particular category does not include the product category column name and thus for single category conversion, category name is manually added from the command line.


-------------------------------------------

**Queries/Insights**

1) *helpfulness_rating.py*
Helpfulness of reviews for each rating
```
spark-submit helpfulness_rating.py
``` 

2) *length_review_vs_rating.py*
Plotting length of review vs rating:
```
spark-submit length_review_vs_rating.py "/user/parikh/etl_final_1995_2015"
```

3) *reviews_day_of_week.py*
Intresting insights into the dataset:
```
spark-submit reviews_day_of_week.py "/user/parikh/etl_final_1995_2015"
```
These queries can also be ran over smaller subsets of data to save time and see year wise trends:
```
spark-submit reviews_day_of_week.py "/user/parikh/etl_final_2000_2005"
```
4) *special_days_count.py*
Number of reviews on Holidays & it's wordcloud category-wise:
```
spark-submit special_days_count.py "/user/parikh/etl_final_1995_2015/product_category=Electronics" 1995 2015 "Electronics"
```

5) *word_plot.py*
Plotting Adjectives
```
python3 word_plot.py
```

-------------------------------------------

**Queries/Topic Modelling:**
*lda_topic_modelling.py*
Topic modelling over all categories:
```
python3 lda_topic_modelling.py 
```
To run this file, have a bricks folder in your path with etl_final_2k_2k5 subfolder containing reviews data in parquet format
like this: ```path = 'bricks\\etl_final_2k_2k5'```

*topic_modelling_spark.py*
Topic modelling with Spark MLLib 
```
spark-submit topic_modelling_spark.py "/user/parikh/etl_final_1995_2015/product_category=Electronics"
```

-------------------------------------------

**Queries/Aggregate Queries:**

1) *average_rating_score.py*
To get average rating score for all product categories:
```
spark-submit average_rating_score.py
```
2) *adjectives.py*
Positive & Negative adjectives count in review body:
```
spark-submit adjectives.py
```

3) *category_average_rating.py*
To find out category-wise average rating:
```
spark-submit category_average_rating.py
```
4) *helpfulness.py*
To find out helpfulness ratio of reviews per category:
```
spark-submit helpfulness.py
```
5) *ratings_distribution.py*
Ratings distribution from 1 to 5 across all categories: 
```
spark-submit ratings_distribution.py
```
6) *top_reviewers.py*
Find out top influential reviewers across all amazon reviews: outputs customer ids of top reviewers.
```
spark-submit top_reviewers.py
```
7) *correlations_various.py*
Correlations between various calculated features:
```
spark-submit correlations_various.py
```
8) *average_length_per_rating.py*
```
spark-submit average_length_per_rating.py
```
 
9) *avg_length_per_category.py*
```
spark-submit avg_length_per_category.py
```
10) *product_count_categorywise.py*
```
spark-submit product_count_categorywise.py
```

11) *misc_queries.py*

```
spark-submit misc_queries.py
```

-------------------------------------------

**Queries/Wordcloud**

*wordcloud_try.py*
For wordclouds of review bodies of product categories:
```
spark-submit wordcloud_try.py "/user/parikh/etl_final_1995_2015/product_category=Books" "Books"
```

-------------------------------------------
