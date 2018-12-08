Queries:

ETL QUERIES:
1) This script loads the raw dataset from the cluster, performs **ETL** on it, and stores it on the cluster in Parquet format partitioning on product_category. The given configuration enables the script to fully utilize the compute power of the cluster (given that it is not used by other programs) and it should be changed as per cluster specifications: **etl.py**

```time spark-submit --conf spark.dynamicAllocation.enabled=false --num-executors=96 --executor-cores=16 --executor-memory=100G new_etl.py /user/parikh/amazon_raw_datasets /user/parikh/etl_final_1995_2015 1995 2015 >> final_logs_with_broadcast.txt```

Considering this will be a long running process, it is a good idea to store the logs in a text file to analyse later.

2) 
Basic Queries over smaller subsets of data:
time spark-submit reviews_day_of_week.py etl_final_2000_2005

Query for converting parquet to CSV category wise:
time spark-submit parquet_to_csv_single_category.py "etl_final_1995_2015/product_category=Gift Card" csv_GF_1995_2015 "Gift Card"

Special Days Count:
spark-submit special_days_count.py "etl_final_1995_2015/product_category=Electronics" 1995 2015 "Electronics"


