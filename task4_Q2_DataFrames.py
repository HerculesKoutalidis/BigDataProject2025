#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col, when, lit, isnull, udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window

import os


#%%

#os.environ['HADOOP_USER_NAME'] = 'your_username'
#os.environ['PYSPARK_PYTHON'] = r'C:\Users\hercu\Documents\BigData\VScode\.venv\Scripts\python.exe'
#os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\hercu\Documents\BigData\VScode\.venv\Scripts\python.exe'
#os.environ['SPARK_SUBMIT_OPTS']= r'-Djava.security.manager=allow'
#os.environ['PYTHONUNBUFFERED'] ='1'


#%%========================================================================
#============ Using DataFramess ===================================================
spark = SparkSession \
    .builder \
    .appName("Query 2 execution with Spark DataFramess") \
    .getOrCreate() \
    #.sparkContext

#%% Read both Parquet files 
read_from_hdfs = True
if read_from_hdfs:
    save_dir_root= "hdfs://hdfs-namenode:9000/user/hkoutalidis/data/parquet/"
    LA_crime_data_2010_2019_output_dir = save_dir_root+ r'LA_Crime_Data_2010_2019_output'
    LA_crime_data_2020_2025_output_dir = save_dir_root+r'LA_Crime_Data_2020_2025_output'
    df_2010_2019 = spark.read.parquet(LA_crime_data_2010_2019_output_dir)
    df_2020_2025 = spark.read.parquet(LA_crime_data_2020_2025_output_dir)
else:
    df_2010_2019 = spark.read.parquet("C:/Users/hercu/Documents/BigData/data/LA_Crime_Data_2010_2019_output")
    df_2020_2025 = spark.read.parquet("C:/Users/hercu/Documents/BigData/data/LA_Crime_Data_2020_2025_output")


sc = spark.sparkContext
# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")
combined_df = df_2010_2019.union(df_2020_2025)

# Process the combined DataFrame with corrected timestamp format
processed_df = combined_df.withColumn(
    "timestamp",
    F.to_timestamp(F.col("Date Rptd"), "MM/dd/yyyy hh:mm:ss a")
).withColumn(
    "year",
    F.year("timestamp")
).withColumn(
    "is_closed",
    F.when((F.col("Status") != "UNK") & (F.col("Status") != "IC"), 1).otherwise(0)
)


# %%
# Aggregate to calculate total crimes and closed cases per area and year
aggregated_df = processed_df.groupBy("year", "AREA NAME").agg(
    F.count("*").alias("total_crimes"),
    F.sum("is_closed").alias("total_closed")
).withColumn(
    "closed_rate",  F.col("total_closed") / F.col("total_crimes") 
)
# %%
# Define window specification to rank areas by closed rate within each year
window_spec = Window.partitionBy("year").orderBy(F.desc("closed_rate"))
# %%
# Rank the areas and filter top 3
ranked_df = aggregated_df.withColumn(
    "rank", F.row_number().over(window_spec)
).filter(
    F.col("rank") <= 3
).select(
    "year", "AREA NAME", "closed_rate", "rank"
).orderBy("year", "rank")

#%% Show all results without truncation
print('Q2: With Spark DataFrames')
ranked_df.show(n=ranked_df.count(), truncate=False)