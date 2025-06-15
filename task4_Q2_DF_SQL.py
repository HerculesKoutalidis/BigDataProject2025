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
#============ Using SQL API ===================================================
spark = SparkSession \
    .builder \
    .appName("Query 2 execution with Spark SQL API") \
    .getOrCreate() \
    #.sparkContext

sc = spark.sparkContext
# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

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


# Create temporary views with explicit column name quoting
df_2010_2019.createOrReplaceTempView("dataset1")
df_2020_2025.createOrReplaceTempView("dataset2")

#%%
# Execute SQL query
query = """
    WITH combined_data AS (
        SELECT * FROM dataset1
        UNION ALL
        SELECT * FROM dataset2
    ),
    processed_data AS (
        SELECT 
            YEAR(TO_TIMESTAMP(`Date Rptd`, 'MM/dd/yyyy hh:mm:ss a')) AS year,
            `AREA NAME`,
            CASE 
                WHEN Status NOT IN ('UNK', 'IC') THEN 1 
                ELSE 0 
            END AS is_closed
        FROM combined_data
    ),
    aggregated_data AS (
        SELECT 
            year,
            `AREA NAME`,
            COUNT(*) AS total_crimes,
            SUM(is_closed) AS total_closed,
            SUM(is_closed)/COUNT(*) AS closed_rate
        FROM processed_data
        GROUP BY year, `AREA NAME`
    ),
    ranked_data AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY year 
                ORDER BY closed_rate DESC
            ) AS rank
        FROM aggregated_data
    )
    SELECT 
        year,
        `AREA NAME`,
        closed_rate,
        rank
    FROM ranked_data
    WHERE rank <= 3
    ORDER BY year, rank
"""

result = spark.sql(query)

# Show all results without truncation
print('Q2: With Spark SQL API')
result.show(n=result.count(), truncate=False)