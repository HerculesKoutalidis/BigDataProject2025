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
#============ Using RDDs ===================================================
spark = SparkSession \
    .builder \
    .appName("Query 2 execution with RDDs") \
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



# %%
# Combine DataFrames and convert to RDD
combined_rdd = df_2010_2019.union(df_2020_2025)


#%%
#n_statuses = combined_rdd.map(lambda row: row['Status']).distinct().collect()
# Extract the 'Status' column and count occurrences of each distinct value
#status_counts = combined_rdd.map(lambda row: row['Status']).countByValue()

# Display the results
#for status, count in status_counts.items():
#    print(f"Status: {status}, Count: {count}")
#%%
# Convert the combined DataFrame to RDD and process each row. # output RDD entries in the form of ((year, area), (closed, ones to be uses for comp. total))
processed_rdd = combined_rdd.rdd.map(lambda row: (
    int(row['Date Rptd'].split()[0].split('/')[2].strip()),
    row['AREA NAME'].strip(),
    1 if row["Status"] not in ["UNK", "IC"] else 0  # 1=closed, 0=open #case_is_closed(row['Status'])
)).map(lambda x: ((x[0], x[1]), (x[2], 1))).filter(lambda x: x is not None)

# Aggregate closed and total cases per (year, area)
aggregated_rdd = processed_rdd.reduceByKey(lambda year, area: (year[0] + area[0], year[1] + area[1]))

# %%
# Calculate closed rate and restructure data
closed_rate_rdd = aggregated_rdd.map(lambda x: (
        x[0][0],  # year
        x[0][1],  # area
        round(x[1][0] / x[1][1],5),  # closed rate
        #x[1][0],  # closed cases
    ))
#sorted_rdd = closed_rate_rdd.sortBy(lambda x: x[2], ascending=False)
# Collect all unique years from the data
years = closed_rate_rdd.map(lambda x: x[0]).distinct().collect()
years.sort()  # Process years in order

# %%
# Initialize empty RDD with schema: (Year, Area, Rate, Rank)
rdd_combined = sc.emptyRDD()

for year in years: 
    # Filter data for the current year and convert to list
    yearly_data = closed_rate_rdd.filter(lambda x: x[0] == year) #.map(lambda x: (x[0],x[1],x[2]))
        
    # Sort by closed rate (descending) and take top 3
    top_areas = yearly_data.sortBy(lambda x: -x[2])

    # Take top 3 areas
    top3 = top_areas.take(3)

    # Add ranking (1st, 2nd, 3rd) to the results
    ranked_results = []
    for rank, (year, area, rate) in enumerate(top3, start=1):
        ranked_results.append((year, area, rate, rank))
    
    # Convert to RDD and union with combined results
    rdd_combined = rdd_combined.union(sc.parallelize(ranked_results))

#Print the results
rdd_combined_collect = rdd_combined.collect()
print('Q2: With RDDs')
for ele in rdd_combined_collect: print(ele)





'''
#%%========================================================================
#============ Using DataFramess ===================================================
spark = SparkSession \
    .builder \
    .appName("Q2 execution with Spark DataFramess") \
    .getOrCreate() \
    #.sparkContext

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





#%%========================================================================
#============ Using SQL API ===================================================
spark = SparkSession \
    .builder \
    .appName("Q2 execution with Spark SQL API") \
    .getOrCreate() \
    #.sparkContext

sc = spark.sparkContext
# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

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
# %%
'''