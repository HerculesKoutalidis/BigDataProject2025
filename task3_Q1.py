#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col, when, lit, isnull, udf
from pyspark.sql.types import StringType

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
    .appName("RDD query 1 execution") \
    .getOrCreate() \
    #.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
spark.sparkContext.setLogLevel("ERROR")

#%% Load both Parquet files into DataFrames
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

#%%
# Combine DataFrames and convert to RDD
combined_rdd = df_2010_2019.union(df_2020_2025).rdd
#%%
# Filter rows with "AGGRAVATED ASSAULT" in the crime description
aggravated_assault_rdd = combined_rdd.filter(
    lambda row: "AGGRAVATED ASSAULT" in row["Crm Cd Desc"]
)

# %%
# Function to categorize age into groups
def age_group(row):
    try:
        age = int(row["Vict Age"])
    except:
        return None  # Skip invalid ages 
    if age < 18:
        return "Children"
    elif 18 <= age <= 24:
        return "Youngsters"
    elif 25 <= age <= 64:
        return "Adults"
    elif age >= 65:
        return "Elderly"
    else:
        return None  # Skip negative ages
# %%
# Map each valid entry to (age_group, 1) and filter out invalid entries
age_group_rdd = aggravated_assault_rdd.map(
    lambda row: (age_group(row), 1)
).filter(
    lambda x: x[0] is not None)  # drop any records where age_bucket returned None


#%%
# Sum counts for each age group
result_rdd = age_group_rdd.reduceByKey(lambda a, b: a + b)\
             .sortBy(lambda x: x[1], ascending=False)
#%%
# Collect and display results
results = result_rdd.collect()
print("Query 1: using RDDs")
for agegroup, count in results:
    print(f"{agegroup}: {count}")





#%%========================================================================
#============ Using DataFrames ===================================================
spark = SparkSession.builder \
    .appName("DataFrames query 1 execution") \
    .getOrCreate() \
    #.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
spark.sparkContext.setLogLevel("ERROR")

#%% Load both Parquet files into DataFrames and combine them
df = df_2010_2019.union(df_2020_2025)

# Filter for "AGGRAVATED ASSAULT" crimes and valid ages
result = df \
    .filter(col("Crm Cd Desc").contains("AGGRAVATED ASSAULT")) \
    .withColumn("Vict Age", col("Vict Age").cast("integer")) \
    .filter(
        (~isnull(col("Vict Age"))) &  # Remove rows with invalid/non-integer ages
        (col("Vict Age") >= 0)        # Remove negative ages
    ) \
    .withColumn(
        "age_group",
        when(col("Vict Age") < 18, "Children")
        .when((col("Vict Age") >= 18) & (col("Vict Age") <= 24), "Youngsters")
        .when((col("Vict Age") >= 25) & (col("Vict Age") <= 64), "Adults")
        .when(col("Vict Age") >= 65, "Elderly")
        .otherwise(lit(None))  # Explicitly handle edge cases (optional)
    ) \
    .filter(col("age_group").isNotNull()) \
    .groupBy("age_group") \
    .count() \
    .orderBy(col("count").desc())  # Sort by count descending

# Show the result
print("Query 1: using DataFrames")
result.show()



#%%========================================================================
#============ Using DataFrames & UDF ===================================================
spark = SparkSession.builder \
    .appName("DataFrames query 1 with udf execution") \
    .getOrCreate() \
    #.sparkContext

# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
spark.sparkContext.setLogLevel("ERROR")

# Define the UDF for age grouping
def categorize_age_udf(age):
    if age is None:
        return None
    if age < 18:
        return "Children"
    elif 18 <= age <= 24:
        return "Youngsters"
    elif 25 <= age <= 64:
        return "Adults"
    elif age >= 65:
        return "Elderly"
    else:
        return None  # Handles edge cases (e.g., negative values)
    
# Register the UDF
categorize_age = udf(categorize_age_udf, StringType())
combined_df = df_2010_2019.union(df_2020_2025)

# Process data
result = combined_df.filter(
    col("Crm Cd Desc").contains("AGGRAVATED ASSAULT")  # Filter for aggravated assaults
).withColumn(
    "Vict Age", col("Vict Age").cast("integer")  # Cast age to integer
).filter(
    (col("Vict Age").isNotNull()) & (col("Vict Age") >= 0)  # Remove invalid/negative ages
).withColumn(
    "age_group", categorize_age(col("Vict Age"))  # Apply the UDF to create age_group
).filter(
    col("age_group").isNotNull()  # Remove entries with unclassified age groups
).groupBy(
    "age_group"  # Group by age category
).count().orderBy(
    col("count").desc()  # Sort by count descending order of aggravated assaults
)

# Display results
print("Query 1: using DataFrames with user defined function (udf)")
result.show()


# %%
