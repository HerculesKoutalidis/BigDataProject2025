#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col, when, lit, isnull, udf,regexp_replace
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.window import Window

import os, time


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
    .appName("Q3-execution with RDDs") \
    .getOrCreate() 
    #.sparkContext

sc = spark.sparkContext
# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

#%% Read both Parquet files 
read_from_hdfs = True
t1_read_parquet= time.time() #Measure time to read parquet files
if read_from_hdfs:
    save_dir_root= "hdfs://hdfs-namenode:9000/user/hkoutalidis/data/parquet/"
    census_2010_output_dir = save_dir_root+ r'2010_Census_Populations_by_Zip_Code_output'
    census_2015_income_output_dir = save_dir_root+r'LA_income_2015_output'
    df_2010_census = spark.read.parquet(census_2010_output_dir)
    df_2025_income = spark.read.parquet(census_2015_income_output_dir)
else:
    df_2010_census = spark.read.parquet("C:/Users/hercu/Documents/BigData/data/2010_Census_Populations_by_Zip_Code_output")
    df_2025_income = spark.read.parquet("C:/Users/hercu/Documents/BigData/data/LA_income_2015_output")
t2_read_parquet= time.time()
read_parquet_time = t2_read_parquet - t1_read_parquet
print('Time to read parquets: ', round(read_parquet_time,5),' s')
#df_2010_census.take(10)
#df_2025_income.take(10)
# %%
#Convert parquets into RDDs, and map them into form (index, [other data]) to join them on Zip Code.
df_2010_census_rdd = df_2010_census.rdd.map(lambda x: [int(x[0]),[x[6]]]   ) #convert the parkquet df object into RDD
df_2025_income_rdd = df_2025_income.rdd.map(lambda x: [int(x[0]), [float(x[2].replace(',','').split('$')[1])] ] )    #Same. Also convert the '$income' into float. 
#Join the 2010 census containing the number of households per Zip Code, and the 2015 census containing median income per household per Zip Code
joined_data = df_2010_census_rdd.join(df_2025_income_rdd)
joined_data_transformed = joined_data.map(lambda x: [x[0], x[1][0][0], x[1][1][0]]) #Simply transform it in cleaner form, removing unnecessary nested lists: (ZipCode, av.household size, av. income per hh)
joined_data_transformed = joined_data_transformed.map(lambda x: [x[0], round(x[2]/x[1],2)]) #Divide income col by av.hh size, and select only Zip Code and income perperson column

#Print a 10 results snipet of the results : (Zip Code, av.income per person)
print('#### Query 3: For every Zip Code find the average est. income per person ####')
print('==========With Spark RDDs=========================')
print('A 10-snipet for Query 3 with RDDs. Format: (Zip Codes, Average income per person)')
joined_data_transformed.take(10)
for entry in joined_data_transformed.take(10): print(entry)

#Filter on rows of RDD to see entries with .filter() of entries (rows)
#result_rdd = joined_data_transformed.filter(lambda x: x[0] == 90011)




#%%========================================================================
#============ Using Spark DataFrames API ===================================================
spark = SparkSession \
    .builder \
    .appName("Q3-execution with Spark DataFrames") \
    .getOrCreate() \
    #.sparkContext

sc = spark.sparkContext
# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")


#%% Read both csv files 
read_from_csv = False

if read_from_csv:
    t1_read_csv= time.time() #Measure time to read from csv
    if read_from_hdfs:
        save_dir_root= "hdfs://hdfs-namenode:9000/user/hkoutalidis/data/parquet/"
        census_2010_output_dir = save_dir_root+ r'2010_Census_Populations_by_Zip_Code_output'
        census_2015_income_output_dir = save_dir_root+r'LA_income_2015_output'
        df_2010_census = spark.read.parquet(census_2010_output_dir)
        df_2025_income = spark.read.parquet(census_2015_income_output_dir)
    else:
        df_2010_census = spark.read.csv("C:/Users/hercu/Documents/BigData/data/2010_Census_Populations_by_Zip_Code.csv", header=True, inferSchema=True)
        df_2025_income = spark.read.csv("C:/Users/hercu/Documents/BigData/data/LA_income_2015.csv", header=True, inferSchema=True)
    t2_read_csv= time.time() 
    read_csv_time = t2_read_csv - t1_read_csv
    print('Time to read csvs: ', round(read_csv_time,5), 's')
    print(f'Time to read csvs is {round(read_csv_time/read_parquet_time,2)} times more than to read parquet files.')

#We do not need to define files schemas, because they are Parquet files, which store the schema internally, and Spark reads it automatically.
#and creates a variable containing the schema and datata types. Definition of Schema explicitly programmatically is usually needed for csv or json, which do not
#store schemas internally.
#%%
#Rename 'Zip Code' col to 'ZipCode, 'Average Household Size' to 'AverageHHSize' 
#and 'Estimated Median Income' to 'EstMedianIncome'.
df_2010_census_df = df_2010_census.withColumnRenamed("Zip Code", "ZipCode")
df_2010_census_df = df_2010_census_df.withColumnRenamed("Average Household Size", "AverageHHSize")
df_2025_income_df = df_2025_income.withColumnRenamed("Zip Code", "ZipCode")
df_2025_income_df = df_2025_income_df.withColumnRenamed("Estimated Median Income", "EstMedianIncome")

#Select only ZipCode and Av.hh size columns
df_2010_census_df = df_2010_census_df.select(col("ZipCode"), col("AverageHHSize"))
df_2025_income_df = df_2025_income_df.select(col("ZipCode"), col("EstMedianIncome"))
#Turn string format '$36743' of the income into float: 36743.0
df_2025_income_df = df_2025_income_df.withColumn(
    "EstMedianIncome",
    regexp_replace(col("EstMedianIncome"), "[$,]", "").cast("float")
)

#%%Join the 2 dataframes with Spark's DataFrames API
joinedDf = df_2010_census_df.join(df_2025_income_df, df_2010_census_df.ZipCode == df_2025_income_df.ZipCode, "inner")
# %%
joinedDf = joinedDf.withColumn("Av_income_per_person", col("EstMedianIncome") / col("AverageHHSize"))
#Print 10 results snipet
print('==========With Spark DataFrames=========================')
print('Here is a 10 entries snipet for Query 3:')
joinedDf.show(10)

# %%
#Count the number of entries/rows
#row_count = joinedDf.count()
#Count distinct number of entries
#distinct_row_count = joinedDf.distinct().count()
# %%
#Find specific entry in rdd
#element = joinedDf.filter(col("ZipCode") == 90002)

