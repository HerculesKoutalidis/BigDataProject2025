#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import split

import os
#import pandas as pd


#%%
#os.environ['HADOOP_USER_NAME'] = 'your_username'
#os.environ['PYSPARK_PYTHON'] = r'C:\Users\hercu\Documents\BigData\VScode\.venv\Scripts\python.exe'
#os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\hercu\Documents\BigData\VScode\.venv\Scripts\python.exe'
#os.environ['SPARK_SUBMIT_OPTS']= r'-Djava.security.manager=allow'
#os.environ['PYTHONUNBUFFERED'] ='1'

#%%===============================================
#Data directories
read_data_from_hdfs = True

if not read_data_from_hdfs:
    #Data input directories local
    data_dir = r'..\..\data'
    LA_crime_data_2010_2019_dir = r'..\..\data\LA_Crime_Data_2010_2019.csv'
    LA_crime_data_2020_2025_dir = r'..\..\data\LA_Crime_Data_2020_2025.csv'
    LA_Police_Stations_dir = r'..\..\data\LA_Police_Stations.csv'
    Median_hh_income_dir = r'..\..\data\LA_income_2015.csv'
    Census_2010_Zip_code_dir = r'..\..\data\2010_Census_Populations_by_Zip_Code.csv'
    MO_codes_dir = r'..\..\data\MO_codes.txt'

    #Data output (parquet) directories local
    LA_crime_data_2010_2019_output_dir = r'..\..\data\LA_Crime_Data_2010_2019_output'
    LA_crime_data_2020_2025_output_dir = r'..\..\data\LA_Crime_Data_2020_2025_output'
    LA_Police_Stations_output_dir = r'..\..\data\LA_Police_Stations_output'
    Median_hh_income_output_dir = r'..\..\data\LA_income_2015_output'
    Census_2010_Zip_code_output_dir = r'..\..\data\2010_Census_Populations_by_Zip_Code_output'
    MO_codes_output_dir = r'..\..\data\MO_codes_output'

else:
    hdfs_read_dir_root = "hdfs://hdfs-namenode:9000/user/root/data/"
    #Data input directories hdfs
    LA_crime_data_2010_2019_dir = hdfs_read_dir_root+r'LA_Crime_Data_2010_2019.csv'
    LA_crime_data_2020_2025_dir = hdfs_read_dir_root+r'LA_Crime_Data_2020_2025.csv'
    LA_Police_Stations_dir = hdfs_read_dir_root+r'LA_Police_Stations.csv'
    Median_hh_income_dir = hdfs_read_dir_root+r'LA_income_2015.csv'
    Census_2010_Zip_code_dir = hdfs_read_dir_root+r'2010_Census_Populations_by_Zip_Code.csv'
    MO_codes_dir = hdfs_read_dir_root+r'MO_codes.txt'

    #Data output (parquet) directories hdfs
    save_dir_root= "hdfs://hdfs-namenode:9000/user/hkoutalidis/data/parquet/"
    LA_crime_data_2010_2019_output_dir = save_dir_root+ r'LA_Crime_Data_2010_2019_output'
    LA_crime_data_2020_2025_output_dir = save_dir_root+r'LA_Crime_Data_2020_2025_output'
    LA_Police_Stations_output_dir = save_dir_root+r'LA_Police_Stations_output'
    Median_hh_income_output_dir = save_dir_root+r'LA_income_2015_output'
    Census_2010_Zip_code_output_dir = save_dir_root+ r'2010_Census_Populations_by_Zip_Code_output'
    MO_codes_output_dir = save_dir_root+ r'MO_codes_output'

#df = pd.read_csv(LA_crime_data_2010_2019_dir)

n_show_rows = 10

#%%
# Create a Spark session
spark = SparkSession.builder \
    .appName("CSVs to Parquet and storage to HDFS") \
    .getOrCreate()


#%% Dataset 1: Read 2010 Census Populations by Zip Code CSV file (adjust options as needed)
LA_crime_data_2010_2019_df = spark.read.csv(
    LA_crime_data_2010_2019_dir,
    header=True,          # If file has header
    inferSchema=True,     # Let Spark guess schema
    sep=",",             # Specify delimiter
    #quote='"',            # Specify quote character
    #escape='"',           # Specify escape character
    #multiLine=True        # If fields contain newlines
)
# Write to Parquet (partitioned if needed)
LA_crime_data_2010_2019_df.write.mode('overwrite').parquet(
    LA_crime_data_2010_2019_output_dir,
    mode="overwrite",     # or "append"
    compression="snappy"  # or "gzip", "lz4", etc.
)
# Now read the (multiple partitioned) parquet files
LA_crime_data_2010_2019_df = spark.read.parquet(LA_crime_data_2010_2019_output_dir)
#LA_crime_data_2010_2019_df.printSchema() # Display the schema
#LA_crime_data_2010_2019_df.show(5) # Show a sample of the data
LA_crime_data_2010_2019_df.show(n_show_rows)

#%% =============================================================
# Dataset 2: LA_crime_data_2020_2025
LA_crime_data_2020_2025_df = spark.read.csv(
    LA_crime_data_2020_2025_dir,header=True, inferSchema=True, sep=",",
)
# Write to Parquet (partitioned if needed)
LA_crime_data_2020_2025_df.write.mode('overwrite').parquet(
    LA_crime_data_2020_2025_output_dir,
    mode="overwrite",     # or "append"
    compression="snappy"  # or "gzip", "lz4", etc.
)
# Now read the (multiple partitioned) parquet files
LA_crime_data_2020_2025_df = spark.read.parquet(LA_crime_data_2020_2025_output_dir)
LA_crime_data_2020_2025_df.show(n_show_rows)

#%% =============================================================
# Dataset 3: LA_Police_Stations
LA_Police_Stations_df = spark.read.csv(
    LA_Police_Stations_dir,header=True, inferSchema=True, sep=",",
)
# Write to Parquet (partitioned if needed)
LA_Police_Stations_df.write.mode('overwrite').parquet(
    LA_Police_Stations_output_dir,
    mode="overwrite",     # or "append"
    compression="snappy"  # or "gzip", "lz4", etc.
)
# Now read the (multiple partitioned) parquet files
LA_Police_Stations_df = spark.read.parquet(LA_Police_Stations_output_dir)
LA_Police_Stations_df.show(n_show_rows)


#%% =============================================================
# Dataset 4: Median_hh_income
Median_hh_income_df = spark.read.csv(
    Median_hh_income_dir,header=True, inferSchema=True, sep=",",
)
# Write to Parquet (partitioned if needed)
Median_hh_income_df.write.mode('overwrite').parquet(
    Median_hh_income_output_dir,
    mode="overwrite",     # or "append"
    compression="snappy"  # or "gzip", "lz4", etc.
)
# Now read the (multiple partitioned) parquet files
Median_hh_income_df = spark.read.parquet(Median_hh_income_output_dir)
Median_hh_income_df.show(n_show_rows)


#%% =============================================================
# Dataset 5: Census_2010_Zip_code
Census_2010_Zip_code_df = spark.read.csv(
    Census_2010_Zip_code_dir,header=True, inferSchema=True, sep=",",
)
# Write to Parquet (partitioned if needed)
Census_2010_Zip_code_df.write.mode('overwrite').parquet(
    Census_2010_Zip_code_output_dir,
    mode="overwrite",     # or "append"
    compression="snappy"  # or "gzip", "lz4", etc.
)
# Now read the (multiple partitioned) parquet files
Census_2010_Zip_code_df = spark.read.parquet(Census_2010_Zip_code_output_dir)
Census_2010_Zip_code_df.show(n_show_rows)



#%% =============================================================
# Dataset 6:  MO_codes
MO_codes_dir_df = spark.read.text(MO_codes_dir) # Read the input text file

# Split each line into number and description columns
MO_codes_dir_df = MO_codes_dir_df.select(
    split(MO_codes_dir_df["value"], " ", 2).getItem(0).alias("number"),
    split(MO_codes_dir_df["value"], " ", 2).getItem(1).alias("description")
)
# Write the result to a Parquet file
MO_codes_dir_df.write.mode('overwrite').parquet(MO_codes_output_dir)
MO_codes_dir_df.show(n_show_rows)

"""
# %%#####################################
# Read and store file from HDFS
# Dataset 3: LA_Police_Stations
LA_Police_Stations_df = spark.read.csv(
    LA_Police_Stations_dir,header=True, inferSchema=True, sep=",",
)
# Write to Parquet (partitioned if needed)
LA_Police_Stations_df.write.mode('overwrite').parquet(
    LA_Police_Stations_output_dir,
    mode="overwrite",     # or "append"
    compression="snappy"  # or "gzip", "lz4", etc.
)
# Now read the (multiple partitioned) parquet files
LA_Police_Stations_df = spark.read.parquet(LA_Police_Stations_output_dir)
LA_Police_Stations_df.show(10)

"""
