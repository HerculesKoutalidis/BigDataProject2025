#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col, lower, when, lit, isnull, udf,regexp_replace, regexp_extract, avg, count
from pyspark.sql.functions import radians, sin, cos, sqrt, atan2
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, IntegerType, ArrayType
from pyspark.sql.window import Window

import os, time


#%%
read_from_hdfs = True
if not read_from_hdfs:
    os.environ['HADOOP_USER_NAME'] = 'your_username'
    os.environ['PYSPARK_PYTHON'] = r'C:\Users\hercu\Documents\BigData\VScode\.venv\Scripts\python.exe'
    os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\hercu\Documents\BigData\VScode\.venv\Scripts\python.exe'
    os.environ['SPARK_SUBMIT_OPTS']= r'-Djava.security.manager=allow'
    os.environ['PYTHONUNBUFFERED'] ='1'

#%%========================================================================
#============ Using DataFrames ===================================================
start_time = time.time()
spark = SparkSession \
    .builder \
    .appName("Q4 execution with DataFramess") \
    .getOrCreate() 
    #.sparkContext

sc = spark.sparkContext
# ΕΛΑΧΙΣΤΟΠΟΙΗΣΗ ΕΞΟΔΩΝ ΚΑΤΑΓΡΑΦΗΣ (LOGGING)
sc.setLogLevel("ERROR")

#%% Read both Parquet files 
if read_from_hdfs:
    save_dir_root= "hdfs://hdfs-namenode:9000/user/hkoutalidis/data/parquet/"
    LA_crime_data_2010_2019_output_dir = save_dir_root+ r'LA_Crime_Data_2010_2019_output'
    LA_crime_data_2020_2025_output_dir = save_dir_root+r'LA_Crime_Data_2020_2025_output'
    LA_police_stations_output_dir = save_dir_root+r'LA_Police_Stations_output'
    mocodes_output_dir = save_dir_root+r'MO_codes_output'
    df_2010_2019 = spark.read.parquet(LA_crime_data_2010_2019_output_dir)
    df_2020_2025 = spark.read.parquet(LA_crime_data_2020_2025_output_dir)
    df_police_stations = spark.read.parquet(LA_police_stations_output_dir)
    df_mocodes = spark.read.parquet(mocodes_output_dir)
    
else:
    df_2010_2019 = spark.read.parquet("C:/Users/hercu/Documents/BigData/data/LA_Crime_Data_2010_2019_output")
    df_2020_2025 = spark.read.parquet("C:/Users/hercu/Documents/BigData/data/LA_Crime_Data_2020_2025_output")
    df_police_stations = spark.read.parquet("C:/Users/hercu/Documents/BigData/data/LA_Police_Stations_output")
    df_mocodes = spark.read.parquet("C:/Users/hercu/Documents/BigData/data/MO_codes_output")


# %%
#Combine the 2 crime datasets into 1.
crimes_df = df_2010_2019.union(df_2020_2025)
crimes_df = crimes_df.withColumnRenamed("AREA ", "AREA") #col name AREA has a backspace on the rght. Correct it.

#%%Select the columns needed. Many are not.
crimes_df = crimes_df.select(col("AREA"), col("AREA NAME"),col("Mocodes"),col("LAT"),col("LON"))

#%%# ----------------------------------------------------------------------
# 1. Filter to only those Mocodes whose description mentions "weapon" or "gun"
# ----------------------------------------------------------------------
# We lowercase the description to make the search case‐insensitive.
# Also, ensure we treat the "number" column as a string when comparing against codes in crimes_df.
#df_mocodes = df_mocodes.withColumnRenamed("number", "mocode")
#%%
weapon_codes_df = (
    df_mocodes
    .withColumn("code_str", col("number").cast(StringType()))
    .filter(
        (lower(col("description")).contains("weapon")) | #check if 'weapon' or 'gun' is contained in the mocode
        (lower(col("description")).contains("gun"))
    )
    .select("code_str")
)
#%%# Collect the weapon‐related codes into a Python set and broadcast it to all workers, so that every worker can compare it to 
#each entry's mocode value(s), and see if this is a crime involving a gun or weapon.
weapon_codes_list = [row.code_str for row in weapon_codes_df.collect()]
weapon_codes_bcast = spark.sparkContext.broadcast(set(weapon_codes_list))



#%% ----------------------------------------------------------------------
# 2. Define a UDF that checks if any of the Mocodes for a crime is in the weapon set
# ----------------------------------------------------------------------
def flag_weapon_involved(mocodes_str):
    """
    Returns 1 if any of the space‐separated codes in mocodes_str
    appears in the broadcasted weapon_codes set, else 0.
    """
    if mocodes_str is None:
        return 0
    # Split on one or more whitespace characters
    codes = mocodes_str.strip().split()
    weapon_set = weapon_codes_bcast.value
    for c in codes:
        # Compare each code (string) against the set
        if c in weapon_set:
            return 1
    return 0

has_weapon_udf = udf(flag_weapon_involved, IntegerType())




#%% ----------------------------------------------------------------------
# 3. Add "Weapon_Involved" column to the crimes DataFrame
# ----------------------------------------------------------------------
# Assume the crimes DataFrame has a column "Mocodes" (string of codes like "1822 1402 0344")
crimes_with_weapon_df = crimes_df.withColumn(
    "Weapon_Involved",
    has_weapon_udf(col("Mocodes"))
)
#Filter out the crimes with no weapon or gun, namely where Weapon_Involved = 0
crimes_with_weapon_df = crimes_with_weapon_df.filter(
    col('Weapon_Involved') ==1
)

#%%Filter out NULL Mocode entries:
crimes_with_weapon_df = crimes_with_weapon_df.filter(col("Mocodes").isNotNull())
#Rename the LAT and LON col names into LAT_CRM and LON_CRM.
crimes_with_weapon_df = crimes_with_weapon_df.withColumnRenamed("LAT", "LAT_CRM")
crimes_with_weapon_df = crimes_with_weapon_df.withColumnRenamed("LON", "LON_CRM")        


#%%Rename police stations DF X,Y into LON_PD, LAT_PD
df_police_stations = df_police_stations.withColumnRenamed("X", "LON_PD")
df_police_stations = df_police_stations.withColumnRenamed("Y", "LAT_PD")  


# %%
# Now join on the common name
# Perform inner join on crimes_with_weapon_df.AREA == df_police_stations.PREC
joined_df = crimes_with_weapon_df.join(
    df_police_stations,
    crimes_with_weapon_df["AREA"] == df_police_stations["PREC"],
    how="inner"
)
print('CHCECKPOINT1')
# %%
# Select desired columns and drop the others.
joined_df = joined_df.select(
    crimes_with_weapon_df["*"],
    #df_police_stations["PREC"].alias("PoliceStationID"),
    df_police_stations['LON_PD'],
    df_police_stations['LAT_PD'],
)
print('CHCECKPOINT2')
# %% Filter out Null island entries:
filtered_df = joined_df.filter(
    (col("LON_CRM") < -100) & 
    (col("LAT_CRM") > 10) & 
    (col("LON_PD") < -100) & 
    (col("LAT_PD") > 10)
)
# %%Compute the distances between crimes and their respective PD, based on the Haversine distance
# Haversine formula constants
R = 6371.0  # Earth’s radius in kilometers

# 1. Compute the differences in radians
dlat = radians(col("LAT_PD") - col("LAT_CRM"))
dlon = radians(col("LON_PD") - col("LON_CRM"))

# 2. Compute the Haversine intermediate value 'a'
a = (
    sin(dlat / 2) ** 2
    + cos(radians(col("LAT_CRM"))) * cos(radians(col("LAT_PD"))) * sin(dlon / 2) ** 2
)

# 3. Compute the angular distance 'c'
c = 2 * atan2(sqrt(a), sqrt(1 - a))

# 4. Multiply by Earth's radius to get distance in kilometers
distance_expr = lit(R) * c

# 5. Add the new column "distance" to the DataFrame
joined_with_distance_df = joined_df.withColumn("distance", distance_expr)
# %%Keep only the columns needed, namely AREA, AREA NAME, and distance.
final_df = joined_with_distance_df.select("AREA", "AREA NAME", "distance")



# %%
# Group by "AREA NAME" and compute the required aggregations:
agg_df = final_df.groupBy(col("AREA NAME")) \
    .agg(
        avg(col("distance")).alias("Average_Distance"),
        count("*").alias("Number_of_incidents")
    ) \
    .orderBy(col("Number_of_incidents").desc())
print('CHCECKPOINT5')
# %%Print the results
print('Query 4 results, in format : AREA NAME , Average Distance from PD, Number of incidents/crimes. ')
agg_df.show(30)

end_time = time.time()
total_time = round(end_time-start_time, 3)
print(f'Total execution time: {total_time}')
# %%
