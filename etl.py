import configparser
import datetime as dt
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
import re

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','SECRET_ACCESS_KEY')


'''
create_spark_session():
Used to create a new sparksession
'''

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0","saurfang:spark-sas7bdat:3.0.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark


'''
process_dimension_data():
ETL process for the aiports data and cities demographics dimension data.
Inputs: sparkSesssion, input_data: s3 location, output_data: s3(Ideal) or local or hdfs
'''

def process_dimension_data(spark, input_bucket, output_data):
    # get filepath to airports data file
    airports_data = input_bucket+'AirportsData.parquet'
    
    # read aiports data file
    df_airports = spark.read.parquet(airports_data)
    
    # get filepath to I94_ports
    I94_ports_data = input_bucket+'I94_ports.csv'
    
    # read I94_ports data file
    df_apc =  spark.read.options(delimiter=",", header=True) \
                    .csv(I94_ports_data)
    
    # Merge them using the conditions so that I94ports 
    # location airports are only filtered
    cond = [df_apc.locality==df_airports.municipalityE, \
            df_apc.province==df_airports.state, \
            df_apc.territory==df_airports.country]
    df_airports_apc = df_airports.join(F.broadcast(df_apc), cond, "inner")
    
    # airports table
    airports_table = df_airports_apc.select('ident', 'type', 'elevation_ft', \
                                      'continent', 'gps_code', 'iata_code', \
                                      'local_code', 'coordinates',\
                                      'nameL', 'municipalityL', 'nameE', \
                                      'locality', 'province', 'country', 'code')
    
    # write airports table to parquet files partitioned by province
    airports_table.write.parquet(output_data+'airports', partitionBy=['province'])
    
    # get filepath to cities demographics data file
    cities_demo_data = input_bucket+'us-cities-demographics.csv'
    
    # read cities demographics data file
    df_cd = spark.read.options(delimiter=";", header="true", inferSchema='true')\
            .csv(cities_demo_data)
    
    # Create a new column with race percent by city
    df_cd = df_cd.withColumn("Race_percent_by_city", F.col("Count")*100/F.col("Total Population"))
    
    # Rank the cities for each race
    df_cd =  df_cd.withColumn("Race_rank_by_city", \
                    F.dense_rank().over(Window.partitionBy("Race").orderBy(F.desc("Race_percent_by_city"))))
    
    # Merge them using the conditions so that I94ports 
    # location cities are only filtered
    cond = [df_cd.City==df_apc.locality, df_cd.State==df_apc.province]
    df_cd_apc = df_cd.join(F.broadcast(df_apc), cond, "inner")
    
    # cities_demo_table
    cities_demo_table = df_cd_apc.select("City", "State", "territory", F.col("Median Age").alias("Median_Age") , \
            F.col("Male Population").alias("Male_Population"), F.col("Female Population").alias("Female_Population"), \
            F.col("Total Population").alias("Total_Population"), F.col("Number of Veterans").alias("Number_of_Veteranas"), \
            "Foreign-born", F.col("Average Household Size").alias("Avg_Household_Size"), "Race", "Count", "Race_percent_by_city",\
            "Race_rank_by_city", "code")
    
    # write cities_demo_table to parquet files partitioned by state
    cities_demo_table.write.parquet(output_data+'cities_demo', partitionBy=['state'])

'''
process_facts_data():
ETL process for the I94 immigration fact data.
Inputs: sparkSesssion, input_data: s3 location, output_data: s3(Ideal) or local or hdfs
'''
    
def process_facts_data(spark, input_data, input_bucket, output_data):
    # get filepath to I94 immigration data file
    I94_data = input_data

    # read I94 immigration data file
    df_I94 =spark.read.format('com.github.saurfang.sas.spark').load(input_data)
    
    # get datetime from arrdate and depdate column value
    get_date1 = F.udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None, T.StringType())

    df_I94 = df_I94.withColumn('iso_arrdate', get_date1(F.col('arrdate')))
    df_I94 = df_I94.withColumn('iso_depdate', get_date1(F.col('depdate')))
    
    # get datetime from duedate column value
    get_date2 = F.udf(lambda x: x[4:]+'-'+x[:2]+'-'+x[2:4] if x, T.StringType())
    
    # get datetime from duedate column value
    df_I94 = df_I94.withColumn('iso_duedate', get_date2(F.col('dtaddto')))
    
    # add mode mappings directly to df
    i94mode = [(1, 'Air'),(2,'Sea'),(3,'Land'),(9,'Not Reported')]
    i94mode_rdd = spark.sparkContext.parallelize(i94mode).map(lambda x: Row(i94mode=x[0], i94_mode=x[1]))
    i94mode_df = spark.createDataFrame(i94mode_rdd)
    
    df_I94 = df_I94.join(F.broadcast(i94mode_df), df_I94.i94mode==i94mode_df.i94mode, 'left')
    
     # add visa mappings directly to df
    i94visa = [(1, 'Business'),(2,'Pleasure'),(3,'Student')]
    i94visa_rdd = spark.sparkContext.parallelize(i94visa).map(lambda x: Row(i94visa=x[0], i94_visa=x[1]))
    i94visa_df = spark.createDataFrame(i94visa_rdd)
    
    df_I94 = df_I94.join(F.broadcast(i94visa_df), df_I94.i94visa==i94visa_df.i94visa, 'left')
    
     # add US state mappings directly to df
    df_i94addr = spark.read.options(delimiter=",", header=True)\
                    .csv(input_bucket+"I94_addr.csv")
    
    df_I94 = df_I94.join(F.broadcast(df_i94addr), df_I94.i94addr==df_i94addr.i94addr, "left")
    
    # I94_table
    I94_table = df_I94.select('cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'iso_arrdate', 'iso_depdate', 'iso_duedate', \
                 'i94_visa', 'i94_mode', 'admnum', 'insnum', 'i94addr_US_state', 'airline', 'fltno', 'visatype', 'i94bir', 'gender')
    
    I94_table = I94_table.dropna(subset="i94port")

    # write I94_table to parquet files partitioned by yr, mon, port
    I94_table.write.mode('overwrite').parquet(output_data+'I94_data', partitionBy=['i94yr','i94mon','i94port'])

    
    
'''
main():
The main function.
''' 

def main():
    spark = create_spark_session()
    
    input_bucket = "s3a://us-immigration-cleaned-data/" 
    output_data = "s3://us-immigration-dl/" 
    input_data = "s3a://us-immigration-cleaned-data/i94_apr16_sub.sas7bdat" 
    
    process_dimension_data(spark, input_bucket, output_data)    
    process_facts_data(spark, input_data, input_bucket, output_data)


if __name__ == "__main__":
    main()