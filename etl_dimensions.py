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
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
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
                                      'local_code', 'coordinates','iso_region', \
                                      'nameL', 'municipalityL', 'code')
    
    # write airports table to parquet file
    airports_table.write.parquet(output_data+'airports')
    
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
    cities_demo_table = df_cd_apc.select(F.col("Median Age").alias("Median_Age") , \
            F.col("Male Population").alias("Male_Population"), F.col("Female Population").alias("Female_Population"), \
            F.col("Total Population").alias("Total_Population"), F.col("Number of Veterans").alias("Number_of_Veteranas"), \
            "Foreign-born", F.col("Average Household Size").alias("Avg_Household_Size"), "Race", "Count", "Race_percent_by_city",\
            "Race_rank_by_city", "code")
    
    # write cities_demo_table to parquet file
    cities_demo_table.write.parquet(output_data+'cities_demo')
    
    
def quality_check_dim(spark, output_data):
    
    try:
        
        df_airports = spark.read.parquet(output_data+'airports')
    
        df_cities_demo = spark.read.parquet(output_data+'cities_demo')
    
    except:
        print("Failed to read the dimension data")
        
    
    if df_airports.count==0 or df_cities_demo.count()==0:
        print("Failed the data quality check")
        
        
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
    quality_check_dim(spark, output_data)


if __name__ == "__main__":
    main()