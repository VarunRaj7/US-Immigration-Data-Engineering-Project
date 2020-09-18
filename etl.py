import configparser
import datetime as dt
import os
from pyspark.sql import SparkSession
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
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()
    return spark


'''
process_dimension_data():
ETL process for the aiports data and cities demographics dimension data.
Inputs: sparkSesssion, input_data: s3 location, output_data: s3(Ideal) or local or hdfs
'''

def process_dimension_data(spark, input_data, output_data):
    # get filepath to airports data file
    airports_data = input_data+'AirportsData.parquet'
    
    # read aiports data file
    df_airports = spark.read.parquet(airports_data)
    
    # get filepath to I94_ports
    I94_ports_data = input_data+'I94_ports.csv'
    
    # read I94_ports data file
    df_apc =  spark.read.options(delimiter=",", header=True) \
                    .csv(I94_ports_data)
    
    # Merge them using the conditions so that I94ports 
    # location airports are only filtered
    cond = [df_apc.locality==df_apd.municipalityE, \
            df_apc.province==df_apd.state, \
            df_apc.territory==df_apd.country]
    df_airports_apc = df_apd.join(F.broadcast(df_apc), cond, "inner")
    
    # airports table
    airports_table = df_airports_apc.select('ident', 'type', 'elevation_ft', \
                                      'continent', 'gps_code', 'iata_code', \
                                      'local_code', 'coordinates',\
                                      'nameL', 'municipalityL', 'nameE', \
                                      'locality', 'province', 'country', 'code')
    
    # write airports table to parquet files partitioned by province
    airports_table.write.parquet(output_data+'airports', partitionBy=['province'])
    
    # get filepath to cities demographics data file
    cities_demo_data = input_data+'us-cities-demographics.csv'
    
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
    cities_demo_table = df_cd_apc.select("City", "State", "territory", "Median Age", "Male Population", "Female Population", "Total Population", \
         "Number of Veterans", "Foreign-born", "Average Household Size", "Race", "Count", "Race_percent_by_city",\
          "Race_rank_by_city", "code")
    
    # write cities_demo_table to parquet files partitioned by state
    cities_demo_table.write.parquet(output_data+'airports', partitionBy=['state'])

'''
process_facts_data():
ETL process for the I94 immigration fact data.
Inputs: sparkSesssion, input_data: s3 location, output_data: s3(Ideal) or local or hdfs
'''
    
def process_facts_data(spark, input_data, output_data):
    # get filepath to I94 immigration data file
    I94_data = input_data

    # read I94 immigration data file
    df_I94 =spark.read.format('com.github.saurfang.sas.spark').load('input_data')
    
    # filter by actions for song plays
    df = df.filter("page='NextSong'")

    # extract columns for users table    
    users_table = df.select([col('userId').alias('user_id'),\
                             col('firstName').alias('first_name'),\
                             col('lastName').alias('last_name'), 'gender', 'level'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users')

    # create timestamp column from original timestamp column
    df = df.withColumn('tsX', to_timestamp(from_unixtime(df.ts/1000, \
                                             format='yyyy-MM-dd HH:mm:ss')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda row: {'year': row.year, \
                                    'month': row.month,
                                    'week': row.isocalendar()[1],
                                    'day': row.day,
                                    'hour': row.hour,
                                    'weekday': row.weekday()
                                   },\
                   MapType(StringType(), IntegerType()))
    df =  df.withColumn('dt', get_datetime(df.tsX))
    
    # extract columns to create time table
    time_table = df.drop_duplicates(subset=['tsX']) \
                    .select([col('tsX').alias('start_time'), 'dt.hour', 'dt.day', 'dt.week', \
                              'dt.month', 'dt.year', 'dt.weekday'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+'time', partitionBy=['year', 'month'])
    
    song_data = input_data+'song_data/*/*/*/'
    
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data).dropna(subset=['song_id', 'artist_id'])
    
    cond = [(song_df.title==df.song), \
             (song_df.artist_name==df.artist)]
    
    df_join = df.join(song_df, cond, 'left')
    
    df_join = df_join.withColumn("songplay_id", monotonically_increasing_id() )
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_join.select('songplay_id', col('tsX').alias('start_time'), \
                                     col('userId').alias('user_id'), 'level', 'song_id', \
                                     'artist_id', col('sessionId').alias('session_id'), \
                                     col('artist_location').alias('location'), \
                                     col('userAgent').alias('user_agent'),\
                                    'dt.year', 'dt.month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+'songplays', partitionBy=['year', 'month'])

    
'''
main():
The main function.
''' 

def main():
    spark = create_spark_session()
    
    ## 
    input_data = "s3a://udacity-dend/" # use: 'data/' on local sample data
    output_data = "s3://sparkify-datalake-7/" # user: 'DataLake/' on local sample data
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()