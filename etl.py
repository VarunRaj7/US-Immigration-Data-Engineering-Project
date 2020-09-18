import configparser
import datetime as dt
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import from_unixtime, to_timestamp
from pyspark.sql.types import MapType, StringType, IntegerType
from pyspark.sql.functions import monotonically_increasing_id


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
        .getOrCreate()
    return spark


'''
process_song_data():
ETL process for the song data and create the songs and artists dimension data.
Inputs: sparkSesssion, input_data: s3 location, output_data: s3(Ideal) or local or hdfs
'''

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/'
    
    # read song data file
    df = spark.read.json(song_data).dropna(subset=['song_id', 'artist_id'])

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', \
                             'artist_latitude', 'artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists')


'''
process_log_data():
ETL process for the log data and create the users and time dimension data, and songplays fact data.
Inputs: sparkSesssion, input_data: s3 location, output_data: s3(Ideal) or local or hdfs
'''
    
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/'  # use: 'log_data' on local sample data

    # read log data file
    df = spark.read.json(log_data).dropna(subset='userId') 
    
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