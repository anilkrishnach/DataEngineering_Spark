import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, count, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['DEFAULT']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['DEFAULT']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_path, output_path):
    """
    Summary: Processes the song data in JSON format and writes the songs and artists tables to parquet files.
    
    Parameters:
    spark: SparkSession
    """
    song_data_path = input_path+"/song_data/*/*/*/*.json"
    
    # Reading song data from S3
    df = spark.read.json(song_data_path)
    df.createOrReplaceTempView('song_data')
    print('Data read from Song_data and created song_data view.')
    
    # Extract columns to create songs table
    df_songs = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).drop_duplicates()
    print('Extracted columns for songs table.')
    
    # Write songs table to parquet files partitioned by year and artist
    songs_output_path = output_path+"songs_table"
    df_songs.write.mode('overwrite').parquet(songs_output_path, partitionBy = ['year', 'artist_id'])
    print(f'Stored the parquet file in {songs_output_path}')
    
    # Extract columns to create artists table
    artists_output_path = output_path+"artists_table"
    df_artists = df.select(['artist_id', 'title', 'artist_location', 'artist_latitude', 'artist_longitude']).drop_duplicates(['artist_id'])
    print('Extracted columns for artists table.')
    
    # Write artists table to parquet files
    df_artists.write.mode('overwrite').parquet(artists_output_path)
    print(f'Stored the parquet file in {artists_output_path}')
    

def process_log_data(spark, input_path, output_path):
    """
    Summary: Processes the log data in JSON format and writes the users and time tables to parquet files. The song data and log data are joined to create the Songplays fact table and writes it to parquet file.
    
    Parameters:
    spark: SparkSession
    """
    # get filepath to log data file
    log_data_path = input_path+"/log_data/"

    # read log data file
    df = spark.read.json(log_data_path)
    print("Data read from log data")
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView('log_data')
    print("Created Log_data view")
    
    # extract columns for users table    
    df_users = df.select(['userId', 'firstName', 'lastName', 'gender', 'level'])
    df_users_with_level = spark.sql("""
    SELECT userId, firstName, lastName, gender, level FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY USERID ORDER BY TS DESC) AS RN FROM log_data) users     WHERE RN=1
    """)
    print("Extracted columns for users table.")
    
    
    # write users table to parquet files
    users_output_path = output_path+"users_table"
    df_users_with_level.drop_duplicates(['userId']).write.mode('overwrite').parquet(users_output_path)
    print(f'Stored the parquet file in {users_output_path}')    

    # create timestamp column from original timestamp column
    @udf
    def gethour(ts):
        return datetime.datetime.fromtimestamp(int(ts/1000)).strftime("%H")
    @udf
    def getday(ts):
        return datetime.datetime.fromtimestamp(int(ts/1000)).strftime("%d")
    @udf
    def getweek(ts):
        return datetime.datetime.fromtimestamp(int(ts/1000)).strftime("%W")

    @udf
    def getmonth(ts):
        return datetime.datetime.fromtimestamp(int(ts/1000)).strftime("%M")

    @udf
    def getyear(ts):
        return datetime.datetime.fromtimestamp(int(ts/1000)).strftime("%Y")

    @udf
    def getweekday(ts):
        return datetime.datetime.fromtimestamp(int(ts/1000)).strftime("%w")

    # extract columns to create time table  
    df_time_table = df.select('ts').withColumn('hour',gethour(df.ts)).withColumn('day',getday(df.ts))
    .withColumn('week',getweek(df.ts)).withColumn('month',getmonth(df.ts))
    .withColumn('year',getyear(df.ts)).withColumn('weekday',getweek(df.ts)).drop_duplicates(['ts'])
    print("Extracted columns for time table.")

    # write time table to parquet files partitioned by year and month
    time_output_path = output_path+"time_table"
    df_time_table.write.mode('overwrite').parquet(time_output_path, partitionBy=['year', 'month'])
    print(f'Stored the parquet file in {time_output_path}')      
    
    # extract columns from joined song and log datasets to create songplays table 
    df_log_data = df
    df_song_data = spark.table('song_data')
    
    # Extracting columns for Songplays table
    df_songplays = df_log_data.join(df_song_data, on=[df_song_data.title == df_log_data.song, df_song_data.artist_name == df_log_data.artist], how='full')
    .join(df_time_table, on=[df_time_table.ts == df_log_data.ts], how='full').
    withColumn('songplay_id', monotonically_increasing_id()).
    select(df_time_table.month, df_time_table.year, df_log_data.ts, df_log_data.userId, df_log_data.level, df_song_data.song_id
           , df_song_data.artist_id, df_log_data.sessionId, df_log_data.location, df_log_data.userAgent).drop_duplicates()
    print("Extracted data for Songplays table.")
    
    songplays_output_path = output_path+"songplays_table"
    df_songplays.write.mode('overwrite').parquet(songplays_output_path, partitionBy=['year', 'month'])
    print(f'Stored the parquet file in {songplays_output_path}')      
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_path = "s3a://demo/"
    
    process_song_data(spark, input_data, output_path)    
    process_log_data(spark, input_data, output_path)


if __name__ == "__main__":
    main()
