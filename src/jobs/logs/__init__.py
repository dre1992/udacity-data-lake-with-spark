from datetime import datetime

import pyspark.sql.functions as F
from shared.context import JobContext

__author__ = 'dre'


class LogsJobContext(JobContext):
    pass


def analyze(sc, input_path, output_path):
    """
    Job that reads the log data and creates the user time and songplays tables

    :param sc: The spark session
    :param input_path: The s3 path the data resides
    :param output_path:  The output s3 path for the tables created
    """
    context = LogsJobContext(sc)
    # get filepath to log data file
    log_data = "{}/log_data/*/*/*.json".format(input_path)
    # read log data file
    df = sc.read.json(log_data)
    df.cache()
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.cache()

    # extract columns for users table
    users = df.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender',
                          'level').dropDuplicates()
    # write users table to parquet files
    users.write.parquet("{}/users.parquet".format(output_path), mode='overwrite')

    start_times_datetime = df.dropDuplicates().selectExpr("cast((from_unixtime(ts/1000.0)) as timestamp) as start_time")
    start_times_datetime.printSchema()
    time = start_times_datetime.selectExpr(["start_time", "hour(start_time) as hour", "day(start_time) as day",
                                            "weekofyear(start_time) as week", "month(start_time) as month",
                                            "year(start_time) as year",
                                            "dayofweek(start_time) as weekday"
                                            ])

    # write time table to parquet files partitioned by year and month
    time.write.partitionBy(['year', 'month']).parquet("{}/times.parquet".format(output_path), mode="overwrite")

    # read in song data to use for songplays table
    song_df = sc.read.parquet("{}/songs.parquet".format(output_path))

    # extract columns from joined song and log datasets to create songplays table
    song_df.createOrReplaceTempView('songs')
    df.createOrReplaceTempView('logs')
    songplays_table = sc.sql('select \
          monotonically_increasing_id() as songplay_id,  \
          from_unixtime(l.ts/1000) as start_time, \
          userId as user_id,\
          l.level,\
          s.song_id,\
          s.artist_id,\
          l.sessionId as session_id,\
          l.location,\
          l.userAgent as user_agent\
          from \
          logs l \
          left join songs s on l.song = s.title')

    songplays_table_for_partitioning = songplays_table\
        .withColumn('year', F.year(songplays_table.start_time))\
        .withColumn('month', F.month(songplays_table.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table_for_partitioning.write.partitionBy(['year', 'month']) \
        .parquet("{}/songplays.parquet".format(output_path), mode='overwrite')
