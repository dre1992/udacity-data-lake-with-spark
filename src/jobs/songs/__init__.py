from shared.context import JobContext

__author__ = 'dre'


class SongsJobContext(JobContext):
    pass


def analyze(sc, input_path, output_path):
    """
     Job that reads the song data and creates the songs and artists tables

     :param sc: The spark session
     :param input_path: The s3 path the data resides
     :param output_path:  The output s3 path for the tables created
     """

    context = SongsJobContext(sc)

    song_data = "{}/song_data/*/*/*/*.json".format(input_path)

    # get filepath to song data file

    # read song data file
    df = sc.read.json(song_data)
    df.cache()
    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.dropDuplicates(['song_id']).write.partitionBy('year', 'artist_id')\
        .parquet("{}/songs.parquet".format(output_path), mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr(['artist_id', 'artist_name as name',
                                   'artist_location as location',
                                   'artist_latitude as latitude',
                                   'artist_longitude as longitude'])

    # write artists table to parquet files
    artists_table.dropDuplicates(['artist_id']).write.parquet("{}/artist.parquet".format(output_path), mode="overwrite")
