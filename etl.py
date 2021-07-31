import configparser
from datetime import datetime
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
    TimestampType,
    LongType,
)


logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """
    Description: Create a spark session

    Returns:
        Spark session

    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark: SparkSession, output_data: str) -> None:
    """
    Description: This method loads song data in a Spark dataframe
        and extracts & writes in parquet the song & artist tables.

    Arguments:
        spark (SparkSession): a Spark session
        output_data (str): path of s3 bucket name
    """
    # get filepath to song data files
    song_data = f"{SONG_DATA}/*/*/*/*.json"

    # song schema
    songSchema = StructType(
        [
            StructField("num_songs", IntegerType()),
            StructField("artist_id", StringType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_name", StringType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("duration", DoubleType()),
            StructField("year", IntegerType()),
        ]
    )

    # read song data file
    logger.info("Loading song data in Spark...")
    try:
        song_df = spark.read.json(song_data, schema=songSchema)
    except Exception as e:
        msg = "ERROR: Could not load song data in Spark df."
        logger.warning(msg, e)
        return

    # extract columns to create songs table (lazy eval)
    logger.info("Extracting columns to create song table...")
    songs_header = ["title", "artist_id", "year", "duration"]
    try:
        songs_table = (
            song_df.select(songs_header)
            .where(song_df.song_id.isNotNull())
            .dropDuplicates()
        )
    except Exception as e:
        msg = "ERROR: Could not extract columns to create song table."
        logger.warning(msg, e)
        return

    # generate song_id
    songs_table = songs_table.withColumn("song_id", monotonically_increasing_id())

    # write songs table to parquet files partitioned by year and artist
    songs_partitionBy = ["year", "artist_id"]
    logger.info("Writing song table in parquet...")
    try:
        songs_table.write.mode("overwrite").partitionBy(songs_partitionBy).parquet(
            f"{output_data}/songs"
        )
    except Exception as e:
        msg = "ERROR: Could not write song table in parquet."
        logger.warning(msg, e)
        return

    # extract columns to create artists table
    artists_header = [
        "artist_id",
        "artist_name as name",
        "artist_location as location",
        "artist_latitude as latitude",
        "artist_longitude as longitude",
    ]

    logger.info("Extracting columns for artist table...")
    try:
        artists_table = (
            song_df.selectExpr(artists_header)
            .where(song_df.artist_id.isNotNull() & song_df.artist_name.isNotNull())
            .dropDuplicates()
        )
    except Exception as e:
        msg = "ERROR: Could not extract columns for artist table."
        logger.warning(msg, e)
        return

    # write artists table to parquet files
    logger.info("Writing artist table in parquet...")
    try:
        artists_table.write.mode("overwrite").parquet(f"{output_data}/artists")
    except Exception as e:
        msg = "ERROR: Could not write artist table in parquet."
        logger.warning(msg, e)
        return


def process_log_data(spark: SparkSession, output_data: str) -> None:
    """
    Description: This method loads log and song data in a Spark dataframe
        and extracts & writes in parquet the users, time, and songplays tables.

    Arguments:
        spark (SparkSession): a Spark session
        output_data (str): path of s3 bucket name
    """
    # get filepath to log data file
    log_data = f"{LOG_DATA}/*/*/*.json"

    logSchema = {
        "artist": StringType(),
        "auth": StringType(),
        "firstName": StringType(),
        "gender": StringType(),
        "itemInSession": LongType(),
        "lastName": StringType(),
        "length": DoubleType(),
        "level": StringType(),
        "location": StringType(),
        "method": StringType(),
        "page": StringType(),
        "registration": TimestampType(),
        "sessionId": LongType(),
        "song": StringType(),
        "status": IntegerType(),
        "ts": TimestampType(),
        "userAgent": StringType(),
        "userId": LongType(),
    }

    # read log data file
    logger.info("Loading log data in Spark...")
    try:
        log_df = spark.read.json(log_data)
    except Exception as e:
        msg = "ERROR: Could not load log data in Spark df."
        logger.warning(msg, e)
        return

    # remove entries where userId is empty string
    log_df = log_df.where(log_df.userId != "")

    # convert columns to timestamp
    to_timestamp = ["registration", "ts"]
    timestamp_udf = udf(
        lambda x: datetime.utcfromtimestamp(int(x) / 1000.0), TimestampType()
    )
    for column in to_timestamp:
        log_df = log_df.withColumn(column, timestamp_udf(column))

    logger.info("Casting `logSchema` to `log_df`...")
    try:
        log_df = log_df.select(
            [col(column).cast(logSchema[column]) for column in log_df.columns]
        )
    except Exception as e:
        msg = "ERROR: Could not cast `logSchema` to `log_df`"
        logger.warning(msg, e)
        return

    # filter by actions for song plays
    logger.info("Filtering `log_df` where `page` is 'NextSong'...")
    try:
        log_df = log_df.where(log_df.page == "NextSong")
    except Exception as e:
        msg = "ERROR: Could not filter `log_df` where `page` is 'NextSong'."
        logger.warning(msg, e)
        return

    # extract columns for users table
    logger.info("Extracting columns for users table...")
    users_header = [
        "userId as user_id",
        "firstName as first_name",
        "lastName as last_name",
        "gender",
        "level",
    ]
    try:
        users_table = (
            log_df.selectExpr(users_header)
            .where(log_df.userId.isNotNull())
            .dropDuplicates()
        )
    except Exception as e:
        msg = "ERROR: Could not extract columns for users table."
        logger.warning(msg, e)
        return

    # write users table to parquet files
    logger.info("Writing users table data to parquet files...")
    try:
        users_table.write.mode("overwrite").parquet(f"{output_data}/users")
    except Exception as e:
        msg = "ERROR: Could not write users table data in parquet files."
        logger.warning(msg, e)
        return

    # extract columns to create time table
    logger.info("Extracting columns to create time table...")
    try:
        time_table = (
            log_df.selectExpr("ts as start_time")
            .where(log_df.ts.isNotNull())
            .dropDuplicates()
        )
    except Exception as e:
        msg = "ERROR: Could not extract column to create time table."
        logger.warning(msg, e)
        return

    time_header = ["hour", "day", "week", "month", "year", "weekday"]
    time_func = [hour, dayofmonth, weekofyear, month, year, dayofweek]
    logger.info("Creating new time table columns...")
    try:
        for column, column_func in zip(time_header, time_func):
            time_table = time_table.withColumn(column, column_func("start_time"))
    except Exception as e:
        msg = "ERROR: Could not create new time table columns."
        logger.warning(msg, e)
        return

    # write time table to parquet files partitioned by year and month
    logger.info("Writing time table to parquet file partitioned by year and month...")
    time_partitionBy = ["year", "month"]
    try:
        time_table.write.mode("overwrite").partitionBy(time_partitionBy).parquet(
            f"{output_data}/time"
        )
    except Exception as e:
        msg = """ERROR: Could not write time table
            to parquet files partitioned by year and month."""
        logger.warning(msg, e)
        return

    # read in song data to use for songplays table
    logger.info("Reading song data to use for songplays table...")
    try:
        song_df = (
            spark.read.format("parquet")
            .option("basePath", f"{output_data}/songs/")
            .load(f"{output_data}/songs/*/*/")
        )
    except Exception as e:
        msg = "ERROR: Could not read song data."
        logger.warning(msg, e)
        return

    # extract columns from joined song and log datasets to create songplays table
    logger.info(
        """Extracting columns from joined songs and
        log datasets to create songplays table..."""
    )
    logDf_songDf_header = [
        monotonically_increasing_id().alias("songplay_id"),
        col("ts").alias("start_time"),
        col("userId").alias("user_id"),
        "level",
        "song_id",
        "artist_id",
        col("sessionId").alias("session_id"),
        "location",
        col("userAgent").alias("user_agent"),
    ]

    try:
        songplays_table = log_df.join(
            song_df, log_df.song == song_df.title, how="inner"
        ).select(logDf_songDf_header)
    except Exception as e:
        msg = "ERROR: Could not join log_df with song_df."
        logger.warning(msg, e)
        return

    songplaysTbl_timeTbl_header = [
        "songplay_id",
        songplays_table.start_time,
        "user_id",
        "level",
        "song_id",
        "artist_id",
        "session_id",
        "location",
        "user_agent",
        "year",
        "month",
    ]
    try:
        songplays_table = (
            songplays_table.join(
                time_table,
                songplays_table.start_time == time_table.start_time,
                how="inner",
            )
            .select(songplaysTbl_timeTbl_header)
            .dropDuplicates()
        )
    except Exception as e:
        msg = "ERROR: Could not join songplays_table with time_table."
        logger.warning(msg, e)
        return

    # write songplays table to parquet files partitioned by year and month
    logger.info(
        "Writing songplays table to parquet files partitioned by year and month..."
    )
    songplays_partitionBy = ["year", "month"]
    try:
        songplays_table.write.mode("overwrite").partitionBy(
            songplays_partitionBy
        ).parquet(f"{output_data}/songplays")
    except Exception as e:
        msg = "ERROR: Could not write songplays table to parquet file."
        logger.warning(msg, e)
        return


def main() -> None:
    """
    Description: This method executes both `process_song_data` and
        `process_log_data` functions.
    """
    spark = create_spark_session()
    output_data = f"s3://{S3_BUCKET_NAME}"

    process_song_data(spark, output_data)
    process_log_data(spark, output_data)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("dl.cfg")

    os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
    os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")

    LOG_DATA = config.get("S3", "LOG_DATA")
    SONG_DATA = config.get("S3", "SONG_DATA")
    S3_BUCKET_NAME = config.get("S3", "S3_BUCKET_NAME")

    # execute main function
    main()
