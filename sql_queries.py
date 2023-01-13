import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist_name        VARCHAR, 
    auth               VARCHAR, 
    first_name         VARCHAR, 
    gender             VARCHAR,
    iteminsession      INT, 
    last_name          VARCHAR, 
    length             NUMERIC, 
    level              VARCHAR, 
    location           VARCHAR, 
    method             VARCHAR, 
    page               VARCHAR, 
    registration       VARCHAR, 
    session_id         INT, 
    song               VARCHAR, 
    status             INT, 
    start_time         BIGINT NOT NULL, 
    user_agent         VARCHAR, 
    user_id            VARCHAR NOT NULL)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id            VARCHAR PRIMARY KEY, 
    num_songs          INT, 
    artist_id          VARCHAR NOT NULL, 
    artist_latitude    FLOAT, 
    artist_longitude   FLOAT, 
    artist_location    VARCHAR, 
    artist_name        VARCHAR, 
    title              VARCHAR, 
    duration           NUMERIC,
    year               INT)
""")

songplay_table_create = (""" 
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id        INT IDENTITY(0,1) PRIMARY KEY, 
    start_time         TIMESTAMP NOT NULL, 
    user_id            VARCHAR NOT NULL, 
    level              VARCHAR, 
    song_id            VARCHAR NOT NULL, 
    artist_id          VARCHAR NOT NULL, 
    session_id         INT, 
    location           VARCHAR,
    user_agent         VARCHAR)
                         """)

user_table_create = (""" 
CREATE TABLE IF NOT EXISTS users (
    user_id           VARCHAR PRIMARY KEY,
    first_name        VARCHAR, 
    last_name         VARCHAR, 
    gender            VARCHAR, 
    level             VARCHAR)
                    """)

song_table_create = (""" 
CREATE TABLE IF NOT EXISTS songs (
    song_id           VARCHAR PRIMARY KEY,
    title             VARCHAR, 
    artist_id         VARCHAR NOT NULL, 
    year              INT,
    duration          NUMERIC)
                    """)

artist_table_create = (""" 
CREATE TABLE IF NOT EXISTS artists (
    artist_id         VARCHAR PRIMARY KEY,
    name              VARCHAR, 
    location          VARCHAR, 
    latitude          FLOAT,
    longitude         FLOAT)
                       """)

time_table_create = (""" 
CREATE TABLE IF NOT EXISTS time (
    start_time        TIMESTAMP PRIMARY KEY,
    hour              INT, 
    day               INT, 
    week              INT, 
    month             INT, 
    year              INT,
    weekday           INT)
                    """)

# STAGING TABLES

staging_events_copy = ("""
    COPY {} FROM {}
    iam_role {}
    json {} region '{}';
""").format(
    'staging_events', 
    config.get('S3','LOG_DATA'), 
    config.get('IAM_ROLE','ARN'), 
    config.get('S3','LOG_JSONPATH'),
    config.get('CLUSTER','REGION'))
    
staging_songs_copy = ("""
    COPY {} FROM {}
    iam_role {}
    json 'auto' region '{}';
""").format(
    'staging_songs', 
    config.get('S3','SONG_DATA'), 
    config.get('IAM_ROLE','ARN'), 
    config.get('CLUSTER','REGION'))

# FINAL TABLES

songplay_table_insert = (""" 
INSERT INTO songplays (
    start_time, user_id, level, song_id,
    artist_id, session_id, location, user_agent) 
    SELECT DISTINCT 
                    TIMESTAMP 'epoch' + (e.start_time/1000 * INTERVAL '1 second') AS start_time,
                    e.user_id,
                    e.level,
                    s.song_id,
                    s.artist_id,
                    e.session_id,
                    e.location,
                    e.user_agent
      FROM staging_events e
      LEFT JOIN staging_songs s 
        ON e.song = s.title 
       AND e.artist_name = s.artist_name 
       AND e.length = s.duration
     WHERE e.page = 'NextSong'
       AND e.user_id is NOT NULL
                        """)

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
                    user_id,
                    first_name,
                    last_name,
                    gender,
                    level
      FROM staging_events
     WHERE  user_id is NOT NULL
                    """)

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
   SELECT DISTINCT 
                   song_id, 
                   title,
                   artist_id,
                   year,
                   duration
     FROM staging_songs
    WHERE song_id IS NOT NULL
                    """)

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
                    artist_id,
                    artist_name,
                    artist_location,
                    artist_latitude,
                    artist_longitude
      FROM staging_songs
     WHERE artist_id IS NOT NULL
                       """)

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
           extract(hour from start_time),
           extract(day from start_time),
           extract(week from start_time), 
           extract(month from start_time),
           extract(year from start_time), 
           extract(dayofweek from start_time)
      FROM songplays
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
