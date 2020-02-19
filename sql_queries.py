import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist text, 
auth text, 
firstName text, 
gender text, 
ItemInSession int,
lastName text, 
length float, 
level text, 
location text, 
method text,
page text, 
registration text, 
sessionId int, 
song text, 
status int,
ts bigint, 
userAgent text, 
userId int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
artist_id text, 
artist_latitude float, 
artist_longitude float, 
artist_location text, 
artist_name text, 
song_id text, 
title text, 
duration float, 
year smallint);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay
(
    songplay_id integer IDENTITY(0,1),
    start_time bigint NOT NULL sortkey,
    user_id text,
    level text NOT NULL,
    song_id text distkey,
    artist_id text,
    session_id integer,
    location text,
    user_agent text
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user
(
    user_id text sortkey,
    first_name text,
    last_name text,
    gender char(1),
    level text NOT NULL
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song
(
    song_id text distkey sortkey,
    title text NOT NULL,
    artist_id text,
    year smallint,
    duration float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist
(
    artist_id text sortkey,
    name text NOT NULL,
    location text,
    latitude float,
    longitude float
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    start_time bigint sortkey,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs (artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, 
duration, year)
from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON 'auto' truncatecolumns
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT ts, userId, level, song_id, artist_id, sessionId, location, userAgent
FROM staging_events ste LEFT JOIN staging_songs sts 
    ON ste.song = sts.title AND ste.artist = sts.artist_name AND ste.length = sts.duration
WHERE page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO dim_user (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level 
FROM staging_events
WHERE userId IS NOT NULL;
""")


song_table_insert = ("""
INSERT INTO dim_song (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration 
FROM staging_songs
WHERE song_id IS NOT NULL;
""")


artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts, extract(hour from TIMESTAMP 'epoch' + ts/1000 * interval '1 second'), 
                    extract(day from TIMESTAMP 'epoch' + ts/1000 * interval '1 second'), 
                    extract(week from TIMESTAMP 'epoch' + ts/1000 * interval '1 second'), 
                    extract(month from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') ,
                        extract(year from TIMESTAMP 'epoch' + ts/1000 * interval '1 second'), 
                        extract(weekday from TIMESTAMP 'epoch' + ts/1000 * interval '1 second') 
FROM staging_events
WHERE page = 'NextSong';
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


