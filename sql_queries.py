#import libraries
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

#CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(event_id INT IDENTITY(0,1),
artist text,
auth text,
firstName text,
gender text,
itemInSession int,
lastName text,
length float,
level text,
location text,
method text,
page text,
registration bigint,
sessionId int,
song text,
status int,
ts bigint,
userAgent text,
userId int)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(num_songs int,
artist_id text,
artist_latitude float,
artist_longitude float,
artist_location text,
artist_name text,
song_id text,
title text,
duration float,
year int)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(songplay_id int IDENTITY(0,1) PRIMARY KEY,
 start_time datetime NOT NULL,
 user_id int NOT NULL,
 level text,
 song_id text,
 artist_id text,
 session_id int,
 location text,
 user_agent text)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(user_id int NOT NULL PRIMARY KEY,
first_name text NOT NULL,
last_name text NOT NULL,
gender text,
level text)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(song_id text NOT NULL PRIMARY KEY,
 title text NOT NULL,
 artist_id text NOT NULL,
 year int,
 duration float)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artist_id text NOT NULL PRIMARY KEY,
 name text NOT NULL,
 location text,
 latitude float,
 longitude float)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time datetime NOT NULL,
 hour int,
 day int,
 week_of_year int,
 month int,
 year int,
 weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
iam_role {}
json {}
region 'us-west-2';
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
copy staging_songs from {} 
iam_role {}
json 'auto'
region 'us-west-2'
COMPUPDATE OFF 
STATUPDATE OFF;
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' +  se.ts/1000 * interval '1 second' AS start_time, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
FROM staging_events se
JOIN staging_songs ss ON se.song = ss.title
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, max(level)
FROM (SELECT * FROM staging_events WHERE page = 'NextSong' AND userId IS NOT NULL ORDER BY ts ASC )
GROUP BY userId, firstName, lastName, gender
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week_of_year, month, year, weekday)
SELECT DISTINCT start_time, extract(hour from start_time) AS hour, extract(day from start_time) AS day, extract(week from start_time) AS week, extract(month from start_time) AS month, extract(year from start_time) AS year, extract(weekday from start_time) AS weekday
FROM songplays
WHERE start_time IS NOT NULL
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
