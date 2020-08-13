import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
iam_role = config['IAM_ROLE']['ARN']
log_data_location = config['S3']['LOG_DATA']
song_data_location = config['S3']['SONG_DATA']
log_json_path = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events (artist varchar, auth varchar, firstName varchar, gender varchar, 
itemInSession int, lastName varchar, length float, level varchar, location varchar, method varchar, page varchar, registration bigint,
       sessionId int, song varchar, status int, ts bigint, userAgent varchar, userId int)
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs (artist_id varchar, artist_latitude numeric, artist_location varchar, artist_longitude numeric, artist_name varchar, duration numeric, num_songs int, song_id varchar, title varchar, year int)
""")

songplay_table_insert = (""" 
INSERT INTO songplays
(SELECT events.ts, events.userId, events.level, songs.song_id, songs.artist_id, events.sessionId, events.location, events.user_agent
FROM staging_events as events
JOIN staging_songs as songs
ON staging_events.song = staging_songs.title)
""")


songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(0, 1) PRIMARY KEY, start_time bigint, user_id int, level varchar, 
song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar)
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float)
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude float, longitude float)
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (start_time bigint PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int)
""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events

from {}

iam_role {}
json {}

region 'us-west-2';
""").format(log_data_location, iam_role, log_json_path)

staging_songs_copy = (""" COPY staging_songs (artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year)

from {}

iam_role {}
json 'auto'

region 'us-west-2';
""").format(song_data_location, iam_role)

# FINAL TABLES

songplay_table_insert = (""" 
INSERT INTO songplays
(SELECT ts, userId, lastName, gender, level,
FROM staging_events)

(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) 
ON CONFLICT (user_id) 
DO NOTHING;
INSERT INTO users
(SELECT userId, firstName, lastName, gender, level,
FROM staging_events)
""")

song_table_insert = (""" 
INSERT INTO songs
(SELECT song_id, title, artist_id, year, duration,
FROM staging_songs)
""")

artist_table_insert = ("""
INSERT INTO artists
(SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude,
FROM staging_songs)
""")

time_table_insert = (""" 
INSERT INTO time
(SELECT ts, extract(hour from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour, 
extract(day from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day, 
extract(week from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
extract(month from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
extract(year from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year, 
extract(weekday from TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
