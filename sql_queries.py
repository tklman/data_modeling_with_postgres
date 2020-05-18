# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY, 
    start_time varchar, 
    user_id varchar, 
    level varchar, 
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name varchar, 
    last_name varchar,
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar NOT NULL, 
    year int, 
    duration numeric
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar, 
    location varchar, 
    latitude varchar, 
    longitude varchar
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time timestamptz PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
);
""")

# Staging in tables in order to use copy_from

sti_songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS sti_songplays (
    songplay_id varchar, 
    start_time varchar, 
    user_id varchar, 
    level varchar, 
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    user_agent varchar
);
""")

sti_user_table_create = ("""
CREATE TABLE IF NOT EXISTS sti_users (
    user_id int, 
    first_name varchar, 
    last_name varchar,
    gender varchar, 
    level varchar
);
""")

sti_song_table_create = ("""
CREATE TABLE IF NOT EXISTS sti_songs (
    song_id varchar, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration numeric
);
""")

sti_artist_table_create = ("""
CREATE TABLE IF NOT EXISTS sti_artists (
    artist_id varchar, 
    name varchar, 
    location varchar, 
    latitude varchar, 
    longitude varchar
);
""")

sti_time_table_create = ("""
CREATE TABLE IF NOT EXISTS sti_time (
    start_time timestamptz, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO users
SELECT *
FROM sti_users
ON CONFLICT(user_id) DO NOTHING;
TRUNCATE sti_users;
""")

song_table_insert = ("""
INSERT INTO songs
SELECT *
FROM sti_songs
ON CONFLICT(song_id) DO NOTHING;
TRUNCATE sti_songs;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT *
FROM sti_artists
ON CONFLICT(artist_id) DO NOTHING;
TRUNCATE sti_artists;
""")

time_table_insert = ("""
INSERT INTO time
SELECT *
FROM sti_time
ON CONFLICT(start_time) DO NOTHING;
TRUNCATE sti_time;
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, songs.artist_id
FROM songs
INNER JOIN artists
ON artists.artist_id = songs.artist_id
WHERE songs.title = %s
AND artists.name = %s
AND songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    sti_songplay_table_create,
    sti_user_table_create,
    sti_song_table_create,
    sti_artist_table_create,
    sti_time_table_create
]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]