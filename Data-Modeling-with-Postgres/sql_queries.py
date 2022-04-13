# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# timestamp, userID, level, song_id, artist_id, session_id, location, userAgent

user_table_create = """
    CREATE TABLE IF NOT EXISTS users (
                       user_id int PRIMARY KEY,
                       firstName varchar,
                       lastName varchar,
                       gender varchar,
                       level varchar
                       );
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs (
                       song_id varchar PRIMARY KEY,
                       title varchar NOT NULL,
                       artist_id varchar,
                       year int,
                       duration numeric NOT NULL
                       );
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        artist_name varchar NOT NULL,
        artist_location varchar,
        latitude double precision,
        longitude double precision
        );
"""
time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        time_id SERIAL PRIMARY KEY,
        start_time timestamp,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    );
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_ID SERIAL PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id int NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id varchar,
    location varchar,
    userAgent varchar
    );
"""


# INSERT RECORDS

songplay_table_insert = """
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        userAgent
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

user_table_insert = """
    INSERT INTO users (
        user_id,
        firstName,
        lastName,
        gender,
        level
        )
        VALUES (%s, %s, %s, %s, %s) on conflict(user_id) do update set level
        = excluded.level"""

song_table_insert = """
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
        )
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

artist_table_insert = """
    INSERT INTO artists (
        artist_id,
        artist_name,
        artist_location,
        latitude,
        longitude
        )
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

time_table_insert = """
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""

# FIND SONGS
# Implement the song_select query in sql_queries.py to find the song ID and artist ID based on the title, artist name, and duration of a song.
song_select = """
    SELECT s.song_id,
           a.artist_id
    FROM   songs s
    JOIN   artists a ON a.artist_id = s.artist_id
        WHERE s.title = %s
        AND a.artist_name = %s
        AND s.duration = %s
"""

# QUERY LISTS

create_table_queries = [songplay_table_create, song_table_create, user_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
