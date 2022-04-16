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

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
    artist_name     text,
    auth            text,
    first_name      text,
    gender          varchar,
    item_in_session int,
    last_name       text,
    length          float,
    level           text,
    location        text,
    method          text,
    page            text,
    registration    bigint,
    session_id      int,
    song            text,
    status          int,
    ts              bigint,
    user_agent      text,
    user_id         bigint
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
    num_songs           int,
    artist_id           text,
    artist_latitude     text,
    artist_longitude    text,
    artist_location     text,
    artist_name         text,
    song_id             text,
    title               text,
    duration            decimal,
    year                int
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id     bigint identity(0,1) primary key,
        start_time      timestamp sortkey,
        user_id         bigint distkey,
        level           text,
        song_id         text,
        artist_id       text,
        session_id      int,
        location        text,
        user_agent      text
    )
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id         bigint primary key sortkey distkey,
        first_name      text,
        last_name       text,
        gender          varchar,
        level           text
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
    song_id         text primary key sortkey,
    title           text,
    artist_id       text,
    year            int,
    duration        decimal
    )
""")

artist_table_create = ("""
    CREATE TABLE artists(
    artist_id       text primary key sortkey,
    name            text,
    location        text,
    latitude        text,
    longitude       text
    )
""")

time_table_create = ("""
    CREATE TABLE time(
    start_time  timestamp primary key sortkey,
    hour        int,
    day         int,
    week        int,
    month       int,
    year        int,
    weekday     int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {}
    CREDENTIALS 'aws_iam_role={}'
    format as JSON {}
    compupdate off
    region 'us-west-2';
""").format(config.get("S3", "LOG_DATA"),
            config.get("IAM_ROLE", "ARN"),
            config.get("S3", "LOG_JSONPATH"),
            )

staging_songs_copy = ("""
    COPY staging_songs from {}
    CREDENTIALS 'aws_iam_role={}'
    format as JSON 'auto'
    compupdate off
    region 'us-west-2';
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT DISTINCT
        timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
        e.user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e, staging_songs s
        WHERE e.song = s.title AND e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
        WHERE page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        'epoch'::timestamp + e.ts/1000 * '1 second'::interval as start_time,
        extract(hour from start_time) as hour,
        extract(day from start_time) as day,
        extract(week from start_time) as week,
        extract(month from start_time) as month,
        extract(year from start_time) as year,
        extract(weekday from start_time) as weekday
    FROM staging_events e
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
