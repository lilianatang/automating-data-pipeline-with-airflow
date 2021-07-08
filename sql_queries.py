import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config['S3']['LOG_DATA'] 
SONG_DATA = config['S3']['SONG_DATA'] 
LOG_JSONPATH = config['S3']['LOG_JSONPATH'] 
HOST= config['CLUSTER']['HOST']
DB_NAME= config['CLUSTER']['DB_NAME']
DB_USER= config['CLUSTER']['DB_USER']
DB_PASSWORD= config['CLUSTER']['DB_PASSWORD']
DB_PORT= config['CLUSTER']['DB_PORT']
KEY = config['CLUSTER']['KEY']
SECRET = config['CLUSTER']['SECRET']
ARN= config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession INT,
        lastName varchar,
        length varchar,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId INT,
        song varchar,
        status INT,
        ts bigint NOT NULL,
        userAgent varchar,
        userId INT
    ); 
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id varchar,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year INT
    ); 
    """
)

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int identity(0, 1) primary key sortkey,
        session_id int,
        location varchar,
        user_agent varchar,
        start_time timestamp NOT NULL,
        user_id int NOT NULL distkey,
        artist_id varchar,
        song_id varchar,
        level varchar
    );    
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar
    );    
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        year int NOT NULL,
        duration numeric NOT NULL,
        artist_id varchar
    );
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude numeric,
        longitude numeric
    );    
    """
)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    );    
    """
)



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]