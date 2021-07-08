from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.
"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_user_data = """INSERT INTO users (user_id, first_name, last_name, gender, level) SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events WHERE page= 'NextSong' AND userId NOT IN (SELECT DISTINCT user_id FROM users)"""
    
    insert_artist_data = """INSERT INTO artists (artist_id, name, location, latitude, longitude) SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)"""
    
    insert_song_data = """INSERT INTO songs (song_id, title, year, duration, artist_id) SELECT DISTINCT song_id, title, year, duration, artist_id FROM staging_songs WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)"""
    
    insert_time_data = """INSERT INTO time (start_time, hour, day, week, month, year, weekday) SELECT start_time, EXTRACT(hour FROM start_time) AS hour, EXTRACT(day FROM start_time), EXTRACT(week FROM start_time) AS day, EXTRACT(month FROM start_time), EXTRACT(year FROM start_time) AS year, EXTRACT(weekday FROM start_time) AS weekday FROM (SELECT DISTINCT timestamp 'epoch' + s.ts/1000 * INTERVAL '1 second' as start_time FROM staging_events s) WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);"""

        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table_name = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name        

    def execute(self, context):
        if self.table_name == "users":
            redshift = PostgresHook(self.redshift_conn_id)
            redshift.run(insert_user_data)
            self.log.info('User Data Was Imported')
        elif self.table_name == "artists":
            redshift = PostgresHook(self.redshift_conn_id)
            redshift.run(insert_artist_data)
            self.log.info('Artist Data Was Imported')
        elif self.table_name == "songs":
            redshift = PostgresHook(self.redshift_conn_id)
            redshift.run(insert_song_data)
            self.log.info('Song Data Was Imported')
        elif self.table_name == "time":
            redshift = PostgresHook(self.redshift_conn_id)
            redshift.run(insert_time_data)
            self.log.info('Time Data Was Imported')
        else:
            self.log.info('Data Import Failed.')                
            
