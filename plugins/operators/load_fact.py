from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.
"""
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_songplay_data = """INSERT INTO songplays (session_id, location, user_agent, start_time, user_id, artist_id , song_id, level) SELECT e.sessionId, e.location, e.userAgent, TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time, e.userId, a.artist_id, s.song_id, e.level FROM staging_events e LEFT OUTER JOIN artists a ON a.name = e.artist    LEFT OUTER JOIN songs s ON s.title = e.song WHERE e.page= 'NextSong'"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 append_data = None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.append_data == True:            
            redshift.run(insert_songplay_data)
            self.log.info('Fact Data Was Imported Through Append')
        else:
            sql_statement = 'DELETE FROM songplays'
            redshift.run(sql_statement)
            sql_statement = insert_songplay_data
            redshift.run(sql_statement)
            self.log.info('Fact Data Was Imported Through Truncate')