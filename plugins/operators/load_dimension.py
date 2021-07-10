from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.
"""
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = '''INSERT INTO {table_name} {sql}'''
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table_name = "",
                 sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        insert_data = LoadDimensionOperator.insert_sql.format(table_name = self.table_name, sql = self.sql)
        redshift.run(insert_data)
        self.log.info('Dimension Data Was Imported')
              
            
