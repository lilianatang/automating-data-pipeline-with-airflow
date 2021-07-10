from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
"""
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

"""
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 test_cases = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        for case in self.test_cases:
            result = int(redshift_hook.get_first(sql=test_cases['sql'])[0])
            # check if equal
            if case['op'] == 'eq':
                if result != case['val']:
                    raise AssertionError(f"Check failed: {result} {case['op']} {case['val']}")
            # check if not equal
            elif case['op'] == 'ne':
                if result == case['val']:
                    raise AssertionError(f"Check failed: {result} {case['op']} {case['val']}")
            # check if greater than
            elif case['op'] == 'gt':
                if result <= case['val']:
                    raise AssertionError(f"Check failed: {result} {case['op']} {case['val']}")
            self.log.info(f"Passed check: {result} {case['op']} {case['val']}")