from sqlalchemy import and_
import time
import logging

from airflow.utils.decorators import apply_defaults
from airflow.utils.db import provide_session
from airflow.models import BaseOperator, DagRun

from airflow.hooks import PostgresHook


class TableCleanupOperator(BaseOperator):
    """
    Table Cleanup Operator

    This operator looks for tables in a specified database and schema and
    drops any that contain a specified key.

    :param db_conn_id:          The database connection id.
    :type db_conn_id:           string
    :param db_type:             The type of database being connected to.
                                Currently, only Redshift is supported.
    :type db_type:              string
    :param db_schema:           The relevant schema to inspect within
                                the database.
    :type db_schema:            string
    :param key:                 The substring to look for in the table names.
                                If a match is found for any part of the string,
                                the table will be dropped.
    :type key:                  string
    :param dag_dependencies:    Any other dags that may be running. If a dag_run
                                corresponding to a dag_id providing is found to
                                have the state "running", this operator will
                                sleep for a configurable amount of time.
    :type dag_dependencies:     string/list
    :param sleep_duration:      The amount of time the operator should sleep
                                before trying again if a dag_dependencies
                                conflict is found.
    :type sleep_duration:       int
    """

    template_field = ()

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 db_type='redshift',
                 db_schema=None,
                 key='tmp',
                 dag_dependencies=[],
                 sleep_duration=15,
                 *args,
                 **kwargs
                 ):
        super().__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_type = db_type.lower()
        self.db_schema = db_schema
        self.key = key
        self.dag_dependencies = dag_dependencies
        self.sleep_duration = sleep_duration

        if db_type.lower() not in ('redshift'):
            raise ValueError('The only db currently supported is Redshift.')

        if not isinstance(self.sleep_duration, int):
            raise ValueError('Please specify "sleep_duration" as an integer.')

    def execute(self, context):

        if self.dag_dependencies:
            self.check_for_dependencies()

        tables_sql = \
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{0}'
        """.format(self.db_schema)

        hook = PostgresHook(self.db_conn_id)

        records = [record[0] for record in hook.get_records(tables_sql)]

        for record in records:
            if self.key in record:
                logging.info('Dropping: {}.{}'.format(str(self.db_schema),
                                                      str(self.key)))
                drop_sql = \
                """
                DROP TABLE {0}.{1}
                """.format(self.db_schema, record)

                hook.run(drop_sql)


    @provide_session
    def check_for_dependencies(self, session=None):
        if isinstance(self.dag_dependencies, str):
            running = (session
                       .query(DagRun)
                       .filter(and_(DagRun.dag_id == self.dag_dependencies,
                                    DagRun.state == 'running'))
                       .all())
        elif isinstance(self.dag_dependencies, list):
            running = []
            for dag in self.dag_dependencies:
                running.extend((session
                                .query(DagRun)
                                .filter(and_(DagRun.dag_id == dag,
                                             DagRun.state == 'running'))
                                .all()))

        if running:
            logging.info('Dag Dependencies running.')
            logging.info('Sleeping for {} seconds.'.format(str(self.sleep_duration)))
            time.sleep(self.sleep_duration)
            self.check_for_dependencies()
        else:
            logging.info('No Dag Dependencies running.')
            logging.info('Proceeding...')
