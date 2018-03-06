"""

    Cleanup Plugin

    This plugin provides an interface to the Github v3 API.


"""

from airflow.plugins_manager import AirflowPlugin
from cleanup_plugin.operators.table_cleanup_operator import TableCleanupOperator


class CleanupPlugin(AirflowPlugin):
    name = "cleanup_plugin"
    operators = [TableCleanupOperator]
    hooks = []
