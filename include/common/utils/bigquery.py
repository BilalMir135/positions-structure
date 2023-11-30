from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryExecuteQueryOperator

from include.common.constants.index import GCP_CONN_ID

def create_dataset(task_id, dataset_id):
    return BigQueryCreateEmptyDatasetOperator(
            task_id = task_id,
            dataset_id = dataset_id,
            gcp_conn_id = GCP_CONN_ID,
            if_exists = 'skip',
        )

def execute_query(task_id, sql):
    return BigQueryExecuteQueryOperator(
        task_id = task_id,
        sql = sql,
        use_legacy_sql = False,
        gcp_conn_id = GCP_CONN_ID,
    )