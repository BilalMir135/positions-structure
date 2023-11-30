import os

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from datetime import datetime

from include.common.utils.bigquery import create_dataset, execute_query
from include.common.utils.logs_decoder_udf import decode_logs_udf

def build_dag(project):
    @dag(dag_id=project,
         schedule=None,
         start_date=datetime(2023,11,1),
         catchup=False)
    def new_dag():

        dataset_id = project

        _start = EmptyOperator(task_id="start")

        _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

        _create_ds = create_dataset(task_id='create_dataset', dataset_id=dataset_id)

        _add_logs_decoder_udf = execute_query(
             task_id='add_logs_decoder_udf',
             sql=generate_parser_udfs_sql(project)
            )
    
        chain(_start, _create_ds, _add_logs_decoder_udf, _finish)
    
    generated_dag = new_dag()

    return generated_dag



def generate_parser_udfs_sql(project):
    parser_directory = f"/usr/local/airflow/projects/{project}/parser"

    sql = ''
    for filename in os.listdir(parser_directory):
        abi_file_path = os.path.join(parser_directory,filename)
        sql += decode_logs_udf(dataset_id=project,abi_file_path=abi_file_path)

    return sql