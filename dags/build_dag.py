import os

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig, ExecutionConfig

from include.common.utils.bigquery import create_dataset, execute_query
from include.common.utils.logs_decoder_udf import decode_logs_udf
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from include.common.constants.index import PROTOCOL_POSITIONS_PATH

def build_dag(project):
    @dag(dag_id=project,
         schedule=None,
         start_date=datetime(2023,11,1),
         catchup=False)
    def new_dag():

        dataset_id = f"p_{project}"

        _start = EmptyOperator(task_id="start")

        _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

        _create_ds = create_dataset(task_id='create_dataset', dataset_id=dataset_id)

        _add_logs_decoder_udf = execute_query(
             task_id='add_logs_decoder_udf',
             sql=generate_parser_udfs_sql(project)
            )
        
        transform = DbtTaskGroup(
            group_id='transform',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=[f'path:models/protocol_positions/{project}/transform']
            ),
            execution_config=ExecutionConfig(
                dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
            ),
            operator_args={
            "install_deps": True,  
            "full_refresh": True,
            },
    )
    
        chain(_start, _create_ds, _add_logs_decoder_udf, transform, _finish)
    
    generated_dag = new_dag()

    return generated_dag



def generate_parser_udfs_sql(project):
    parser_directory = os.path.join(PROTOCOL_POSITIONS_PATH, project, 'parser')
    dataset_id = f"p_{project}"
    sql = ''

    for filename in os.listdir(parser_directory):
        abi_file_path = os.path.join(parser_directory,filename)
        sql += decode_logs_udf(dataset_id=dataset_id,abi_file_path=abi_file_path)

    return sql