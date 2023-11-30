from glob import glob

from dags.build_dag import build_dag

protocols = ['aave', 'unipilot', 'uniswapv3']

for protocol in glob('/usr/local/airflow/projects/*'):
    dag_id = protocol.split('/')[-1] #name of project
    globals()[dag_id] = build_dag(dag_id)
