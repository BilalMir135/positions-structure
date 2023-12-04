from dags.build_dag import build_dag
from include.common.constants.index import PROTOCOLS

for dag_id in PROTOCOLS:
    globals()[dag_id] = build_dag(dag_id)