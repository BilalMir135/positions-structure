from glob import glob

GCP_CONN_ID = 'gcp'
PROJECT_ID = 'spock-main'
PROTOCOL_POSITIONS_PATH = '/usr/local/airflow/include/dbt/models/protocol_positions'

PROTOCOLS = [protocol.split('/')[-1] for protocol in glob('/usr/local/airflow/include/dbt/models/protocol_positions/*')]
