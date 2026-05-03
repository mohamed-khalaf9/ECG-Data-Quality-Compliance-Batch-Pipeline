from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Mohamed khalaf',
    'depends_on_past': False,
    'start_date': datetime(2026, 5, 1), #  run immediately
    'retries': 1,                       
    'retry_delay': timedelta(minutes=2) 
}


with DAG(
    dag_id='clinical_data_orchestration',
    default_args=default_args,
    description='Orchestrates the ECG Machine and Waveform Beam pipelines',
    schedule='@daily',        
    catchup=False,                      
) as dag:

    
    clean_machine_task = BashOperator(
        task_id='clean_machine_measurements',
        bash_command='python /opt/airflow/dags/beam-cleaining-machine-measurements.py '
    )

    clean_waveform_task = BashOperator(
        task_id='clean_waveform_notes',
        bash_command='python /opt/airflow/dags/beam-cleaning-waveform-note-links.py '
    )

    join_enrich_task = BashOperator(
        task_id='join_and_enrich_data',
        bash_command='python /opt/airflow/dags/beam-join-and-enrich.py '
    )

    aggregate_audit_task = BashOperator(
        task_id='aggregate_and_audit',
        bash_command='python /opt/airflow/dags/beam-aggregate-and-audit.py '
    )

    clean_machine_task >> clean_waveform_task >> join_enrich_task >> aggregate_audit_task