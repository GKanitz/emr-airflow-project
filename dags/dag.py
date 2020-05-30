"""Airflow dag definition"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import emrlib.emr_lib as emr


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2016, 1, 1, 0, 0, 0, 0),
    "end_date": datetime(2016, 12, 1, 0, 0, 0, 0),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "provide_context": True,
}

# Initialize the DAG
dag = DAG(
    "trasform_immigration",
    concurrency=3,
    schedule_interval="@monthly",
    default_args=default_args,
    max_active_runs=1,
)
region = emr.get_region()
emr.client(region_name=region)


def create_emr():
    """Create a new EMR cluster"""
    cluster_id = emr.create_cluster(
        region_name=region, cluster_name="immigration_cluster", num_core_nodes=2
    )
    return cluster_id


def wait_for_completion(**kwargs):
    """Waits for the EMR cluster to be ready to accept jobs"""
    task_instance = kwargs["ti"]
    cluster_id = task_instance.xcom_pull(task_ids="create_cluster")
    emr.wait_for_cluster_creation(cluster_id)


def terminate_emr(**kwargs):
    """Terminates the EMR cluster"""
    task_instance = kwargs["ti"]
    cluster_id = task_instance.xcom_pull(task_ids="create_cluster")
    emr.terminate_cluster(cluster_id)


def submit_to_emr(script_file, **kwargs):
    """Submits an EMR job

    Arguments:
        script_file {string} -- Python script file that should be executed for the job
    """
    task_instance = kwargs["ti"]
    cluster_id = task_instance.xcom_pull(task_ids="create_cluster")
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, "spark")
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    execution_date = kwargs["execution_date"]
    month = execution_date.strftime("%b").lower()
    year = execution_date.strftime("%y")
    statement_response = emr.submit_statement(
        session_url, script_file, f"month_year = '{month + year}'\n"
    )
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)


# Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id="create_cluster", python_callable=create_emr, dag=dag
)

wait_for_cluster_completion = PythonOperator(
    task_id="wait_for_cluster_completion", python_callable=wait_for_completion, dag=dag
)

transform_labels = PythonOperator(
    task_id="transform_immigration",
    python_callable=submit_to_emr,
    op_kwargs={"script_file": "/root/airflow/dags/transform/labels.py"},
    dag=dag,
)

transform_immigration = PythonOperator(
    task_id="transform_immigration",
    python_callable=submit_to_emr,
    op_kwargs={"script_file": "/root/airflow/dags/transform/immigration.py"},
    dag=dag,
)

transform_states = PythonOperator(
    task_id="transform_immigration",
    python_callable=submit_to_emr,
    op_kwargs={"script_file": "/root/airflow/dags/transform/states.py"},
    dag=dag,
)

terminate_cluster = PythonOperator(
    task_id="terminate_cluster",
    python_callable=terminate_emr,
    trigger_rule="all_done",
    dag=dag,
)

# pylint: disable=pointless-statement
create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> transform_labels >> transform_immigration
wait_for_cluster_completion >> transform_states >> transform_immigration
transform_immigration >> terminate_cluster
