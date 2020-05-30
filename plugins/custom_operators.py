"""Custom airflow operators"""
import logging

from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator
from airflow.models import Variable
import boto3
import requests


def get_region():
    """Determine what reagin the instance is running in.
       (https://docs.aws.amazon.com/AWSEC2/latest/WindowsGuide/instance-identity-documents.html)

    Returns:
        [string] -- region id string (i.e. 'us-west-2')
    """
    response_json = requests.get(
        "http://169.254.169.254/latest/dynamic/instance-identity/document"
    ).json()
    return response_json.get("region")


def client(region_name):
    """Create a low-level EMR service client.

    Arguments:
        region_name {string} -- Region where the client should be situated

    Returns:
        Service client instance
    """
    return boto3.client("emr", region_name=region_name)


def get_cluster_status(emr_client, cluster_id):
    """Get state of the EMR cluster

    Arguments:
        emr_client -- EMR client instance
        cluster_id -- EMR cluster ID

    Returns:
        string -- Cluster state
    """
    response = emr_client.describe_cluster(ClusterId=cluster_id)
    return response["Cluster"]["Status"]["State"]


region = get_region()
emr = client(region)


class ETLDAGCheckCompleteSensor(BaseSensorOperator):
    """Check if all ETL are finished, not necessarily successful"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ETLDAGCheckCompleteSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        dag_normalize_state = Variable.get("dag_normalize_state")
        dag_analytics_state = Variable.get("dag_analytics_state")
        return bool(dag_normalize_state != "na" and dag_analytics_state != "na")


class NormalizeDAGCheckCompleteSensor(BaseSensorOperator):
    """Check if normalized tables are successfully created"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(NormalizeDAGCheckCompleteSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        dag_normalize_state = Variable.get("dag_normalize_state")
        return bool(dag_normalize_state == "done")


class ClusterCheckSensor(BaseSensorOperator):
    """Check if cluster is up and available"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(ClusterCheckSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        try:
            cluster_id = Variable.get("cluster_id")
            status = get_cluster_status(emr, cluster_id)
            logging.info(status)
            return bool(status == "WAITING")
        except Exception as exception:
            logging.info(exception)
            return False


class CustomPlugin(AirflowPlugin):
    """Define which oprators are exported"""

    name = "custom_plugin"
    operators = [
        ClusterCheckSensor,
        ETLDAGCheckCompleteSensor,
        NormalizeDAGCheckCompleteSensor,
    ]
