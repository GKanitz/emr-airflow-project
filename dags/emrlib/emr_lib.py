"""Library to interact with EMR cluster"""
import json
import time
import logging
from datetime import datetime
import boto3
import requests


def get_region():
    """Determine what reagin the instance is running in.
       (https://docs.aws.amazon.com/AWSEC2/latest/WindowsGuide/instance-identity-documents.html)

    Returns:
        str -- region id string (i.e. 'us-west-2')
    """
    response = requests.get(
        "http://169.254.169.254/latest/dynamic/instance-identity/document"
    )
    response_json = response.json()
    return response_json.get("region")


def client(region_name):
    """Create a low-level EMR service client.

    Arguments:
        region_name {str} -- Region where the client should be situated

    Returns:
        Service client instance
    """
    global EMR
    EMR = boto3.client("emr", region_name=region_name)


def get_security_group_id(group_name, region_name):
    """Describes the specified security groups.

    Arguments:
        group_name {list} -- [EC2-Classic and default VPC only] The names of the security groups. You can specify either the security group name or the security group ID.
        region_name {str} -- Region where the client is situated

    Returns:
        dict -- Description of security groups.
    """
    ec2 = boto3.client("ec2", region_name=region_name)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response["SecurityGroups"][0]["GroupId"]


def create_cluster(
    region_name,
    cluster_name="Airflow-" + str(datetime.now()),
    release_label="emr-5.9.0",
    master_instance_type="m3.xlarge",
    num_core_nodes=2,
    core_node_instance_type="m3.xlarge",
):
    """Create new EMR cluster

    Arguments:
        region_name {str} -- Region where the client is situated

    Keyword Arguments:
        cluster_name {str} -- Cluster name (default: {"Airflow-"+str(datetime.now())})
        release_label {str} -- The Amazon EMR release label, which determines the version of
        open-source application packages installed on the cluster. Release labels are in the form
        emr-x.x.x , where x.x.x is an Amazon EMR release version such as emr-5.14.0 . For more
        information about Amazon EMR release versions and included application versions and
        features, see https://docs.aws.amazon.com/emr/latest/ReleaseGuide/ . The release label
        applies only to Amazon EMR releases version 4.0 and later. Earlier versions use AmiVersion.
        (default: {"emr-5.9.0"})
        master_instance_type {str} -- The EC2 instance type of the master node.
        (default: {"m3.xlarge"})
        num_core_nodes {int} -- The number of slave nodes in the cluster. (default: {2})
        core_node_instance_type {str} -- The EC2 instance type for the salve nodes.
        (default: {"m3.xlarge"})

    Returns:
        str -- An unique identifier for the job flow.
    """
    emr_master_security_group_id = get_security_group_id(
        "AirflowEMRMasterSG", region_name=region_name
    )
    emr_slave_security_group_id = get_security_group_id(
        "AirflowEMRSlaveSG", region_name=region_name
    )
    cluster_response = EMR.run_job_flow(
        Name=cluster_name,
        ReleaseLabel=release_label,
        Instances={
            "InstanceGroups": [
                {
                    "Name": "Master nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": master_instance_type,
                    "InstanceCount": 1,
                },
                {
                    "Name": "Slave nodes",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "CORE",
                    "InstanceType": core_node_instance_type,
                    "InstanceCount": num_core_nodes,
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": True,
            "Ec2KeyName": "udacity-final-project",
            "EmrManagedMasterSecurityGroup": emr_master_security_group_id,
            "EmrManagedSlaveSecurityGroup": emr_slave_security_group_id,
        },
        VisibleToAllUsers=True,
        JobFlowRole="EmrEc2InstanceProfile",
        ServiceRole="EmrRole",
        Applications=[
            {"Name": "hadoop"},
            {"Name": "spark"},
            {"Name": "hive"},
            {"Name": "livy"},
            {"Name": "zeppelin"},
        ],
    )
    return cluster_response["JobFlowId"]


def get_cluster_dns(cluster_id):
    """Get public DNS name of master node

    Arguments:
        cluster_id {str} -- Cluster Id

    Returns:
        str -- Public DNS name of the master node
    """
    response = EMR.describe_cluster(ClusterId=cluster_id)
    return response["Cluster"]["MasterPublicDnsName"]


def get_cluster_status(cluster_id):
    """Get cluster statud

    Arguments:
        cluster_id {str} -- Cluster Id

    Returns:
        str -- Cluster state ('STARTING'|'BOOTSTRAPPING'|'RUNNING'|'WAITING'|'TERMINATING'|
        'TERMINATED'|'TERMINATED_WITH_ERRORS')
    """
    response = EMR.describe_cluster(ClusterId=cluster_id)
    return response["Cluster"]["Status"]["State"]


def wait_for_cluster_creation(cluster_id):
    """Polls EMR.Client.describe_cluster() every 30 seconds until a successful state is reached.
    An error is returned after 60 failed checks.

    Arguments:
        cluster_id {str} -- Cluster Id
    """
    EMR.get_waiter("cluster_running").wait(ClusterId=cluster_id)


def terminate_cluster(cluster_id):
    """Terminates the cluster

    Arguments:
        cluster_id {str} -- Cluster Id
    """
    EMR.terminate_job_flows(JobFlowIds=[cluster_id])


def create_spark_session(master_dns, kind="spark"):
    """Creates an interactive scala spark session.

    Arguments:
        master_dns {str} -- DNS name of master node of the spark cluster

    Keyword Arguments:
        kind {str} -- Spark session kind to be created Python(kind=pyspark), R(kind=sparkr) and
        SQL(kind=sql) (default: {"spark"})

    Returns:
        object -- Spark session
    """
    # 8998 is the port on which the Livy server runs
    host = "http://" + master_dns + ":8998"
    data = {
        "kind": kind,
        "conf": {
            "spark.jars.packages": "saurfang:spark-sas7bdat:2.0.0-s_2.11",
            "spark.driver.extraJavaOptions": "-Dlog4jspark.root.logger=WARN,console",
        },
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(host + "/sessions", data=json.dumps(data), headers=headers)
    logging.info(response.json())
    return response.headers


def wait_for_idle_session(master_dns, response_headers):
    """Wait for the session to be idle or ready for job submission

    Arguments:
        master_dns {str} -- DNS name of master node of the spark cluster
        response_headers {object} -- Spark session

    Returns:
        str -- Session url
    """
    status = ""
    host = "http://" + master_dns + ":8998"
    logging.info(response_headers)
    session_url = host + response_headers["location"]
    while status != "idle":
        time.sleep(3)
        status_response = requests.get(session_url, headers=response_headers)
        status = status_response.json()["state"]
        logging.info("Session status: %s", status)
    return session_url


def kill_spark_session(session_url):
    """Kills a spark session

    Arguments:
        session_url {str} -- Session url
    """
    requests.delete(session_url, headers={"Content-Type": "application/json"})


def submit_statement(session_url, statement_path, args=""):
    """Submits the code as a simple JSON command to the Livy server

    Arguments:
        session_url {str} -- Session url
        statement_path {str} -- Path to code file

    Keyword Arguments:
        args {str} -- Arguments to be included before code (default: {""})

    Returns:
        object -- Response of the call
    """
    statements_url = session_url + "/statements"
    with open(statement_path, "r") as file:
        code = file.read()
    code = args + code
    data = {"code": code}
    response = requests.post(
        statements_url,
        data=json.dumps(data),
        headers={"Content-Type": "application/json"},
    )
    logging.info("%s", response.json())
    return response


def track_statement_progress(master_dns, response_headers):
    """Function to help track the progress of the scala code submitted to Apache Livy

    Arguments:
        master_dns {str} -- DNS name of master node of the spark cluster
        response_headers {object} -- Spark session

    Raises:
        ValueError: If code terminests with exception

    Returns:
        str -- Log of the executed code
    """
    statement_status = ""
    host = "http://" + master_dns + ":8998"
    session_url = host + response_headers["location"].split("/statements", 1)[0]
    # Poll the status of the submitted scala code
    while statement_status != "available":
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and
        # provides a statement URL that can be polled until it is complete:
        statement_url = host + response_headers["location"]
        statement_response = requests.get(
            statement_url, headers={"Content-Type": "application/json"}
        )
        statement_status = statement_response.json()["state"]
        logging.info("Statement status: %s", statement_status)
        if "progress" in statement_response.json():
            progress = statement_response.json()["progress"]
            logging.info("Progress: %s", str(progress))
        time.sleep(10)
    final_statement_status = statement_response.json()["output"]["status"]
    if final_statement_status == "error":
        exception = statement_response.json()["output"]["evalue"]
        logging.info("Statement exception: %s", exception)
        for trace in statement_response.json()["output"]["traceback"]:
            logging.info(trace)
        raise ValueError(f"Final Statement Status: {final_statement_status}")

    # Get the logs
    lines = requests.get(
        f"{session_url}/log", headers={"Content-Type": "application/json"}
    ).json()["log"]
    logging.info("Final Statement Status: %s", final_statement_status)
    return lines


def get_public_ip(cluster_id):
    """Get public IP of cluster

    Arguments:
        cluster_id {str} -- Cluster Id

    Returns:
        str -- Public IP address
    """
    instances = EMR.list_instances(ClusterId=cluster_id, InstanceGroupTypes=["MASTER"])
    return instances["Instances"][0]["PublicIpAddress"]
