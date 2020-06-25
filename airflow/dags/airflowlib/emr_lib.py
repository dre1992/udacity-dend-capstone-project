import configparser
import logging

import boto3, json, pprint, requests, textwrap, time, requests
from datetime import datetime

from airflow.contrib.hooks.aws_hook import AwsHook


def client(region_name, aws_credentials_id):
    """
    Create the connection to emr
    @param region_name: region to be used
    @param aws_credentials_id: credentials to be used
    """
    global emr
    aws_hook = AwsHook(aws_credentials_id)
    credentials = aws_hook.get_credentials()
    emr = boto3.client('emr', region_name="us-west-2", aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key)


def get_security_group_id(group_name, region_name):
    """
    Get the security group by group name and region
    @param group_name: the group name of the security group
    @param region_name: the region
    @return: the group id
    """
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    ec2 = boto3.client('ec2', region_name=region_name, aws_access_key_id=credentials.access_key,
                       aws_secret_access_key=credentials.secret_key)
    response = ec2.describe_security_groups(GroupNames=[group_name])
    return response['SecurityGroups'][0]['GroupId']


def create_cluster(region_name, cluster_name='Airflow-' + str(datetime.now()), release_label='emr-5.9.0',
                   master_instance_type='m4.large', num_core_nodes=2, core_node_instance_type='m4.large'):
    """
    Creates the emr cluster
    @param region_name: region to create the cluster
    @param cluster_name: name of the cluster
    @param release_label: label
    @param master_instance_type: instance type of the master node
    @param num_core_nodes: number of worker nodes
    @param core_node_instance_type: instance type of worker nodes
    @return: the cluster id
    """
    emr_master_security_group_id = get_security_group_id('ElasticMapReduce-master', region_name=region_name)
    emr_slave_security_group_id = get_security_group_id('ElasticMapReduce-slave', region_name=region_name)
    cluster_response = emr.run_job_flow(
        Name=cluster_name,
        ReleaseLabel=release_label,
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': core_node_instance_type,
                    'InstanceCount': num_core_nodes
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName': 'spark-udacity',
            'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
            'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id
        },
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Applications=[
            {'Name': 'hadoop'},
            {'Name': 'spark'},
            {'Name': 'livy'}
        ]
    )
    return cluster_response['JobFlowId']


def get_cluster_dns(cluster_id):
    """
    Returns the dns resolution of the cluster
    @param cluster_id: the id of the cluster
    @return: the public dns name
    """
    response = emr.describe_cluster(ClusterId=cluster_id)
    return response['Cluster']['MasterPublicDnsName']


def wait_for_cluster_creation(cluster_id):
    """
    Wait for the cluster to finish creating
    @param cluster_id: the id of the cluster to wait
    """
    emr.get_waiter('cluster_running').wait(ClusterId=cluster_id)


def terminate_cluster(cluster_id):
    """
    Kills the cluster
    @param cluster_id: the id of the cluster to kill
    """
    emr.terminate_job_flows(JobFlowIds=[cluster_id])


# Creates an interactive scala spark session.
# Python(kind=pyspark), R(kind=sparkr) and SQL(kind=sql) spark sessions can also be created by changing the value of kind.
def create_spark_batch(master_dns, file_path, py_files_path=[], files=[], args=[]):
    """
    Submits a batch to livy server
    @param master_dns: the dns of livy
    @param file_path: the location of the runnable
    @param py_files_path: location of additional py files
    @param files: other files to include
    @param args: arguments to be passed
    @return: the json response of livy
    """
    # 8998 is the port on which the Livy server runs
    host = 'http://' + master_dns + ':8998'
    data = {'file': file_path}
    if py_files_path :
        data["pyFiles"] = py_files_path
    if files:
        data["files"] = files
    if args:
        data["args"] = args
    logging.info(data)
    headers = {'Content-Type': 'application/json'}
    response = requests.post(host + '/batches', data=json.dumps(data), headers=headers)
    logging.info(response.reason)
    return response


# Function to help track the progress of the scala code submitted to Apache Livy
def track_statement_progress(master_dns, response_id):
    """
    Tracks the statement progress and returns when successful of failed
    @param master_dns: the dns of livy
    @param response_id: the response of the creation statement
    """
    statement_status = 'x'
    host = 'http://' + master_dns + ':8998'
    session_url = host + '/batches/' + str(response_id)
    # Poll the status of the submitted scala code
    while statement_status not in 'success, error, dead ':
        # If a statement takes longer than a few milliseconds to execute, Livy returns early and provides a statement URL that can be polled until it is complete:
        statement_response = requests.get(session_url, headers={'Content-Type': 'application/json'})
        statement_status = statement_response.json()['state']
        logging.info('Batch status: ' + statement_status)
        # logging the logs
        lines = requests.get(session_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
            logging.info(line)

        if 'progress' in statement_response.json():
            logging.info('Progress: ' + str(statement_response.json()['progress']))
        time.sleep(10)
    if statement_status in 'error, dead':
        logging.info('Batch exception: ' + statement_response.json()['output']['evalue'])
        for trace in statement_response.json()['output']['traceback']:
            logging.info(trace)
        raise ValueError('Final Batch Status: ' + statement_status)
    logging.info('Final Batch Status: ' + statement_status)


def get_public_ip(cluster_id):
    """
    Gets the public ip of a given cluster
    @param cluster_id: the cluster id
    @return: the public ip address
    """
    instances = emr.list_instances(ClusterId=cluster_id, InstanceGroupTypes=['MASTER'])
    return instances['Instances'][0]['PublicIpAddress']
