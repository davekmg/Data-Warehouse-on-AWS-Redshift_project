import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import configparser


def create_iam_role(key, secret, iam_role_name):
    # create IAM Client
    iam = boto3.client('iam', aws_access_key_id=key,
                     aws_secret_access_key=secret,
                     region_name='us-west-2'
                  )

    try:
        # create IAM Role
        print("Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=iam_role_name, # DWH_IAM_ROLE_NAME
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
        
        # Attach role with the policy AmazonS3ReadOnlyAccess
        print("Attaching Policy")
        iam.attach_role_policy(RoleName=iam_role_name,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

        print(" Get the IAM role ARN")
        roleArn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']

        print(roleArn)

        return roleArn
    except Exception as e:
        print(e)


def create_redshift_cluster(key, secret, cluster_type, node_type, num_nodes, 
                            db_name, db_user, db_password, cluster_identifier, roleArn):
    try:
        # create Redshift Client
        redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=key,
                        aws_secret_access_key=secret
                        )
    
        # create Redshift Cluster
        response = redshift.create_cluster(        
            #HW
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=int(num_nodes),

            #Identifiers & Credentials
            DBName=db_name,
            ClusterIdentifier=cluster_identifier,
            MasterUsername=db_user,
            MasterUserPassword=db_password,
            
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    key = config.get('AWS','KEY')
    secret = config.get('AWS','SECRET')
    iam_role_name = config.get('CLUSTER','IAM_ROLE_NAME')
    cluster_type = config.get('CLUSTER','CLUSTER_TYPE')
    node_type = config.get('CLUSTER','NODE_TYPE')
    num_nodes = config.get('CLUSTER','NUM_NODES')
    db_name = config.get('CLUSTER','DB_NAME')
    db_user = config.get('CLUSTER','DB_USER')
    db_password = config.get('CLUSTER','DB_PASSWORD')
    cluster_identifier = config.get('CLUSTER','CLUSTER_IDENTIFIER')

    roleArn = create_iam_role(key, secret, iam_role_name)

    kwargs = {
        "key": key, 
        "secret": secret, 
        "cluster_type": cluster_type, 
        "node_type": node_type, 
        "num_nodes": num_nodes, 
        "db_name": db_name, 
        "db_user": db_user, 
        "db_password": db_password, 
        "cluster_identifier": cluster_identifier,
        "roleArn": roleArn
    }
    create_redshift_cluster(**kwargs)


if __name__ == "__main__":
    main()