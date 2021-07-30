#!/bin/bash

# Retrieve bucket name
# ./$PROJECT_PATH/dl.cfg
BUCKET_NAME=project3-data-lake-bucket

# Declare variables
PROJECT_PATH=~/Documents/DataEngineering/data_lakes/project/files
BUCKET_PATH=s3://${BUCKET_NAME}/
CLUSTER_NAME=data-lake-project
EMR_LABEL=emr-5.28.0
INSTANCE_COUNT=3
LOCAL_BOOTSTRAP_PATH=$PROJECT_PATH/bootstrap.sh
BOOTSTRAP_PATH=s3://${BUCKET_NAME}/resources/bootstrap.sh
KEY_NAME="spark-cluster"
INSTANCE_TYPE=m5.xlarge
JSON_CONFIG=$PROJECT_PATH/configurations.json
ETL_PY_PATH=$PROJECT_PATH/etl.py

clean_s3_bucket() {
    # Check to see if S3 exists
    if aws s3 ls $BUCKET_PATH 2>&1 | grep -q 'NoSuchBucket' -s
    then
        echo "S3 bucket doesn't exist."
    else
        # Empty S3 Bucket
        echo "Emptying any existing buckets in S3 with name $2"
        aws s3 rm $1 --recursive

        echo -e "Deleting any existing buckets in S3 with name $2\n"
        # Delete S3 Bucket
        aws s3api delete-bucket --bucket $2
    fi

}

# Clean S3 buckets if they exist
clean_s3_bucket $BUCKET_PATH $BUCKET_NAME

# Create a new S3 bucket for our project
echo "Creating a new S3 bucket named ${BUCKET_NAME}..."
aws s3api create-bucket --bucket $BUCKET_NAME --region us-east-2 --create-bucket-configuration LocationConstraint=us-east-2
echo -e "S3 bucket named ${BUCKET_NAME} created successfully\n"


# Copy bootstrap.sh to S3
echo "Copying boostrap from local to S3..."
aws s3 cp $LOCAL_BOOTSTRAP_PATH $BOOTSTRAP_PATH
echo -e "Boostrap copied successfully\n"

# Get a SubnetId
SUBNET_ID=$(aws ec2 describe-subnets --query "Subnets[0].SubnetId")

# Create an EMR cluster
echo "Creating EMR cluster..."
aws emr create-cluster --name $CLUSTER_NAME --use-default-roles --release-label $EMR_LABEL --instance-count $INSTANCE_COUNT --applications Name=Spark --bootstrap-actions Path=$BOOTSTRAP_PATH --configurations file://$JSON_CONFIG --ec2-attributes "KeyName=${KEY_NAME},SubnetId=${SUBNET_ID}" --instance-type $INSTANCE_TYPE --profile default

# Get Cluster-Id
CLUSTER_ID=$(aws emr list-clusters --active --query "Clusters[0].Id" --output text)
echo -e "EMR cluster ID is ${CLUSTER_ID}\n"

# Check if EMR cluster created
eval "EMR_STATUS=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.Status.State')"
echo "EMR cluster status is ${EMR_STATUS}..."

while [ "${EMR_STATUS}" != "WAITING" ]; do

    if [[ "${EMR_STATUS}" == "TERMINATED_WITH_ERRORS" ]]
    then
        echo "ERROR: EMR Cluster TERMINATED WITH ERRORS!"
        exit
    fi

    eval "EMR_STATUS=$(aws emr describe-cluster --cluster-id ${CLUSTER_ID} --query 'Cluster.Status.State')";

done

echo -e "EMR cluster status is ${EMR_STATUS}\n"

sleep 15

# Get instance IDs for the running cluster
echo "Getting instance ID..."
INSTANCE_ID=$(aws emr list-instances --cluster-id $CLUSTER_ID --instance-group-types MASTER --query "Instances[0].Ec2InstanceId" --output text)
echo -e "Instance ID is ${INSTANCE_ID}\n"

# Get security group for one of the EC2 instances
echo "Retrieving security group..."
eval "SG=$(aws ec2 describe-instance-attribute --instance-id ${INSTANCE_ID} --attribute groupSet --query 'Groups[*].GroupId | [0]')"
echo -e "Security group is ${SG}\n"

# Get security group cidr with port 22
echo "Getting security group CIDR with port 22..."
eval "OLD_CIDR=$(aws ec2 describe-security-groups --filters Name=ip-permission.from-port,Values=22 Name=ip-permission.to-port,Values=22 Name=ip-permission.protocol,Values=tcp --group-ids $SG --query 'SecurityGroups[*].IpPermissions[?FromPort==`22`] | [0][0].IpRanges[*].CidrIp | [0]')"

# Delete security group with old cidr
echo "Deleting inbound rule with old CIDR..."
aws ec2 revoke-security-group-ingress --group-id $SG --protocol tcp --port 22 --cidr $OLD_CIDR

# Retrieve current IP address
IP=$(curl -s http://whatismyip.akamai.com/)

# Authorize inbound security rule from current IP address
echo "Authorizing inbound security rule from current IP address..."
aws ec2 authorize-security-group-ingress --group-id $SG --protocol tcp --port 22 --cidr $IP/32
echo -e "\n"

# Get master public DNS name
MASTER_DNS=$(aws emr describe-cluster --cluster-id $CLUSTER_ID --query 'Cluster.MasterPublicDnsName' --output text)

# Copy etl.py to EMR master node
echo "Copying etl.py to EMR master node..."
scp -i ~/${KEY_NAME}.pem $ETL_PY_PATH hadoop@$MASTER_DNS:/home/hadoop/
echo -e "\n"

# Copy dl.cfg to EMR master node
echo "Copying dl.cfg to EMR master node..."
scp -i ~/${KEY_NAME}.pem ~/Documents/DataEngineering/data_lakes/project/files/dl.cfg hadoop@$MASTER_DNS:/home/hadoop/
echo -e "\n"

# Final step
echo "******** setup_cluster.sh ran successfully. ********"
# Connect to master node through SSH
echo -e "Run the following on your terminal:\n"
echo "aws emr ssh --cluster-id ${CLUSTER_ID} --key-pair-file ~/${KEY_NAME}.pem"
