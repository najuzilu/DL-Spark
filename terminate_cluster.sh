#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
chmod +x $SCRIPT_DIR/cluster_config.sh
source $SCRIPT_DIR/cluster_config.sh

# Get Cluster-Id
echo "Getting EMR cluster ID..."
CLUSTER_ID=$(aws emr list-clusters --active --query "Clusters[0].Id" --output text)
echo -e "EMR cluster ID is ${CLUSTER_ID}\n"

# terminate cluster
echo "Terminating EMR cluster..."
aws emr terminate-clusters --cluster-id $CLUSTER_ID
echo "EMR cluster terminating!"

# Delete S3 buckets
echo -e "Cleaning s3 buckets...\n"
clean_s3_bucket $BUCKET_PATH $BUCKET_NAME

echo "***************** DONE *****************"
