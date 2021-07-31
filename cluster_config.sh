#!/bin/bash

# Declare variables
PROJECT_PATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
BUCKET_NAME=project3-data-lake-bucket
BUCKET_PATH=s3://$BUCKET_NAME/
CLUSTER_NAME=data-lake-project
EMR_LABEL=emr-5.28.0
INSTANCE_COUNT=3
LOCAL_BOOTSTRAP_PATH=$PROJECT_PATH/bootstrap.sh
BOOTSTRAP_PATH=s3://$BUCKET_NAME/resources/bootstrap.sh
CONFIG_FNAME=configurations.json
LOCAL_CONFIG_PATH=$PROJECT_PATH/$CONFIG_FNAME
KEY_NAME="spark-cluster"
INSTANCE_TYPE=m5.xlarge
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
