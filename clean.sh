#!/bin/bash

# Function to delete EC2 instances
delete_ec2_instances() {
    instance_ids=("$@")
    echo "Deleting EC2 instances: ${instance_ids[*]}"
    aws ec2 terminate-instances --instance-ids "${instance_ids[@]}"
    echo "Successfully initiated deletion of EC2 instances: ${instance_ids[*]}"
}

# Function to delete S3 buckets
delete_s3_buckets() {
    bucket_names=("$@")
    for bucket_name in "${bucket_names[@]}"; do
        echo "Deleting S3 bucket: $bucket_name"
        aws s3 rb "s3://$bucket_name" --force
        echo "Deleted S3 bucket: $bucket_name"
    done
}

# Function to delete DynamoDB table items
delete_all_items() {
    table_name=$1
    echo "Deleting all items from table: $table_name"
    aws dynamodb scan --table-name "$table_name" --attributes-to-get "LockID" --query "Items[*].LockID.S" --output text | \
    while read -r lock_id; do
        echo "Deleting item with LockID: $lock_id"
        aws dynamodb delete-item --table-name "$table_name" --key "{\"LockID\": {\"S\": \"$lock_id\"}}"
    done
}

# Function to delete DynamoDB table
delete_table() {
    table_name=$1
    echo "Deleting table: $table_name"
    aws dynamodb delete-table --table-name "$table_name"
    aws dynamodb wait table-not-exists --table-name "$table_name"
    echo "Table $table_name deleted successfully."
}

# Function to delete Glue jobs
delete_glue_jobs() {
    job_names=("$@")
    for job_name in "${job_names[@]}"; do
        echo "Deleting Glue job: $job_name"
        aws glue delete-job --job-name "$job_name"
        echo "Deleted Glue job: $job_name"
    done
}

# Function to detach IAM policies from roles
detach_iam_policies() {
    role_name=$1
    policy_arns=("${@:2}")
    for policy_arn in "${policy_arns[@]}"; do
        echo "Detaching policy $policy_arn from role $role_name"
        aws iam detach-role-policy --role-name "$role_name" --policy-arn "$policy_arn"
        echo "Detached policy $policy_arn from role $role_name"
    done
}

# Function to delete IAM roles
delete_iam_roles() {
    role_names=("$@")
    for role_name in "${role_names[@]}"; do
        echo "Deleting IAM role: $role_name"
        aws iam delete-role --role-name "$role_name"
        echo "Deleted IAM role: $role_name"
    done
}

# Function to delete IAM policies
delete_iam_policies() {
    policy_arns=("$@")
    for policy_arn in "${policy_arns[@]}"; do
        echo "Deleting IAM policy: $policy_arn"
        aws iam delete-policy --policy-arn "$policy_arn"
        echo "Deleted IAM policy: $policy_arn"
    done
}

# Main script
s3_bucket_names=(
    'spark-handson-tf-state-bucket'
    'goamegah-spark-handson-bucket'
)  # Replace with your S3 bucket names
table_name='spark-handson-db-tf-state-lock'
glue_job_names=(
    'exo2_clean_job'
)  # Replace with your Glue job names
iam_role_names=(
    'glue_role'
)  # Replace with your IAM role names
iam_policy_arns=(
    'arn:aws:iam::<accound_id>:policy/glue_policy'
)  # Replace with your IAM policy ARNs

# Uncomment the following line if you want to delete EC2 instances
# delete_ec2_instances 'your-instance-id1' 'your-instance-id2'

delete_s3_buckets "${s3_bucket_names[@]}"
delete_all_items "$table_name"  # Delete all items in the table
delete_table "$table_name"  # Delete the table
delete_glue_jobs "${glue_job_names[@]}"  # Delete Glue jobs
detach_iam_policies "${iam_role_names[0]}" "${iam_policy_arns[@]}"  # Detach IAM policies from roles
delete_iam_roles "${iam_role_names[@]}"  # Delete IAM roles
delete_iam_policies "${iam_policy_arns[@]}"  # Delete IAM policies