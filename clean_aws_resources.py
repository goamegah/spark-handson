import boto3
from botocore.exceptions import ClientError

def delete_ec2_instances(instance_ids):
    ec2 = boto3.resource('ec2')
    try:
        print(f"Deleting EC2 instances: {instance_ids}")
        ec2.instances.filter(InstanceIds=instance_ids).terminate()
        print(f"Successfully initiated deletion of EC2 instances: {instance_ids}")
    except ClientError as e:
        print(f"Error deleting EC2 instances: {e}")

def delete_s3_buckets(bucket_names):
    s3 = boto3.resource('s3')
    
    for bucket_name in bucket_names:
        try:
            bucket = s3.Bucket(bucket_name)
            print(f"Deleting S3 bucket: {bucket_name}")
            
            # Delete all objects in the bucket
            bucket.objects.all().delete()

            # Delete all versions of objects if versioning is enabled
            versions = bucket.object_versions.all()
            for version in versions:
                print(f"Deleting version: {version.id} of {version.object_key}")
                version.delete()

            # Delete the bucket itself
            bucket.delete()
            print(f"Deleted S3 bucket: {bucket_name}")

        except ClientError as e:
            print(f"Error deleting S3 bucket {bucket_name}: {e}")

def delete_rds_instances(instance_ids):
    rds = boto3.client('rds')
    for instance_id in instance_ids:
        try:
            print(f"Deleting RDS instance: {instance_id}")
            rds.delete_db_instance(
                DBInstanceIdentifier=instance_id,
                SkipFinalSnapshot=True  # Use with caution
            )
            print(f"Successfully initiated deletion of RDS instance: {instance_id}")
        except ClientError as e:
            print(f"Error deleting RDS instance {instance_id}: {e}")

def delete_all_items(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.scan()
        items = response.get('Items', [])

        # Delete all items in the table
        for item in items:
            print(f"Deleting item: {item}")
            table.delete_item(
                Key={
                    'LockID': item['LockID']  # Replace with your primary key
                }
            )

        # Continue deleting items if the table has more items
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items = response.get('Items', [])
            for item in items:
                print(f"Deleting item: {item}")
                table.delete_item(
                    Key={
                        'LockID': item['LockID']  # Replace with your primary key
                    }
                )

    except ClientError as e:
        print(f"Error deleting items from table {table_name}: {e}")

def delete_table(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        print(f"Deleting table: {table_name}")
        table.delete()
        table.wait_until_not_exists()  # Wait for the table to be deleted
        print(f"Table {table_name} deleted successfully.")
    except ClientError as e:
        print(f"Error deleting table {table_name}: {e}")

def main():
    s3_bucket_names = [
        'spark-handson-tf-state-bucket',
        'goamegah-spark-handson-bucket'
    ]  # Replace with your S3 bucket names
    table_name = 'spark-handson-db-tf-state-lock'
    
    # Uncomment the following line if you want to delete EC2 instances
    # delete_ec2_instances(['your-instance-id1', 'your-instance-id2'])  
    
    delete_s3_buckets(s3_bucket_names)
    delete_all_items(table_name)  # Delete all items in the table
    delete_table(table_name)  # Delete the table

if __name__ == '__main__':
    main()
