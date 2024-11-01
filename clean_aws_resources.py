import boto3
from botocore.exceptions import ClientError

def delete_ec2_instances(instance_ids):
    ec2 = boto3.resource('ec2')
    try:
        print(f"Deleting EC2 instances: {instance_ids}")
        ec2.instances.filter(InstanceIds=instance_ids).terminate()
    except ClientError as e:
        print(f"Error deleting EC2 instances: {e}")

def delete_s3_buckets(bucket_names):
    s3 = boto3.resource('s3')
    
    for bucket_name in bucket_names:
        try:
            bucket = s3.Bucket(bucket_name)
            print(f"Deleting S3 bucket: {bucket_name}")
            
            # Supprimer tous les objets dans le bucket
            bucket.objects.all().delete()  # Supprime tous les objets

            # Supprimer toutes les versions des objets si le versionnage est activé
            versions = bucket.object_versions.all()
            for version in versions:
                print(f"Deleting version: {version.id} of {version.object_key}")
                version.delete()

            # Supprimer le bucket lui-même
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
        except ClientError as e:
            print(f"Error deleting RDS instance {instance_id}: {e}")

def delete_all_items(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    try:
        response = table.scan()
        items = response.get('Items', [])

        # Supprimer tous les éléments de la table
        for item in items:
            print(f"Deleting item: {item}")
            table.delete_item(
                Key={
                    'LockID': item['LockID']  # Remplacez par votre clé primaire
                }
            )

        # Continuer à supprimer les éléments si la table a plus d'éléments
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items = response.get('Items', [])
            for item in items:
                print(f"Deleting item: {item}")
                table.delete_item(
                    Key={
                        'LockID': item['LockID']  # Remplacez par votre clé primaire
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
        table.wait_until_not_exists()  # Attendre que la table soit supprimée
        print(f"Table {table_name} deleted successfully.")
    except ClientError as e:
        print(f"Error deleting table {table_name}: {e}")


def main():
    s3_bucket_names = ['spark-handson-tf-state-bucket']      # Remplacez par vos noms de buckets S3
    table_name = 'spark-handson-db-tf-state-lock'
    delete_s3_buckets(s3_bucket_names)
    delete_all_items(table_name)  # Supprime tous les éléments de la table
    delete_table(table_name)  # Supprime la table


if __name__ == '__main__':
    main()
