terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = "eu-west-1"
}

# Create an S3 bucket to store the Terraform state file
resource "aws_s3_bucket" "spark_handson_tf_state_bucket_ref" {
  bucket        = "spark-handson-tf-state-bucket" # REPLACE WITH YOUR BUCKET NAME
  force_destroy = true
}


# Enable versioning on the S3 bucket
resource "aws_s3_bucket_versioning" "spark_handson_tf_state_bucket_version_ref" {
  bucket = aws_s3_bucket.spark_handson_tf_state_bucket_ref.id # REPLACE WITH YOUR BUCKET REFERENCE NAME/ID

  versioning_configuration {
    status = "Enabled"
  }
}

# Enable server-side encryption on the S3 bucket
resource "aws_s3_bucket_server_side_encryption_configuration" "spark_handson_ss_enc_conf_ref" {
  bucket        = aws_s3_bucket.spark_handson_tf_state_bucket_ref.bucket # REPLACE WITH YOUR BUCKET BUCKET
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Create a DynamoDB table to store the Terraform state lock file
resource "aws_dynamodb_table" "spark_handson_db_tf_state_lock_ref" {
  name         = "spark_handson_db_tf_state_lock"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"
  attribute {
    name = "LockID"
    type = "S"
  }
}