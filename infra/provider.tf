terraform {
  // backend configuration for storing the state file
    backend "s3" {
        bucket = "spark-handson-tf-state-bucket "
        key    = "tf-infra/terraform.tfstate"
        region = "us-east-1"
        dynamodb_table = "spark_handson_db_tf_state_lock"
        encrypt = true
    }
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

