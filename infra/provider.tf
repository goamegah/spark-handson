terraform {
  // backend configuration for storing the state file
    backend "s3" {
        bucket = "spark-handson-tf-state-bucket"
        key    = "terraform/state"
        region = "eu-west-1"
        dynamodb_table = "spark-handson-db-tf-state-lock"
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

