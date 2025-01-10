resource "aws_s3_bucket" "bucket" {
  bucket = "goamegah-spark-handson-bucket"

  tags = local.tags
}