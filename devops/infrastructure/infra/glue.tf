resource "aws_glue_job" "exo2_glue_job" {
  name     = "exo2_clean_job"
  role_arn = aws_iam_role.glue-role.arn

  command {
    script_location = "s3://${aws_s3_bucket.bucket.bucket}/spark-jobs/exo2_glue_job.py"
  }

  glue_version = "4.0"
  number_of_workers = 2
  worker_type = "Standard"


  default_arguments = {
    "--additional-python-modules"       = "s3://${aws_s3_bucket.bucket.bucket}/wheel/spark_handson-0.1.0-py3-none-any.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--PARAM_1"                         = "VALUE_1"
    "--PARAM_2"                         = "VALUE_2"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights" = "true"
    "--enable-metrics" = "true"
    "--enable-spark-ui " = "true"
  }

  tags = local.tags
}