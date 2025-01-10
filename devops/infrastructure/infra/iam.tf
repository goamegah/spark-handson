data "aws_iam_policy_document" "glue_assume_role_policy_document" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_role_policy_document" {
  statement {
    actions = [
      "s3:*",
      "kms:*",
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "cloudwatch:PutMetricData"  # put metric on  cloudwatch
    ]
    effect = "Allow"
    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_policy" {
  name = "glue_policy"
  path = "/"
  policy = data.aws_iam_policy_document.glue_role_policy_document.json
}

resource "aws_iam_role" "glue-role" {
  name                = "glue_role"
  assume_role_policy  = data.aws_iam_policy_document.glue_assume_role_policy_document.json
  managed_policy_arns = [aws_iam_policy.glue_policy.arn]
}