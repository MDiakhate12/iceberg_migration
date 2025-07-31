# Liste des emails des développeurs à notifier
variable "developer_emails" {
  description = "Liste des emails des développeurs à notifier"
  type        = list(string)
  default     = ["dev-snow@sogelink.com"]
}

# Liste des emails des développeurs à notifier
variable "perimeter" {
  description = "Perimètre Snowlake pour la Facturation"
  type        = string
  default     = "snowlake"
}

# SNS Topic pour les notifications
resource "aws_sns_topic" "glue_job_failures" {
  name = "${terraform.workspace}-snowlake-glue-job-failures"

  tags = {
    Name        = "${terraform.workspace}-snowlake-glue-job-failures"
    Environment = terraform.workspace
    Perimeter   = var.perimeter
  }
}

# Abonnements email au topic SNS
resource "aws_sns_topic_subscription" "developer_subscriptions" {
  count     = length(var.developer_emails)
  topic_arn = aws_sns_topic.glue_job_failures.arn
  protocol  = "email"
  endpoint  = var.developer_emails[count.index]
}

# IAM Role pour la fonction Lambda
resource "aws_iam_role" "lambda_role" {
  name = "${terraform.workspace}-snowlake-glue-notification-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Politique IAM attachée au rôle Lambda
resource "aws_iam_policy" "lambda_policy" {
  name        = "${terraform.workspace}-snowlake-glue-notification-lambda-policy"
  description = "Politique permettant à Lambda d'accéder à Glue, CloudWatch Logs et SNS"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetJobRun",
          "glue:GetJob"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.glue_job_failures.arn
      }
    ]
  })
}

# Attachement de la politique au rôle
resource "aws_iam_role_policy_attachment" "lambda_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# Code source Lambda archivé dans un fichier ZIP
data "archive_file" "lambda_zip" {
  type        = "zip"
  output_path = "${local.lambda-send-notification}/lambda_function.zip"
  source_file = "${local.lambda-send-notification}/src/lambda_function.py"
}

# Fonction Lambda
resource "aws_lambda_function" "glue_notification_lambda" {
  function_name    = "${terraform.workspace}-snowlake-glue-notification-lambda"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.9"
  timeout          = 60
  memory_size      = 128

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.glue_job_failures.arn
    }
  }

  tags = {
    Name        = "${terraform.workspace}-snowlake-glue-notification-lambda"
    Environment = terraform.workspace
    Perimeter   = var.perimeter
  }
}

# CloudWatch Event Rule pour détecter les changements d'état des jobs Glue
resource "aws_cloudwatch_event_rule" "glue_job_state_change" {
  name        = "${terraform.workspace}-snowlake-glue-job-state-change"
  description = "Capture les changements d'état des jobs Glue"

  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      state   = ["FAILED", "TIMEOUT", "ERROR", "STOPPED"]
      jobName = [{ "prefix" : "${terraform.workspace}" }]
    }
  })
}

# Permission permettant à CloudWatch d'invoquer la fonction Lambda
resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.glue_notification_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.glue_job_state_change.arn
}

# Association de la règle CloudWatch à la fonction Lambda
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.glue_job_state_change.name
  target_id = "InvokeLambda"
  arn       = aws_lambda_function.glue_notification_lambda.arn
}
