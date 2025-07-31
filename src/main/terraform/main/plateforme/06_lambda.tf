# Ressources

resource "aws_s3_object" "lambda_export_sync" {
  bucket = data.aws_s3_bucket.lambda.bucket
  key    = "${terraform.workspace}/snowlake_lambda_export_sync.zip"
  source = local.lambda-export-sync
  etag   = filemd5(local.lambda-export-sync)
}

resource "aws_s3_object" "lambda_instance_identifier" {
  bucket = data.aws_s3_bucket.lambda.bucket
  key    = "${terraform.workspace}/snowlake_lambda_instance_identifier.zip"
  source = local.lambda-instance-identifier
  etag   = filemd5(local.lambda-instance-identifier)
}

# Modules

module "snowlake_lambda_clean_bronze" {
  source = "./terraform_modules/aws-lambda"

  environnement = terraform.workspace
  perimeter     = "snowlake"

  architecture     = "x86_64"
  ecr_image_uri    = "${data.aws_ecr_repository.lambda_clean_bronze.repository_url}@${data.aws_ecr_image.lambda_clean_bronze.id}"
  source_code_hash = data.aws_ecr_image.lambda_clean_bronze.id
  function_name    = "snowlake_lambda_clean_bronze"
  handler          = null
  runtime          = null

  iam_role_arn = data.aws_iam_role.snowlake_lambda_clean_bronze.arn
  timeout      = 900
  memory_size  = 1024

  variables = {
    env = terraform.workspace
  }
}

module "snowlake_lambda_clean_snapshots" {
  source = "./terraform_modules/aws-lambda"

  environnement = terraform.workspace
  perimeter     = "snowlake"

  architecture     = "x86_64"
  ecr_image_uri    = "${data.aws_ecr_repository.lambda_clean_snapshots.repository_url}@${data.aws_ecr_image.lambda_clean_snapshots.id}"
  source_code_hash = data.aws_ecr_image.lambda_clean_snapshots.id
  function_name    = "snowlake_lambda_clean_snapshots"
  handler          = null
  runtime          = null

  iam_role_arn = data.aws_iam_role.snowlake_lambda_clean_snapshots.arn
  timeout      = 900
  memory_size  = 1024

  variables = {
    env = terraform.workspace
  }
}

module "snowlake_lambda_export_sync" {
  source = "./terraform_modules/aws-lambda"

  environnement = terraform.workspace
  perimeter     = "snowlake"

  s3_bucket_id     = data.aws_s3_bucket.lambda.id
  s3_object_key    = aws_s3_object.lambda_export_sync.key
  source_code_hash = filebase64sha256(local.lambda-export-sync)

  function_name = "snowlake_lambda_export_sync"
  handler       = "handler.main"
  runtime       = "python3.12"

  iam_role_arn = data.aws_iam_role.snowlake_lambda_export_sync.arn
  timeout      = 900

  variables = {
    environment = terraform.workspace
  }
}

module "lambda_snowlake_lambda_ingest_external" {
  source = "./terraform_modules/aws-lambda"

  environnement = terraform.workspace
  perimeter     = "snowlake"

  architecture     = "x86_64"
  ecr_image_uri    = "${data.aws_ecr_repository.lambda_ingest_external.repository_url}@${data.aws_ecr_image.lambda_ingest_external.id}"
  source_code_hash = data.aws_ecr_image.lambda_ingest_external.id
  function_name    = "snowlake_lambda_ingest_external"
  handler          = null
  runtime          = null

  iam_role_arn = data.aws_iam_role.snowlake_lambda_ingest_external.arn
  timeout      = 900
  memory_size  = 1024

  variables = {
    env        = terraform.workspace
    kms_key_id = data.aws_kms_key.snowlake_kms_key.key_id
    sp_user    = data.vault_kv_secret_v2.sharepoint.data.username
    sp_pass    = data.vault_kv_secret_v2.sharepoint.data.password
  }
}

module "snowlake_lambda_instance_identifier" {
  source = "./terraform_modules/aws-lambda"

  environnement = terraform.workspace
  perimeter     = "snowlake"

  s3_bucket_id     = data.aws_s3_bucket.lambda.id
  s3_object_key    = aws_s3_object.lambda_instance_identifier.key
  source_code_hash = filebase64sha256(local.lambda-instance-identifier)

  function_name = "snowlake_lambda_instance_identifier"
  handler       = "handler.main"
  runtime       = "python3.12"

  iam_role_arn = data.aws_iam_role.snowlake_lambda_instance_identifier.arn
  timeout      = 60

  variables = {
    environment = terraform.workspace
  }
}

module "lambda_snowlake_lambda_dispatch_pbi" {
  source = "./terraform_modules/aws-lambda"

  environnement = terraform.workspace
  perimeter     = "snowlake"

  architecture     = "x86_64"
  ecr_image_uri    = "${data.aws_ecr_repository.lambda_dispatch_pbi.repository_url}@${data.aws_ecr_image.lambda_dispatch_pbi.id}"
  source_code_hash = data.aws_ecr_image.lambda_dispatch_pbi.id
  function_name    = "snowlake_lambda_dispatch_pbi"
  handler          = null
  runtime          = null

  iam_role_arn = data.aws_iam_role.snowlake_lambda_dispatch_pbi.arn
  timeout      = 900
  memory_size  = 1024

  vpc_config = {
    subnet_ids = data.aws_subnet.private[*].id

    security_group_ids = [
      data.aws_security_group.allow_internet.id
    ]
  }

  variables = {
    env               = terraform.workspace
    pbi_client_id     = data.vault_kv_secret_v2.powerbi.data.client_id
    pbi_client_secret = data.vault_kv_secret_v2.powerbi.data.client_secret
    pbi_tenant_id     = data.vault_kv_secret_v2.powerbi.data.tenant_id
  }
}

# IAM Role pour la fonction Lambda
module "lambda_snowlake_lambda_check_time" {
  source = "./terraform_modules/aws-lambda"

  environnement = terraform.workspace
  perimeter     = "snowlake"

  architecture     = "x86_64"
  ecr_image_uri    = "${data.aws_ecr_repository.lambda_check_time.repository_url}@${data.aws_ecr_image.lambda_check_time.id}"
  source_code_hash = data.aws_ecr_image.lambda_check_time.id
  function_name    = "snowlake_lambda_check_time"
  handler          = null
  runtime          = null

  iam_role_arn = aws_iam_role.snowlake_lambda_dispatch_pbi.arn
  timeout      = 900
  memory_size  = 1024
}
