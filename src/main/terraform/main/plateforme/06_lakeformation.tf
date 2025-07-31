# Create the LakeFormation (only one for all envs)

resource "aws_lakeformation_resource" "snowlake" {
  count = terraform.workspace == "inte" ? 1 : 0

  arn                     = aws_s3_bucket.snowlake.arn
  use_service_linked_role = false
  role_arn                = data.aws_iam_role.lakeformation_role.arn
  hybrid_access_enabled   = true
}

resource "aws_lakeformation_data_lake_settings" "snowlake_settings" {
  count = terraform.workspace == "inte" ? 1 : 0

  admins = concat([for k, v in data.aws_iam_group.snowlake_dev.users[*].arn : v], [data.aws_iam_role.gitlab_runner_cd_eks.arn])

  create_database_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }

  create_table_default_permissions {
    permissions = ["ALL"]
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
}
