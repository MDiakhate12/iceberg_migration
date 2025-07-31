module "kv_snowlake" {
  source = "./terraform_modules/vault-secret-mount"

  type        = "kv-v2"
  name        = "snowlake"
  description = "Key-Value store du périmètre snowlake"

  team = "dev-snow"
}

resource "aws_secretsmanager_secret" "salesforce_api" {
  for_each = { for k, v in toset([module.sglk_common.inte_env, module.sglk_common.prod_env]) : k => v if k == terraform.workspace }
  name     = "salesforce-${each.value}"
}

resource "aws_secretsmanager_secret_version" "salesforce_api_secret_version" {
  for_each = { for k, v in toset([module.sglk_common.inte_env, module.sglk_common.prod_env]) : k => v if k == terraform.workspace }

  secret_id     = aws_secretsmanager_secret.salesforce_api[each.value].id
  secret_string = jsonencode(local.salesforce)
}
