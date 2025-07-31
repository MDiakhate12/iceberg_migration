// Get region informations
data "aws_region" "current" {}

data "aws_availability_zones" "available" {}

data "aws_vpc" "current" {
  filter {
    name   = "tag:Environment"
    values = [terraform.workspace]
  }
}

// Subnet

data "aws_subnet" "data" {
  count = length(data.aws_availability_zones.available.names)

  vpc_id            = data.aws_vpc.current.id
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Environment = terraform.workspace
    Type        = "data"
  }
}

data "aws_subnet" "private" {
  count = length(data.aws_availability_zones.available.names)

  vpc_id            = data.aws_vpc.current.id
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Environment = terraform.workspace
    Type        = "private"
  }
}

// Group IAM

data "aws_iam_group" "snowlake_dev" {
  group_name = "DevSnowlake"
}

data "aws_iam_group" "snowlake_ecco" {
  group_name = "SnowlakeEccoAccess"
}

// Roles IAM
data "aws_iam_role" "gitlab_runner_cd_eks" {
  name = module.sglk_common.roles.gitlab_runner_cd_eks
}

data "aws_iam_role" "lakeformation_role" {
  name = local.lakeformation_role
}

data "aws_iam_role" "snowlake_crawlers" {
  name = module.sglk_common.roles_envs.glue.snowlake_crawlers
}

data "aws_iam_role" "snowlake_eventbridge_step_functions" {
  name = module.sglk_common.roles_envs.event_bridge.snowlake
}

data "aws_iam_role" "snowlake_glue_jobs" {
  name = module.sglk_common.roles_envs.glue.snowlake_jobs
}

data "aws_iam_role" "snowlake_lambda_clean_bronze" {
  name = module.sglk_common.roles_envs.lambda.snowlake_clean_bronze
}

data "aws_iam_role" "snowlake_lambda_clean_snapshots" {
  name = module.sglk_common.roles_envs.lambda.snowlake_clean_snapshots
}

data "aws_iam_role" "snowlake_lambda_dispatch_pbi" {
  name = module.sglk_common.roles_envs.lambda.snowlake_dispatch_pbi
}

# data "aws_iam_role" "snowlake_lambda_check_time" {
#   name = module.sglk_common.roles_envs.lambda.snowlake_check_time
# }

data "aws_iam_role" "snowlake_lambda_export_sync" {
  name = module.sglk_common.roles_envs.lambda.snowlake_export_sync
}

data "aws_iam_role" "snowlake_lambda_ingest_external" {
  name = module.sglk_common.roles_envs.lambda.snowlake_ingest_external
}

data "aws_iam_role" "snowlake_lambda_instance_identifier" {
  name = module.sglk_common.roles_envs.lambda.snowlake_instance_identifier
}

data "aws_iam_role" "snowlake_sfn_export_rds" {
  name = module.sglk_common.roles_envs.sfn.snowlake
}

data "aws_iam_role" "snowlake_sfn_export_snapshot_rds" {
  name = module.sglk_common.roles_envs.sfn.snowlake_export
}

# TODO A changer : Revoir les policies de ce rôle (p-comptes) et le renommer (m-sglk-common)
data "aws_iam_role" "snowlake_sfn_run_workflow" {
  name = module.sglk_common.roles_envs.sfn.snowlake
}

data "aws_iam_role" "snowlake_sfn_supervisor" {
  name = module.sglk_common.roles_envs.sfn.snowlake_supervisor
}

data "aws_iam_role" "snowlake_dms_archives" {
  name = "RoleSnowlakeDmsArchives${title(terraform.workspace)}"
}

// KMS Key

data "aws_kms_key" "snowlake_kms_key" {
  key_id = "alias/snowlake_kms_key"
}

data "aws_kms_key" "go_connect_kms_key" {
  key_id = "alias/${terraform.workspace}_goconnect_kms_key"
}

// Buckets

data "aws_s3_bucket" "lambda" {
  bucket = "sglk-lambda"
}

// ECR

data "aws_ecr_repository" "lambda_clean_bronze" {
  name = "sogelink/snowlake-lambda-clean-bronze"
}

data "aws_ecr_image" "lambda_clean_bronze" {
  repository_name = data.aws_ecr_repository.lambda_clean_bronze.name
  image_tag       = var.project_version
}

data "aws_ecr_repository" "lambda_clean_snapshots" {
  name = "sogelink/snowlake-lambda-clean-snapshots"
}

data "aws_ecr_image" "lambda_clean_snapshots" {
  repository_name = data.aws_ecr_repository.lambda_clean_snapshots.name
  image_tag       = var.project_version
}

data "aws_ecr_repository" "lambda_ingest_external" {
  name = "sogelink/snowlake-lambda-ingest-external"
}

data "aws_ecr_image" "lambda_ingest_external" {
  repository_name = data.aws_ecr_repository.lambda_ingest_external.name
  image_tag       = var.project_version
}

data "aws_ecr_repository" "lambda_dispatch_pbi" {
  name = "sogelink/snowlake-lambda-dispatch-pbi"
}

data "aws_ecr_image" "lambda_dispatch_pbi" {
  repository_name = data.aws_ecr_repository.lambda_dispatch_pbi.name
  image_tag       = var.project_version
}

data "aws_ecr_repository" "lambda_check_time" {
  name = "sogelink/snowlake-lambda-check-time"
}

data "aws_ecr_image" "lambda_check_time" {
  repository_name = data.aws_ecr_repository.lambda_check_time.name
  image_tag       = var.project_version
}
// Security Group

data "aws_security_group" "allow_everwin_sqlserveur" {
  vpc_id = data.aws_vpc.current.id

  tags = {
    Name        = "${terraform.workspace}_allow_everwin_sqlserveur"
    Environment = terraform.workspace
  }
}

data "aws_security_group" "allow_internet" {
  vpc_id = data.aws_vpc.current.id

  tags = {
    Name        = "${terraform.workspace}_allow_internet"
    Environment = terraform.workspace
  }
}

data "aws_security_group" "sg_archives_allow_glue" {
  vpc_id = data.aws_vpc.current.id

  tags = {
    Name        = "${terraform.workspace}_sg_archives_allow_glue"
    Environment = terraform.workspace
  }
}

data "aws_security_group" "grafana_services" {
  vpc_id = data.aws_vpc.current.id

  tags = {
    Name        = "${terraform.workspace}_grafana_services"
    Environment = terraform.workspace
  }
}

// Vault

data "vault_kv_secret_v2" "archives" {
  mount = "kv/archives"
  name  = "main/bdd"
}

data "vault_kv_secret_v2" "gx" {
  mount = "kv/snowlake"
  name  = "main/gx"
}

data "vault_kv_secret_v2" "salesforce" {
  mount = "kv/snowlake"
  name  = "main/salesforce"
}

data "vault_kv_secret_v2" "sharepoint" {
  mount = "kv/snowlake"
  name  = "main/sharepoint"
}

data "vault_kv_secret_v2" "powerbi" {
  mount = "kv/snowlake"
  name  = "main/powerbi"
}

// Glue jar libs from Nexus

data "atn-utils_nexus_package" "jar_libs" {
  for_each = local.jar_libs

  repository_url = each.value.url
  output_path    = "/tmp/${each.value.filename}"
}

//Route53
data "aws_route53_zone" "internal_vpc" {
  name   = "${terraform.workspace}."
  vpc_id = data.aws_vpc.current.id
}
