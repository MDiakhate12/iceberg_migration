# ===========================================================
# DATABASES
# ===========================================================

module "snowlake_glue_database_bronze" {
  source = "./terraform_modules/aws-glue-catalog"

  glue_catalog_database_name         = "snowlake_bronze"
  glue_catalog_database_location_uri = "s3://${aws_s3_bucket.snowlake.bucket}/bronze"
}

module "snowlake_glue_database_silver" {
  source = "./terraform_modules/aws-glue-catalog"

  glue_catalog_database_name         = "snowlake_silver"
  glue_catalog_database_location_uri = "s3://${aws_s3_bucket.snowlake.bucket}/silver"
}

module "snowlake_glue_database_gold" {
  source = "./terraform_modules/aws-glue-catalog"

  glue_catalog_database_name         = "snowlake_gold"
  glue_catalog_database_location_uri = "s3://${aws_s3_bucket.snowlake.bucket}/gold"
}

module "snowlake_glue_database_external" {
  source = "./terraform_modules/aws-glue-catalog"

  glue_catalog_database_name         = "snowlake_external"
  glue_catalog_database_location_uri = "s3://${aws_s3_bucket.snowlake.bucket}/external"
}


module "snowlake_glue_database_salesforce" {
  source = "./terraform_modules/aws-glue-catalog"

  glue_catalog_database_name         = "snowlake_salesforce"
  glue_catalog_database_location_uri = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/salesforce"
}
