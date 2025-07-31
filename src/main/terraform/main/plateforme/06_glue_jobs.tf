# ===========================================================
# UTILS JOBS
# ===========================================================

## Common
resource "aws_s3_object" "glue_common_library" {
  bucket = aws_s3_bucket.snowlake.bucket
  key    = "glue_job_scripts/common-glue.zip"
  source = local.glue-common
  etag   = filemd5(local.glue-common)
}

## Additional Python resources
resource "aws_s3_object" "glue_external_python_resources" {
  for_each = fileset(local.glue-external-python, "**")

  bucket = aws_s3_bucket.snowlake.bucket
  key    = "glue_job_scripts/additional-python/${each.value}"
  source = "${local.glue-external-python}/${each.value}"
  etag   = filemd5("${local.glue-external-python}/${each.value}")
}

## Additional JAR libs
resource "aws_s3_object" "glue_external_jar_resources" {
  for_each = local.jar_libs

  bucket = aws_s3_bucket.snowlake.bucket
  key    = "jars/${each.value.filename}"

  source = "/tmp/${each.value.filename}"

  depends_on = [
    data.atn-utils_nexus_package.jar_libs
  ]
}

## Technique

module "tech_blank" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "tech_blank"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/tech_blank.py"
    script_source_file = local.job-tech-blank
  }

  perimeter = "snowlake"

  # default arguments
  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}


module "catalog" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "catalog"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/catalog.py"
    script_source_file = local.job-catalog
  }

  perimeter = "snowlake"

  # default arguments
  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

## Bronze

### ems_geodesial

module "bronze_ems_geodesial" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_ems_geodesial"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_ems_geodesial.py"
    script_source_file = local.job-bronze-ems-geodesial
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### flydoc_ondemand

module "bronze_flydoc_ondemand" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_flydoc_ondemand"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_flydoc_ondemand.py"
    script_source_file = local.job-bronze-flydoc-ondemand
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### lucca_leave

module "bronze_lucca_leave" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_lucca_leave"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_lucca_leave.py"
    script_source_file = local.job-bronze-lucca-leave
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_city

module "bronze_matomo_city" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_city"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_city.py"
    script_source_file = local.job-bronze-matomo-city
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_country

module "bronze_matomo_country" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_country"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_country.py"
    script_source_file = local.job-bronze-matomo-country
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_forms

module "bronze_matomo_forms" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_forms"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_forms.py"
    script_source_file = local.job-bronze-matomo-forms
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_forms_pages

module "bronze_matomo_forms_pages" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_forms_pages"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_forms_pages.py"
    script_source_file = local.job-bronze-matomo-forms-pages
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_funnels

module "bronze_matomo_funnels" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_funnels"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_funnels.py"
    script_source_file = local.job-bronze-matomo-funnels
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_funnels_flow

module "bronze_matomo_funnels_flow" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_funnels_flow"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_funnels_flow.py"
    script_source_file = local.job-bronze-matomo-funnels-flow
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_funnels_metrics

module "bronze_matomo_funnels_metrics" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_funnels_metrics"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_funnels_metrics.py"
    script_source_file = local.job-bronze-matomo-funnels-metrics
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_pages

module "bronze_matomo_pages" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_pages"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_pages.py"
    script_source_file = local.job-bronze-matomo-pages
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_region

module "bronze_matomo_region" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_region"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_region.py"
    script_source_file = local.job-bronze-matomo-region
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_visitfrequency

module "bronze_matomo_visitfrequency" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_visitfrequency"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_visitfrequency.py"
    script_source_file = local.job-bronze-matomo-visitfrequency
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_visitorinterest_count

module "bronze_matomo_visitorinterest_count" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_visitorinterest_count"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_visitorinterest_count.py"
    script_source_file = local.job-bronze-matomo-visitorinterest-count
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_visitorinterest_days

module "bronze_matomo_visitorinterest_days" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_visitorinterest_days"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_visitorinterest_days.py"
    script_source_file = local.job-bronze-matomo-visitorinterest-days
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_visitorinterest_duration

module "bronze_matomo_visitorinterest_duration" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_visitorinterest_duration"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_visitorinterest_duration.py"
    script_source_file = local.job-bronze-matomo-visitorinterest-duration
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_visitorinterest_page

module "bronze_matomo_visitorinterest_page" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_visitorinterest_page"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_visitorinterest_page.py"
    script_source_file = local.job-bronze-matomo-visitorinterest-page
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### matomo_visittime

module "bronze_matomo_visittime" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_matomo_visittime"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_matomo_visittime.py"
    script_source_file = local.job-bronze-matomo-visittime
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### netsuite_account

module "bronze_netsuite_account" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_account"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "account"
  }
}

### netsuite_accountingperiod
module "bronze_netsuite_accountingperiod" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_accountingperiod"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "accountingperiod"
  }
}

### netsuite_charge

module "bronze_netsuite_charge" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_charge"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "charge"
  }
}

### netsuite_consolidatedexchangerate

module "bronze_netsuite_consolidatedexchangerate" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_consolidatedexchangerate"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout = 150
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "consolidatedexchangerate"
  }
}

### netsuite_customer

module "bronze_netsuite_customer" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_customer"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "customer"
  }
}

### netsuite_employee

module "bronze_netsuite_employee" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_employee"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout = 150
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "employee"
  }
}

### netsuite_entity

module "bronze_netsuite_entity" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_entity"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout = 150
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "entity"
  }
}

### netsuite_item

module "bronze_netsuite_item" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_item"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "item"
  }
}

### netsuite_PUK

module "bronze_netsuite_puk" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_puk"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "puk"
  }
}

### netsuite_revenueelement

module "bronze_netsuite_revenueelement" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_revenueelement"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "revenueelement"
  }
}

### netsuite_revenueplan

module "bronze_netsuite_revenueplan" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_revenueplan"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "revenueplan"
  }
}

### netsuite_revenueplanplannedrevenue

module "bronze_netsuite_revenueplanplannedrevenue" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_revenueplanplannedrevenue"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "revenueplanplannedrevenue"
  }
}

### netsuite_subsidiary

module "bronze_netsuite_subsidiary" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_subsidiary"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout = 150
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "subsidiary"
  }
}

### netsuite_transaction

module "bronze_netsuite_transaction" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_transaction"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "transaction"
  }
}

### netsuite_transactionline

module "bronze_netsuite_transactionline" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_transactionline"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config["prod"].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "transactionline"
  }
}

### netsuite_subscription

module "bronze_netsuite_subscription" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_subscription"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "subscription"
  }
}

### netsuite_subscriptionline

module "bronze_netsuite_subscriptionline" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_netsuite_subscriptionline"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_netsuite.py"
    script_source_file = local.job-bronze-netsuite
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 150
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--datalake-formats"          = "iceberg"
    "--additional-python-modules" = "cryptography"
    "--table"                     = "subscriptionline"
  }
}

### bronze_ref_auth_compte

module "bronze_ref_auth_compte" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_ref_auth_compte"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_ref_auth_compte.py"
    script_source_file = local.job-bronze-ref-auth-compte
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### bronze_ref_emetteur

module "bronze_ref_emetteur" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_ref_emetteur"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_ref_emetteur.py"
    script_source_file = local.job-bronze-ref-emetteur
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### bronze_scodify_project

module "bronze_scodify_project" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_scodify_project"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_scodify_project.py"
    script_source_file = local.job-bronze-scodify-project
  }

  perimeter = "snowlake"

  # default arguments
  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### bronze_superoffice

module "bronze_superoffice" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_superoffice"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_superoffice.py"
    script_source_file = local.job-bronze-superoffice
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
  extra_job_args = {
    "--spark.sql.files.maxPartitionBytes" = 266338304 # 254 MB (default 134217728 (128 MB))
    "--spark.driver.maxResultSize"        = "10g"
    "--datalake-formats"                  = "iceberg"
  }
}

### bronze_visma_customerinvoice

module "bronze_visma_customerinvoice" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_visma_customerinvoice"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_visma_customerinvoice.py"
    script_source_file = local.job-bronze-visma-customerinvoice
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
  extra_job_args = {
    "--spark.sql.files.maxPartitionBytes" = 266338304 # 254 MB (default 134217728 (128 MB))
    "--spark.driver.maxResultSize"        = "10g"
    "--datalake-formats"                  = "iceberg"
  }
}

### bronze_visma_employee

module "bronze_visma_employee" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_visma_employee"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_visma_employee.py"
    script_source_file = local.job-bronze-visma-employee
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### bronze_visma_general_ledger_transaction

module "bronze_visma_general_ledger_transaction" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_visma_general_ledger_transaction"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_visma_general_ledger_transaction.py"
    script_source_file = local.job-bronze-visma-glt
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
  extra_job_args = {
    "--datalake-formats" = "iceberg"
  }
}

### bronze_visma_inventory

module "bronze_visma_inventory" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_visma_inventory"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_visma_inventory.py"
    script_source_file = local.job-bronze-visma-inventory
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
  extra_job_args = {
    "--spark.sql.files.maxPartitionBytes" = 266338304 # 254 MB (default 134217728 (128 MB))
    "--spark.driver.maxResultSize"        = "10g"
    "--datalake-formats"                  = "iceberg"
  }
}

### bronze_visma_salesorder

module "bronze_visma_salesorder" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_visma_salesorder"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_visma_salesorder.py"
    script_source_file = local.job-bronze-visma-salesorder
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
  extra_job_args = {
    "--spark.sql.files.maxPartitionBytes" = 266338304 # 254 MB (default 134217728 (128 MB))
    "--spark.driver.maxResultSize"        = "10g"
    "--datalake-formats"                  = "iceberg"
  }
}

### bronze_visma_subscription

module "bronze_visma_subscription" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_visma_subscription"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_visma_subscription.py"
    script_source_file = local.job-bronze-visma-subscription
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace
}

### bronze_jira_issue

module "bronze_jira_issue" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_jira_issue"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_jira_issue.py"
    script_source_file = local.job-bronze-jira-issue
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 480
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--spark.sql.files.maxPartitionBytes" = 266338304 # 254 MB (default 134217728 (128 MB))
    "--spark.driver.maxResultSize"        = "10g"
    "--datalake-formats"                  = "iceberg"
  }
}

### bronze_jira_project

module "bronze_jira_project" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_jira_project"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_jira_project.py"
    script_source_file = local.job-bronze-jira-project
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  extra_job_args = {
    "--datalake-formats" = "iceberg"
  }
}

### bronze_jira_worklog

module "bronze_jira_worklog" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_jira_worklog"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_jira_worklog.py"
    script_source_file = local.job-bronze-jira-worklog
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 480
  worker_profile = local.profile_config["prod"].hot
  extra_job_args = {
    "--spark.sql.files.maxPartitionBytes" = 266338304 # 254 MB (default 134217728 (128 MB))
    "--spark.driver.maxResultSize"        = "10g"
    "--datalake-formats"                  = "iceberg"
  }
}

### bronze_jira_worklog_deleted

module "bronze_jira_worklog_deleted" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_jira_worklog_deleted"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_jira_worklog_deleted.py"
    script_source_file = local.job-bronze-jira-worklog-deleted
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 480
  worker_profile = local.profile_config["prod"].hot
  extra_job_args = {
    "--spark.sql.files.maxPartitionBytes" = 266338304 # 254 MB (default 134217728 (128 MB))
    "--spark.driver.maxResultSize"        = "10g"
    "--datalake-formats"                  = "iceberg"
  }
}

### bronze_jira_team_member

module "bronze_jira_team_member" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_jira_team_member"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_jira_team_member.py"
    script_source_file = local.job-bronze-jira-team-member
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 480
  worker_profile = local.profile_config[terraform.workspace].hot
  extra_job_args = {
    "--spark.sql.files.maxPartitionBytes" = 266338304 # 254 MB (default 134217728 (128 MB))
    "--spark.driver.maxResultSize"        = "10g"
    "--datalake-formats"                  = "iceberg"
  }
}


module "backup_table" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "backup_table"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/backup_table.py"
    script_source_file = local.job-backup-table
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout        = 480
  worker_profile = local.profile_config[terraform.workspace].hot

  extra_job_args = {
    "--datalake-formats" = "iceberg"
  }
}


### ems_entitlementusage

module "bronze_ems_entitlementusage" {
  source   = "./terraform_modules/aws-glue-job"
  name     = "bronze_ems_entitlementusage"
  role_arn = data.aws_iam_role.snowlake_glue_jobs.arn

  script_location = {
    script_bucket_name = aws_s3_bucket.snowlake.bucket
    script_object_key  = "glue_job_scripts/bronze_ems_entitlementusage.py"
    script_source_file = local.job-bronze-ems-entitlementusage
  }

  perimeter = "snowlake"

  extra_py_files      = "s3://${aws_s3_bucket.snowlake.bucket}/${aws_s3_object.glue_common_library.key}"
  job_bookmark_option = "job-bookmark-enable"
  job_environment     = terraform.workspace

  timeout = 480
  worker_profile = {
    worker_type       = "G.4X",
    number_of_workers = 2
  }
  extra_job_args = {
    "--datalake-formats" = "iceberg",

    "--spark.sql.shuffle.partitions"      = "500",
    "--spark.sql.files.maxPartitionBytes" = "256m",
    "--spark.sql.files.maxPartitionNum"   = "250",

    "--spark.executor.memory" = "64g",
    "--spark.driver.memory"   = "64g",
    "--spark.memory.fraction" = "0.8",

    "--spark.dynamicAllocation.enabled"                 = "true",
    "--spark.dynamicAllocation.minExecutors"            = "2",
    "--spark.dynamicAllocation.initialExecutors"        = "2",
    "--spark.dynamicAllocation.shuffleTracking.enabled" = "true",

    "--spark.sql.adaptive.enabled"                    = "true",
    "--spark.sql.adaptive.join.enabled"               = "true",
    "--spark.sql.adaptive.skewJoin.enabled"           = "true",
    "--spark.sql.adaptive.coalescePartitions.enabled" = "true",
    "--spark.sql.adaptive.localShuffleReader.enabled" = "true",

    "--spark.sql.adaptive.advisoryPartitionSizeInBytes"        = "256m",
    "--spark.sql.adaptive.coalescePartitions.minPartitionSize" = "128m"
  }

}
