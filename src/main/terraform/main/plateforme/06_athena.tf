resource "aws_athena_workgroup" "snowlake_workgroup" {
  name        = "snowlake_${terraform.workspace}_workgroup"
  description = "Athena Workgroup for Snowlake queries in ${title(terraform.workspace)}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://sglk-snowlake-${terraform.workspace}-eu-west-1/athena-results/"
    }
  }

  state = "ENABLED"

  tags = {
    Environment = terraform.workspace
    Name        = "${local.name}_${terraform.workspace}_workgroup"
    Perimeter   = local.name
  }
}

resource "aws_athena_workgroup" "jman_workgroup" {
  count = terraform.workspace == "prod" ? 1 : 0

  name        = "jman_${terraform.workspace}_workgroup"
  description = "Athena Workgroup for Jman-group queries in ${title(terraform.workspace)}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://sglk-snowlake-${terraform.workspace}-eu-west-1/jman-results/"
    }
  }

  state = "ENABLED"

  tags = {
    Environment = terraform.workspace
    Name        = "jman_${terraform.workspace}_workgroup"
    Perimeter   = local.name
  }
}

resource "aws_athena_workgroup" "lakeformation_workgroup" {
  name        = "lakeformation_${terraform.workspace}_workgroup"
  description = "Athena Workgroup for Lakeformation users queries in ${title(terraform.workspace)}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://sglk-snowlake-${terraform.workspace}-eu-west-1/lakeformation-results/"
    }
  }

  state = "ENABLED"

  tags = {
    Environment = terraform.workspace
    Name        = "lakeformation_${terraform.workspace}_workgroup"
    Perimeter   = local.name
  }
}
