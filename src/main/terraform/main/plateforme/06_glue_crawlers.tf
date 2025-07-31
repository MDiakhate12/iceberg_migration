module "snowlake_glue_crawler_s3_target_scream" {
  source = "./terraform_modules/aws-glue-crawler"

  perimeter                  = "snowlake"
  glue_crawler_name          = "snowlake_crawler_scream"
  glue_crawler_database_name = module.snowlake_glue_database_bronze.glue_catalog_database_name
  glue_crawler_role          = data.aws_iam_role.snowlake_crawlers.arn
  glue_crawler_configuration = jsonencode(
    {
      Version              = 1
      CreatePartitionIndex = false
      Grouping = {
        TableGroupingPolicy     = "CombineCompatibleSchemas"
        TableLevelConfiguration = 6
      }
    }
  )

  glue_crawler_s3_target = [
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/scream"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/scream/public.*", "**/scream/pricing.*", "**/scream/stats.*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/statistic"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/statistic/public.*", "**/statistic/odsp.*", "**/statistic/odst.*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/commande"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/commande/public.*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/api"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/api/public.*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/demat"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/demat/public.*"]
    }
  ]
}

module "snowlake_glue_crawler_s3_target_referentiel" {
  source = "./terraform_modules/aws-glue-crawler"

  perimeter                  = "snowlake"
  glue_crawler_name          = "snowlake_crawler_referentiel"
  glue_crawler_database_name = module.snowlake_glue_database_bronze.glue_catalog_database_name
  glue_crawler_role          = data.aws_iam_role.snowlake_crawlers.arn
  glue_crawler_configuration = jsonencode(
    {
      Version              = 1
      CreatePartitionIndex = false
      Grouping = {
        TableGroupingPolicy     = "CombineCompatibleSchemas"
        TableLevelConfiguration = 6
      }
    }
  )

  glue_crawler_s3_target = [
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/referentiel"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/referentiel/public.*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/referencement"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/referencement/public.*", "**/referencement/log.*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/litteralis"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/litteralis/public.*", "**/*_precalcul/*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/formulaire"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/formulaire/public.*"]
    }
  ]
}

module "snowlake_glue_crawler_s3_target_cbyd" {
  source = "./terraform_modules/aws-glue-crawler"

  perimeter                  = "snowlake"
  glue_crawler_name          = "snowlake_crawler_cbyd"
  glue_crawler_database_name = module.snowlake_glue_database_bronze.glue_catalog_database_name
  glue_crawler_role          = data.aws_iam_role.snowlake_crawlers.arn
  glue_crawler_configuration = jsonencode(
    {
      Version              = 1
      CreatePartitionIndex = false
      Grouping = {
        TableGroupingPolicy     = "CombineCompatibleSchemas"
        TableLevelConfiguration = 6
      }
    }
  )

  glue_crawler_s3_target = [
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/declaration"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/declaration/public.*", "**/declaration/log.*", "**/declaration/declaration_precalcul.*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/reponse"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/reponse/public.*", "**/reponse/log.*", "**/reponse/reponse_precalcul.*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/pmsr"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/pmsr/public.*"]
    },
    {
      path       = "s3://${aws_s3_bucket.snowlake.bucket}/bronze/scodify"
      exclusions = ["**/_SUCCESS", "**/*.json", "**/scodify/public.*", "**/scodify/log.*"]
    }
  ]
}

module "snowlake_glue_crawler_s3_target_archives" {
  source = "./terraform_modules/aws-glue-crawler"

  perimeter                  = "snowlake"
  glue_crawler_name          = "snowlake_crawler_archives"
  glue_crawler_database_name = module.snowlake_glue_database_bronze.glue_catalog_database_name
  glue_crawler_role          = data.aws_iam_role.snowlake_crawlers.arn

  glue_crawler_s3_target = [
    {
      path       = "s3://${aws_s3_bucket.archives.bucket}/archives/archives"
      exclusions = []
    }
  ]
}
