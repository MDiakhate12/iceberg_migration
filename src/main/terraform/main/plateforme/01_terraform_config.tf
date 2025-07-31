// Terraform configuration
terraform {
  required_version = ">= 1.6.5, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.99.1"
    }

    random = {
      source  = "hashicorp/random"
      version = "3.3.1"
    }

    vault = {
      source  = "hashicorp/vault"
      version = "4.2.0"
    }

    atn-utils = {
      source = "allence-tunisie/atn-utils"
    }
  }

  backend "s3" {
    bucket         = "sglk-terraform"
    key            = "main/stacks/snowlake"
    dynamodb_table = "forge-terraform-lock"
    region         = "eu-west-1"
  }
}
