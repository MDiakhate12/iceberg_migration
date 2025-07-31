// Define AWS provider
provider "aws" {
  region = "eu-west-1"

  default_tags {
    tags = {
      ExecEnvironment = terraform.workspace
      ManagedBy       = "Pile Terraform p-snowlake"
    }
  }
}

provider "vault" {}

provider "atn-utils" {}
