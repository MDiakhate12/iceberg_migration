# CONNECTIONS

resource "random_integer" "random_id" {
  min = 0
  max = 2
  keepers = {
    index = 1
  }
}

// SG Connection GX
module "sg_glue_connection_gx" {
  source = "./terraform_modules/aws-security-group"

  creation = true
  id       = data.aws_vpc.current.id
  name     = "glue_connection_gx"
}

module "sg_glue_connection_gx_ingress_all" {
  source = "./terraform_modules/aws-security-group"

  creation = false
  id       = module.sg_glue_connection_gx.security_group_id
  type     = "ingress"
  protocol = "all"

  source_security_group_ids = [
    module.sg_glue_connection_gx.security_group_id
  ]
}

// Glue connection GX
resource "aws_glue_connection" "connection_gx" {
  name = "${terraform.workspace}_snowlake_gx"

  connection_type = "JDBC"
  connection_properties = {
    # JDBC_CONNECTION_URL = "jdbc:sqlserver://mssql-compta-1.main.forge:1433;databaseName=ATLOG-PROD"
    JDBC_CONNECTION_URL = "jdbc:sqlserver://mssql-compta-1.main.forge:1433"
    USERNAME            = data.vault_kv_secret_v2.gx.data.username
    PASSWORD            = data.vault_kv_secret_v2.gx.data.password
  }

  physical_connection_requirements {
    availability_zone = data.aws_subnet.data[random_integer.random_id.result].availability_zone
    subnet_id         = data.aws_subnet.data[random_integer.random_id.result].id
    security_group_id_list = [
      module.sg_glue_connection_gx.security_group_id,
      data.aws_security_group.allow_everwin_sqlserveur.id,
      data.aws_security_group.allow_internet.id
    ]
  }
}
