resource "aws_redshift_subnet_group" "eu-west-1" {
  count = terraform.workspace != "staging" ? 1 : 0

  name       = "${terraform.workspace}-subnet"
  subnet_ids = data.aws_subnet.data[*].id
}

// Domain route interne - RECORD A
resource "aws_route53_record" "redshift_regional" {
  count = terraform.workspace == "prod" ? 1 : 0

  zone_id = data.aws_route53_zone.internal_vpc.zone_id
  name    = "redshift.${module.sglk_common.main_region}"
  type    = "CNAME"

  ttl = 20

  records = [
    replace(aws_redshift_cluster.standalone[0].endpoint, ":5439", ""),
  ]
}


//IAM ROLE
resource "aws_iam_role" "redshift" {
  count = terraform.workspace != "staging" ? 1 : 0

  name = "${terraform.workspace}-role-redshift"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          "Service" : [
            "s3.amazonaws.com",
            "redshift.amazonaws.com",
            "iam.amazonaws.com",
            "redshift-serverless.amazonaws.com"
          ]
        }
      },
    ]
  })
}

resource "aws_iam_policy" "RedshiftFullAccessForJman" {
  count = terraform.workspace != "staging" ? 1 : 0

  policy = templatefile("${path.module}/templates/policies/AmazonRedshiftServiceLinkedRole.json", {
    env_name = terraform.workspace
  })
}

resource "aws_iam_role_policy_attachment" "RedShiftFullAccessToJmanCluster" {
  count = terraform.workspace != "staging" ? 1 : 0

  role       = aws_iam_role.redshift[0].name
  policy_arn = aws_iam_policy.RedshiftFullAccessForJman[0].arn
}

resource "aws_redshift_cluster" "standalone" {
  count = terraform.workspace != "staging" ? 1 : 0
  //Infos cluster/node
  cluster_identifier = "${terraform.workspace}-redshift-jman"
  database_name      = "${terraform.workspace}_jman"
  node_type          = "ra3.large"
  cluster_type       = "single-node"

  //credentials
  master_username = "redshift"
  master_password = "$4s98rgbJnsfHm36"

  //Networking-Security
  multi_az = false
  vpc_security_group_ids = [
    data.aws_security_group.allow_internet.id,
    module.sg_redshift[0].security_group_id
  ]
  cluster_subnet_group_name = aws_redshift_subnet_group.eu-west-1[0].name
  port                      = 5439 //default
  publicly_accessible       = false
  iam_roles = [
    aws_iam_role.redshift[0].arn
  ]
  tags = {
    Environment = terraform.workspace
    Name        = "${terraform.workspace}-redshift-jman"
    Perimeter   = local.name
  }
}

/* Security group */
/* --------------- */

module "sg_redshift" {
  source = "./terraform_modules/aws-security-group"
  count  = terraform.workspace != "staging" ? 1 : 0

  creation = true
  id       = data.aws_vpc.current.id
  name     = "redshift_sg"
}

module "sg_redshift_ingress_app" {
  source = "./terraform_modules/aws-security-group"
  count  = terraform.workspace != "staging" ? 1 : 0

  creation = false
  id       = module.sg_redshift[0].security_group_id
  port     = 5439
  type     = "ingress"

  cidrs = module.sglk_common.vpn_ips
}
