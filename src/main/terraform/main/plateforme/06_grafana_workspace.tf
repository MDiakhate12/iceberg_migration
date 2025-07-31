# https://aws.amazon.com/fr/blogs/big-data/part-2-enhance-monitoring-and-debugging-for-aws-glue-jobs-using-new-job-observability-metrics/
# resource "aws_grafana_workspace" "grafana_workspace" {
#   name                     = "sglk-snowlake-${terraform.workspace}-${data.aws_region.current.name}-grafana-workspace"
#   account_access_type      = "CURRENT_ACCOUNT"
#   authentication_providers = ["AWS_SSO"]
#   permission_type          = "SERVICE_MANAGED"

#   data_sources = ["CLOUDWATCH"]

#   vpc_configuration {
#     security_group_ids = [
#       data.aws_security_group.allow_internet.id,
#       data.aws_security_group.grafana_services.id
#     ]

#     subnet_ids = [for subnet in data.aws_subnet.private : subnet.id]
#   }

#   tags = {
#     Name = "sglk-snowlake-${terraform.workspace}-${data.aws_region.current.name}-grafana-workspace"
#   }
# }
