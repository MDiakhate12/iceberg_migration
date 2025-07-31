
# Archives

resource "aws_kms_key" "archives_kms_key" {
  description              = "KMS key to use for Archives bucket"
  key_usage                = "ENCRYPT_DECRYPT"
  customer_master_key_spec = "SYMMETRIC_DEFAULT"
  is_enabled               = true
}

resource "aws_kms_alias" "archives_kms_key_alias" {
  target_key_id = aws_kms_key.archives_kms_key.id
  name          = "alias/${terraform.workspace}/archives_kms_key"
}
