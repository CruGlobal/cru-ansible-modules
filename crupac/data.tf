data "aws_vpc" "main" {
  tags = { Name = "Main VPC" }
}

data "aws_acm_certificate" "star_aws_cru_org" {
  domain      = "*.aws.cru.org"
  most_recent = true
}
