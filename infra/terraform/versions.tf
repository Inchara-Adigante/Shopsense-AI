terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project    = var.project
      managed_by = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id   = data.aws_caller_identity.current.account_id
  name         = "${var.project}-${var.environment}"
  lake_bucket  = "${local.name}-lake-${local.account_id}"
  infra_bucket = "${local.name}-artifacts-${local.account_id}"
  lake_uri     = "s3://${local.lake_bucket}/lake"
}
