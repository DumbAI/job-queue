provider "aws" {
  region = "us-east-1"
}

variable "name_prefix" {
  type = string
  default = ""
}

resource "aws_dynamodb_table" "job_queue" {
  name             = "${var.name_prefix}-job-queue"
  billing_mode     = "PAY_PER_REQUEST"
  hash_key         = "JobId"

  attribute {
    name = "JobId"
    type = "S"
  }

  attribute {
    name = "CreatedAt"
    type = "S"
  }

  attribute {
    name = "Status"
    type = "S"
  }

  global_secondary_index {
    name               = "QueueIndex"
    hash_key           = "Status"
    range_key          = "CreatedAt"
    projection_type    = "ALL"
  }

  tags = {
    Application = "${var.name_prefix}-job-queue"
  }
}

resource "aws_s3_bucket" "job_data" {
  bucket = "${var.name_prefix}-job-data"

  tags = {
    Application = "${var.name_prefix}-job-queue"
  }
}