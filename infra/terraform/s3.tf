# ---------------------------------------------------------------- data lake bucket
resource "aws_s3_bucket" "lake" {
  bucket = local.lake_bucket
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled" # protects raw deliveries from accidental overwrite or delete
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "raw-to-infrequent-access"
    status = "Enabled"
    filter {
      prefix = "lake/raw/"
    }
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 180
      storage_class = "GLACIER_IR" # raw stays replayable for backfills, at a fraction of the cost
    }
  }

  rule {
    id     = "expire-staging"
    status = "Enabled"
    filter {
      prefix = "lake/_staging/"
    }
    expiration {
      days = 1 # write-audit-publish leftovers from failed runs
    }
  }

  rule {
    id     = "expire-old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ---------------------------------------------------------------- artifacts: Glue scripts, wheel, Athena results
resource "aws_s3_bucket" "artifacts" {
  bucket = local.infra_bucket
}

resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "glue/shopsense_job.py"
  source = "${path.module}/../glue/shopsense_job.py"
  etag   = filemd5("${path.module}/../glue/shopsense_job.py")
}

resource "aws_s3_object" "wheel" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "wheels/${basename(var.wheel_path)}"
  source = var.wheel_path
  etag   = filemd5(var.wheel_path)
}
