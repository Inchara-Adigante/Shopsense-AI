# ---------------------------------------------------------------- Glue job role
data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Least privilege: read/write the lake, read the artifacts bucket - nothing else.
data "aws_iam_policy_document" "glue_s3" {
  statement {
    sid       = "LakeReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/lake/*"]
  }
  statement {
    sid       = "LakeList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid       = "ArtifactsRead"
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
  }
}

resource "aws_iam_role_policy" "glue_s3" {
  name   = "lake-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_s3.json
}

# ---------------------------------------------------------------- Redshift role (COPY from the curated zone)
data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com", "redshift-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift" {
  count              = var.enable_redshift ? 1 : 0
  name               = "${local.name}-redshift-s3-read"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
}

data "aws_iam_policy_document" "redshift_s3" {
  statement {
    actions   = ["s3:GetObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/lake/curated/*"]
  }
}

resource "aws_iam_role_policy" "redshift_s3" {
  count  = var.enable_redshift ? 1 : 0
  name   = "curated-read"
  role   = aws_iam_role.redshift[0].id
  policy = data.aws_iam_policy_document.redshift_s3.json
}
