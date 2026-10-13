locals {
  tls_only_policies = {
    for name, bucket in {
      data  = aws_s3_bucket.data
      audit = aws_s3_bucket.audit
      } : name => jsonencode({
        Version = "2012-10-17"
        Statement = [
          {
            Sid       = "DenyInsecureTransport"
            Effect    = "Deny"
            Principal = "*"
            Action    = "s3:*"
            Resource  = [bucket.arn, "${bucket.arn}/*"]
            Condition = {
              Bool = {
                "aws:SecureTransport" = "false"
              }
            }
          },
        ]
    })
  }
}

resource "aws_s3_bucket_policy" "data" {
  bucket = aws_s3_bucket.data.id
  policy = local.tls_only_policies.data

  depends_on = [aws_s3_bucket_public_access_block.data]
}

resource "aws_s3_bucket_policy" "audit" {
  bucket = aws_s3_bucket.audit.id
  policy = local.tls_only_policies.audit

  depends_on = [aws_s3_bucket_public_access_block.audit]
}
