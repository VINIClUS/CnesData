"""Defaults AWS computados compartilhados pelas suites nativas."""

MOCK_AWS_PROVIDERS = '''mock_provider "aws" {
  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:us-east-1:123456789012:certificate/11111111-1111-4111-8111-111111111111"
    }
  }
  mock_resource "aws_cloudfront_function" {
    defaults = {
      arn = "arn:aws:cloudfront::123456789012:function/fixture-spa"
    }
  }
  mock_resource "aws_cognito_user_pool" {
    defaults = {
      endpoint = "cognito-idp.us-east-2.amazonaws.com/us-east-2_fixture"
    }
  }
  mock_resource "aws_s3_bucket" {
    defaults = {
      bucket_regional_domain_name = "fixture-data.s3.us-east-2.amazonaws.com"
    }
  }
  mock_data "aws_region" {
    defaults = {
      region = "us-east-2"
    }
  }
}

mock_provider "aws" {
  alias = "global_edge"
  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:us-east-1:123456789012:certificate/11111111-1111-4111-8111-111111111111"
    }
  }
  mock_resource "aws_cloudfront_function" {
    defaults = {
      arn = "arn:aws:cloudfront::123456789012:function/fixture-spa"
    }
  }
  mock_resource "aws_cognito_user_pool" {
    defaults = {
      endpoint = "cognito-idp.us-east-2.amazonaws.com/us-east-2_fixture"
    }
  }
  mock_resource "aws_s3_bucket" {
    defaults = {
      bucket_regional_domain_name = "fixture-data.s3.us-east-2.amazonaws.com"
    }
  }
  mock_data "aws_region" {
    defaults = {
      region = "us-east-1"
    }
  }
}
'''
