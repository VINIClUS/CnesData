terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

locals {
  gateway_services = toset([for service, enabled in var.gateway_endpoints : service if enabled])
}

data "aws_region" "current" {}

# The S3 gateway endpoint requires both DNS attributes when tasks use the Amazon DNS servers.
resource "aws_vpc" "this" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.name_prefix}-vpc"
  }
}

# Adopting the default group strips its allow-from-self ingress and allow-all egress: the VPC keeps
# no ingress rule at all, and a task launched without an explicit group reaches nothing. Both lists
# are optional and computed, so only these explicit empty values make plan and apply remove a rule
# added outside OpenTofu.
resource "aws_default_security_group" "this" {
  vpc_id  = aws_vpc.this.id
  ingress = []
  egress  = []

  tags = {
    Name = "${var.name_prefix}-default-closed"
  }
}

# This gateway is the tasks' only route to ECR, CloudWatch Logs, Step Functions and STS. Either VPC
# Block Public Access mode cuts that public IPv4 path, so the network needs BPA off in this Region
# or an exclusion for this VPC.
resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-igw"
  }
}

# Tasks take an ephemeral public IPv4 from AssignPublicIp=ENABLED in the state machine, never from
# the subnet. A NAT gateway's fixed hourly charge alone exceeds the product budget; with zero
# ingress and no listener the address only carries connections the task itself opened.
resource "aws_subnet" "public" {
  for_each = var.public_subnets

  vpc_id                  = aws_vpc.this.id
  availability_zone_id    = each.key
  cidr_block              = each.value
  map_public_ip_on_launch = false

  tags = {
    Name = "${var.name_prefix}-public-${each.key}"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id

  tags = {
    Name = "${var.name_prefix}-public"
  }
}

resource "aws_route" "internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.this.id
}

resource "aws_route_table_association" "public" {
  for_each = aws_subnet.public

  subnet_id      = each.value.id
  route_table_id = aws_route_table.public.id
}

resource "aws_vpc_endpoint" "gateway" {
  for_each = local.gateway_services

  vpc_id            = aws_vpc.this.id
  service_name      = "com.amazonaws.${data.aws_region.current.region}.${each.key}"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.public.id]

  tags = {
    Name = "${var.name_prefix}-${each.key}"
  }
}

# The explicit empty ingress makes plan and apply remove an inbound rule added outside OpenTofu.
# Egress lives in the rule resources below, which an inline list would fight, so an egress rule
# added outside OpenTofu survives here: the refreshed-plan policy of PROD-INFRA-011 must pin it.
# No rule opens DNS: security groups never filter the Amazon-provided resolver (VPC+2).
resource "aws_security_group" "tasks" {
  name        = "${var.name_prefix}-tasks"
  description = "Zero-ingress group of the CnesData Fargate tasks"
  vpc_id      = aws_vpc.this.id
  ingress     = []

  tags = {
    Name = "${var.name_prefix}-tasks"
  }
}

resource "aws_vpc_security_group_egress_rule" "gateway_tls" {
  for_each = aws_vpc_endpoint.gateway

  security_group_id = aws_security_group.tasks.id
  description       = "TLS to ${each.key} through its gateway endpoint prefix list"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  prefix_list_id    = each.value.prefix_list_id
}

# ECR (API and registry), CloudWatch Logs, Step Functions and STS publish no AWS-managed prefix
# list, and an interface endpoint per service adds a fixed hourly charge per AZ above the product
# budget, so their TLS reaches the public regional endpoints through the internet gateway.
#trivy:ignore:AVD-AWS-0104:exp:2027-04-11
resource "aws_vpc_security_group_egress_rule" "aws_api_tls" {
  security_group_id = aws_security_group.tasks.id
  description       = "TLS to the ECR, CloudWatch Logs, Step Functions and STS public endpoints"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}
