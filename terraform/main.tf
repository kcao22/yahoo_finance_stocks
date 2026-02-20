terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.95.0"
    }
  }
}

provider "aws" {
  region = var.region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  # # Local stack configuration
  # skip_requesting_account_id = true
  # skip_credentials_validation = true
  # skip_metadata_api_check     = true
  # endpoints {
  #   s3                      = "http://localstack:4566"
  #   iam                     = "http://localstack:4566"
  #   redshiftserverless      = "http://localstack:4566"
  # }
}

# S3 Ingress bucket
resource "aws_s3_bucket" "prod_s3_ingress_bucket" {
  bucket        = var.ingress_bucket_name
  force_destroy = true
  tags = {
    Name = "Prod S3 Ingress"
  }
}

# S3 Archive bucket
resource "aws_s3_bucket" "prod_s3_archive_bucket" {
  bucket        = var.archive_bucket_name
  force_destroy = true
  tags = {
    Name = "Prod S3 Archive"
  }
}

# Redshift serverless role for attaching policies
resource "aws_iam_role" "prod_redshift_serverless_role" {
  name = "prod_redshift_serverless_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = [
            "redshift.amazonaws.com",
            "redshift-serverless.amazonaws.com",
            "s3.amazonaws.com",
            "iam.amazonaws.com"
          ]
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}


# Policy for Redshift serverless access to S3

resource "aws_iam_policy" "prod_s3_redshift_serverless_rw_policy" {
  name        = "prod_s3_redshift_serverless_rw_policy"
  description = "Prod Redshift serverless read and write permissions for S3."
  policy      = <<-EOF
  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "ReadObjects",
        "Effect": "Allow",
        "Action": [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ],
        "Resource": [
          "arn:aws:s3:::kc-prod-data-warehouse-archive/*",
          "arn:aws:s3:::kc-prod-data-warehouse-ingress/*"
        ]
      },
      {
        "Sid": "ListBuckets",
        "Effect": "Allow",
        "Action": "s3:ListBucket",
        "Resource": [
          "arn:aws:s3:::kc-prod-data-warehouse-archive",
          "arn:aws:s3:::kc-prod-data-warehouse-ingress"
        ]
      },
      {
        "Sid": "RedshiftAndSupportPermissions",
        "Effect": "Allow",
        "Action": [
          "redshift:*",
          "redshift-data:*",
          "redshift-serverless:*",
          "sqlworkbench:*",
          "sts:AssumeRole",
          "secretsmanager:GetSecretValue",
          "cloudwatch:*",
          "tag:GetResources",
          "ec2:Describe*",
          "sns:Publish"
        ],
        "Resource": "*"
      }
    ]
  }
  EOF
}


# Attach S3 access policy to prod_s3_redshift_serverless_role
resource "aws_iam_role_policy_attachment" "prod_s3_redshift_serverless_policy_attachment" {
  role       = aws_iam_role.prod_redshift_serverless_role.name
  policy_arn = aws_iam_policy.prod_s3_redshift_serverless_rw_policy.arn
}

# Attach Redshift full access policy to prod_redshift_serverless_role
resource "aws_iam_role_policy_attachment" "prod_redshift_full_access_policy_attachment" {
  role       = aws_iam_role.prod_redshift_serverless_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess"
}


# Prod Redshift cluster
resource "aws_vpc" "prod_redshift_serverless_vpc" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_subnet" "prod_redshift_subnet_a" {
  vpc_id                  = aws_vpc.prod_redshift_serverless_vpc.id
  cidr_block              = "10.0.4.0/24"
  availability_zone       = "us-west-2a"
  map_public_ip_on_launch = true
}

resource "aws_subnet" "prod_redshift_subnet_b" {
  vpc_id                  = aws_vpc.prod_redshift_serverless_vpc.id
  cidr_block              = "10.0.2.0/24"
  availability_zone       = "us-west-2b"
  map_public_ip_on_launch = true
}

resource "aws_subnet" "prod_redshift_subnet_c" {
  vpc_id                  = aws_vpc.prod_redshift_serverless_vpc.id
  cidr_block              = "10.0.3.0/24"
  availability_zone       = "us-west-2c"
  map_public_ip_on_launch = true
}

resource "aws_security_group" "prod_redshift_sg" {
  name        = "allow-all-redshift"
  description = "Allow all inbound access"
  vpc_id      = aws_vpc.prod_redshift_serverless_vpc.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_internet_gateway" "prod_redshift_igw" {
  vpc_id = aws_vpc.prod_redshift_serverless_vpc.id
}

resource "aws_route_table" "prod_redshift_route_table" {
  vpc_id = aws_vpc.prod_redshift_serverless_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.prod_redshift_igw.id
  }
}

resource "aws_route_table_association" "prod_redshift_subnet_a_association" {
  subnet_id      = aws_subnet.prod_redshift_subnet_a.id
  route_table_id = aws_route_table.prod_redshift_route_table.id
}

resource "aws_route_table_association" "prod_redshift_subnet_b_association" {
  subnet_id      = aws_subnet.prod_redshift_subnet_b.id
  route_table_id = aws_route_table.prod_redshift_route_table.id
}

resource "aws_route_table_association" "prod_redshift_subnet_c_association" {
  subnet_id      = aws_subnet.prod_redshift_subnet_c.id
  route_table_id = aws_route_table.prod_redshift_route_table.id
}

resource "aws_redshiftserverless_namespace" "prod_redshift_namespace" {
  namespace_name = "prod-redshift-namespace"
  db_name       = "prod_redw"
  default_iam_role_arn = aws_iam_role.prod_redshift_serverless_role.arn
  iam_roles =   [
    aws_iam_role.prod_redshift_serverless_role.arn
  ]
  depends_on = [
    aws_iam_role.prod_redshift_serverless_role,
    aws_iam_role_policy_attachment.prod_s3_redshift_serverless_policy_attachment,
    aws_iam_role_policy_attachment.prod_redshift_full_access_policy_attachment
  ]
}

resource "aws_redshiftserverless_workgroup" "prod_redshift_workgroup" {
  namespace_name    = aws_redshiftserverless_namespace.prod_redshift_namespace.namespace_name
  workgroup_name     = "prod-redshift-workgroup"
  base_capacity     = 8
  max_capacity = 8
  enhanced_vpc_routing = true
  security_group_ids = [aws_security_group.prod_redshift_sg.id]
  subnet_ids        = [
    aws_subnet.prod_redshift_subnet_a.id,
    aws_subnet.prod_redshift_subnet_b.id,
    aws_subnet.prod_redshift_subnet_c.id
  ]
  publicly_accessible = true
}
