variable "project_id" {
  type      = string
  sensitive = true
}

variable "region" {
  type      = string
  sensitive = true
}

variable "ingress_bucket_name" {
  type      = string
  sensitive = true
}

variable "archive_bucket_name" {
  type      = string
  sensitive = true
}

variable "data_sources" {

  type = list(string)

  default = ["yahoo"]

}

