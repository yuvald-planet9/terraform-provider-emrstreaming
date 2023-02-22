terraform {
  required_providers {
    emrstreaming = {
      source = "registry.terraform.io/b-b3rn4rd/emrstreaming"
    }
  }
}

# Configure the EMRStreaming Provider
provider "emrstreaming" {
  region = "ap-southeast-2"
}