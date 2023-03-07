# Terraform Provider: EMRStreaming

The emrstreaming provider offers continuous deployment functionality for streaming steps into an EMR cluster. 
This provider is designed to submit streaming jobs that supposed to continuously run on a permanent EMR cluster.It consists of a single emrstreaming_step resource that acts as a continuous deployment resource to manage jobs lifecycle on an EMR cluster.

# Usage Example

```
terraform {
  required_providers {
    emrstreaming = {
      source = "b-b3rn4rd/emrstreaming"
    }
  }
}

# Configure the EMRStreaming Provider
provider "emrstreaming" {
  region = "ap-southeast-2"
}

resource "emrstreaming_step" "step_example" {
  cluster_id = "j-xxxxxxxxxx"
  step = {
    hadoop_jar_step = {
      args = [
        "spark-submit",
        "--name", "example-app",
        "--deploy-mode", "cluster",
        "--master", "yarn",
        "--driver-cores", 2,
        "--conf", "spark.dynamicAllocation.enabled=true",
        "--conf", "spark.executorEnv.PYTHONPATH=app-site-packages",
        "s3://example-bucket-name/example-app/main.py"
      ]
      jar = "command-runner.jar"
    }
    health_check_monitor_period = 120
    name                        = "example-app"
    force_redeploy              = true
    pre_shutdown_wait_period    = 10
    shutdown_timeout            = 60
  }
}
```
# Links
