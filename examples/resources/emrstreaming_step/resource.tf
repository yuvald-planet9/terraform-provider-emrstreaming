// add a job (example-app) to the running cluster
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
        "--executor-memory", "6g",
        "--driver-memory", "12g",
        "--executor-cores", "1",
        "--num-executors", "1",
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