package provider

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func TestAccStepResource(t *testing.T) {
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		Steps: []resource.TestStep{
			// Create and Read testing
			{
				Config: testAccStepResourceConfig("one"),
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("emrstreaming_step.example", "cluster_id", clusterId),
					resource.TestCheckResourceAttr("emrstreaming_step.example", "step.name", "example-app"),
					resource.TestCheckResourceAttr("emrstreaming_step.example", "step.status", "RUNNING"),
					resource.TestCheckResourceAttr("emrstreaming_step.example", "step.force_redeploy", "true"),
					resource.TestCheckResourceAttr("emrstreaming_step.example", "step.health_check_monitor_period", "120"),
					resource.TestCheckResourceAttr("emrstreaming_step.example", "step.pre_shutdown_wait_period", "0"),
					resource.TestCheckResourceAttr("emrstreaming_step.example", "step.shutdown_timeout", "120"),
					resource.TestCheckResourceAttr("emrstreaming_step.example", "step.hadoop_jar_step.args.#", "34"),
					resource.TestCheckResourceAttr("emrstreaming_step.example", "step.hadoop_jar_step.jar", "command-runner.jar"),
				),
			},
		},
	})
}

func testAccStepResourceConfig(configurableAttribute string) string {
	return providerConfig + fmt.Sprintf(`
resource "emrstreaming_step" "example" {
	cluster_id = %[1]q
	step  = {
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
			"--conf", "spark.shuffle.service.enabled=true", 
			"--conf", "spark.dynamicAllocation.minExecutors=1",
			"--conf", "spark.dynamicAllocation.maxExecutors=5",
			"--conf", "spark.yarn.appMasterEnv.PYTHONPATH=app-site-packages",
			"--conf", "yarn.nodemanager.vmem-check-enabled=false",
			"--conf", "spark.executor.memoryOverhead=512",
			"--conf", "spark.executorEnv.PYTHONPATH=app-site-packages",
			"s3://%[2]s/example-app/main.py"
		  ]
		  jar = "command-runner.jar"
		}
		health_check_monitor_period = 120
		name = "example-app"
		force_redeploy = true
		pre_shutdown_wait_period = 0
		shutdown_timeout = 120
	}
}
`, clusterId, bucketName)
}
