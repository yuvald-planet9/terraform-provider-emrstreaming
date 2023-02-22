package provider

import (
	"os"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

const (
	providerConfig = `
provider "emrstreaming" {}
`
)

// running EMR cluster id
var clusterId = os.Getenv("CLUSTER_ID")

// spark application source code bucket
var bucketName = os.Getenv("BUCKET_NAME")
var testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"emrstreaming": providerserver.NewProtocol6WithError(New("test")()),
}
