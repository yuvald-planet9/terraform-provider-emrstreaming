package provider

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var _ provider.Provider = &EMRStepProvider{}

type EMRStepProvider struct {
	version string
}

type AssumeRole struct {
	RoleARN     types.String `tfsdk:"role_arn"`
	SessionName types.String `tfsdk:"session_name"`
	ExternalID  types.String `tfsdk:"external_id"`
}

type EMRStepProviderModel struct {
	Profile    types.String `tfsdk:"profile"`
	Region     types.String `tfsdk:"region"`
	AssumeRole *AssumeRole  `tfsdk:"assume_role"`
}

func (p *EMRStepProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "emrstreaming"
	resp.Version = p.version
}

func (p *EMRStepProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "The *emrstreaming* provider offers continuous deployment functionality for streaming steps into an EMR cluster." +
			" Use the provider to manage EMR steps via terraform.<br/>" +
			" You must configure the provider with the proper credentials before you can use it. The configuration options for the provider are identical to the terraform-aws-provider without the support for access_key and secret_key authentication",
		Attributes: map[string]schema.Attribute{
			"profile": schema.StringAttribute{
				Description: "The profile for API operations. If not set, the default profile\ncreated with `aws configure` will be used.",
				Optional:    true,
			},
			"region": schema.StringAttribute{
				Description: "The region where AWS operations will take place. Examples\nare us-east-1, us-west-2, etc.",
				Optional:    true,
			},
		},
		Blocks: map[string]schema.Block{
			"assume_role": schema.SingleNestedBlock{
				Attributes: map[string]schema.Attribute{
					"role_arn": schema.StringAttribute{
						Optional:    true,
						Description: "Amazon Resource Name (ARN) of an IAM Role to assume prior to making API calls.",
					},
					"session_name": schema.StringAttribute{
						Optional:    true,
						Description: "An identifier for the assumed role session.",
					},
					"external_id": schema.StringAttribute{
						Optional:    true,
						Description: "A unique identifier that might be required when you assume a role in another account.",
					},
				},
			},
		},
	}
}

func (p *EMRStepProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var data EMRStepProviderModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &data)...)

	if resp.Diagnostics.HasError() {
		return
	}

	var options []func(*config.LoadOptions) error
	if !data.Region.IsNull() {
		options = append(options, config.WithRegion(data.Region.ValueString()))
	}

	if !data.Profile.IsNull() {
		tflog.Info(ctx, "Creating EMR client using given profile")
		options = append(options, config.WithSharedConfigProfile(data.Profile.ValueString()))
	}

	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to create AWS EMR API client",
			"An unexpected error occurred when creating the AWS EMR API client. "+
				"If the error is not clear, please contact the provider developers.\n\n"+
				"Error: "+err.Error(),
		)
		return
	}

	var client *emr.Client
	if data.AssumeRole != nil {
		if !data.AssumeRole.RoleARN.IsNull() {
			tflog.Info(ctx, "Creating EMR client using STS")
			var stsOptions []func(*stscreds.AssumeRoleOptions)
			if !data.AssumeRole.ExternalID.IsNull() {
				stsOptions = append(stsOptions, func(o *stscreds.AssumeRoleOptions) {
					o.ExternalID = aws.String(data.AssumeRole.ExternalID.ValueString())
				})
			}
			if !data.AssumeRole.SessionName.IsNull() {
				stsOptions = append(stsOptions, func(o *stscreds.AssumeRoleOptions) {
					o.RoleSessionName = data.AssumeRole.SessionName.ValueString()
				})
			}
			stsSvc := sts.NewFromConfig(cfg)
			creds := stscreds.NewAssumeRoleProvider(stsSvc, data.AssumeRole.RoleARN.ValueString(), stsOptions...)
			cfg.Credentials = aws.NewCredentialsCache(creds)
			client = emr.NewFromConfig(cfg)
		}
	} else {
		client = emr.NewFromConfig(cfg)
	}

	resp.DataSourceData = client
	resp.ResourceData = client
}

func (p *EMRStepProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewStepResource,
	}
}

func (p *EMRStepProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &EMRStepProvider{
			version: version,
		}
	}
}
