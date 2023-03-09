package provider

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/hashicorp/terraform-plugin-framework-validators/int64validator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/objectplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"github.com/hashicorp/terraform-plugin-log/tflog"
)

var _ resource.Resource = &StepResource{}

func NewStepResource() resource.Resource {
	return &StepResource{}
}

type StepResource struct {
	client *emr.Client
}

type StepResourceModel struct {
	ClusterId types.String `tfsdk:"cluster_id"`
	Id        types.String `tfsdk:"id"`
	Step      StepModel    `tfsdk:"step"`
}

type StepModel struct {
	Name                     types.String       `tfsdk:"name"`
	Status                   types.String       `tfsdk:"status"`
	ForceRedeploy            types.Bool         `tfsdk:"force_redeploy"`
	HealthCheckMonitorPeriod types.Int64        `tfsdk:"health_check_monitor_period"`
	PreShutdownWaitPeriod    types.Int64        `tfsdk:"pre_shutdown_wait_period"`
	ShutdownTimeout          types.Int64        `tfsdk:"shutdown_timeout"`
	HadoopJarStep            HadoopJarStepModel `tfsdk:"hadoop_jar_step"`
}

type HadoopJarStepModel struct {
	Args       types.List   `tfsdk:"args"`
	Jar        types.String `tfsdk:"jar"`
	MainClass  types.String `tfsdk:"main_class"`
	Properties types.Map    `tfsdk:"properties"`
}

type StatusUpdateModifier struct {
}

func (m StatusUpdateModifier) Description(ctx context.Context) string {
	return "Forces resource to be re-created if the jobs status is not RUNNING"
}

func (m StatusUpdateModifier) MarkdownDescription(ctx context.Context) string {
	return "Forces resource to be re-created if the jobs status is not RUNNING"
}

func (m StatusUpdateModifier) PlanModifyString(ctx context.Context, req planmodifier.StringRequest, resp *planmodifier.StringResponse) {
	if req.StateValue.ValueString() != string(emrtypes.StepStateRunning) {
		resp.RequiresReplace = true
		resp.PlanValue = types.StringValue(string(emrtypes.StepStateRunning))
	}
}

func StatusUpdate() planmodifier.String {
	return StatusUpdateModifier{}
}

func (r *StepResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_step"
}

func (r *StepResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "The resource *emrstreaming_step* adds a step to an existing EMR cluster. This resource " +
			"is designed to submit streaming jobs that supposed to continuously run on a permanent EMR cluster." +
			"The *emrstreaming_step* acts as a continuous deployment resource to manage jobs lifecycle on an EMR cluster." +
			"A step in any status except RUNNING will be marked for replacement",
		Attributes: map[string]schema.Attribute{
			"cluster_id": schema.StringAttribute{
				Description: "A unique string that identifies a cluster.",
				Required:    true,
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Step id a unique string that identifies the step on a cluster.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"step": schema.SingleNestedAttribute{
				Required:    true,
				Description: "A step to be deployed to the cluster.",
				Attributes: map[string]schema.Attribute{
					"force_redeploy": schema.BoolAttribute{
						Required:    true,
						Description: "Force to always re-deploy step regardless of the current status",
						PlanModifiers: []planmodifier.Bool{
							boolplanmodifier.RequiresReplaceIf(func(ctx context.Context, req planmodifier.BoolRequest, resp *boolplanmodifier.RequiresReplaceIfFuncResponse) {
								var forceRedeploy types.Bool
								diags := req.Plan.GetAttribute(ctx, path.Root("step").AtName("force_redeploy"), &forceRedeploy)
								resp.Diagnostics.Append(diags...)
								if forceRedeploy.ValueBool() {
									resp.RequiresReplace = true
								}

							}, "", ""),
						},
					},
					"name": schema.StringAttribute{
						Required:    true,
						Description: "The name of the step",
						PlanModifiers: []planmodifier.String{
							stringplanmodifier.RequiresReplace(),
						},
					},
					"status": schema.StringAttribute{
						Computed:    true,
						Description: "The current status of the step, should be RUNNING for successfully submitted jobs",
						PlanModifiers: []planmodifier.String{
							StatusUpdate(),
						},
					},
					"health_check_monitor_period": schema.Int64Attribute{
						Required:    true,
						Description: "A period of time in seconds to monitor the submitted job to become RUNNING",
						Validators: []validator.Int64{
							int64validator.Between(0, 3600),
						},
					},
					"pre_shutdown_wait_period": schema.Int64Attribute{
						Optional: true,
						Description: "A period of time in seconds to wait before cancelling the job. Use this parameter when " +
							"utilizing your own shutdown approach",
						Validators: []validator.Int64{
							int64validator.Between(0, 3600),
						},
					},
					"shutdown_timeout": schema.Int64Attribute{
						Required:    true,
						Description: "A timeout value in seconds for a job to become cancelled",
						Validators: []validator.Int64{
							int64validator.Between(0, 3600),
						},
					},
					"hadoop_jar_step": schema.SingleNestedAttribute{
						Required: true,
						PlanModifiers: []planmodifier.Object{
							objectplanmodifier.RequiresReplace(),
						},
						Attributes: map[string]schema.Attribute{
							"args": schema.ListAttribute{
								Optional:    true,
								Description: "A list of command line arguments to pass to the step.",
								ElementType: types.StringType,
							},
							"jar": schema.StringAttribute{
								Description: "A path to a JAR file run during the step.",
								Required:    true,
							},
							"main_class": schema.StringAttribute{
								Description: "The name of the main class in the specified Java file.",
								Optional:    true,
							},
							"properties": schema.MapAttribute{
								Description: "A list of Java properties that are set when the step runs. You can use these properties to pass key value pairs to your main function.",
								ElementType: types.StringType,
								Optional:    true,
							},
						},
					},
				},
			},
		},
	}
}

func (r *StepResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	// Prevent panic if the provider has not been configured.
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*emr.Client)

	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *emr.Client, got: %T. Please report this issue to the provider developers.", req.ProviderData),
		)

		return
	}

	r.client = client
}

func (r *StepResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan *StepResourceModel

	// Read Terraform plan data into the model
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)

	if resp.Diagnostics.HasError() {
		return
	}
	var steps []emrtypes.StepConfig
	var hadoopJarStepArgs []string
	var hadoopJarStepProperties []emrtypes.KeyValue
	resp.Diagnostics.Append(plan.Step.HadoopJarStep.Args.ElementsAs(ctx, &hadoopJarStepArgs, false)...)
	resp.Diagnostics.Append(plan.Step.HadoopJarStep.Properties.ElementsAs(ctx, &hadoopJarStepProperties, false)...)
	steps = append(steps, emrtypes.StepConfig{
		HadoopJarStep: &emrtypes.HadoopJarStepConfig{
			Jar:        aws.String(plan.Step.HadoopJarStep.Jar.ValueString()),
			Args:       hadoopJarStepArgs,
			MainClass:  aws.String(plan.Step.HadoopJarStep.MainClass.ValueString()),
			Properties: hadoopJarStepProperties,
		},
		Name:            aws.String(plan.Step.Name.ValueString()),
		ActionOnFailure: emrtypes.ActionOnFailureContinue,
	})
	addJobFlowStepsInput := &emr.AddJobFlowStepsInput{
		JobFlowId: aws.String(plan.ClusterId.ValueString()),
		Steps:     steps,
	}
	emrStepResp, err := r.client.AddJobFlowSteps(ctx, addJobFlowStepsInput)
	if err != nil {
		resp.Diagnostics.AddError("Client Error", fmt.Sprintf("Unable to add  EMR step, got error: %s", err))
		return
	}

	plan.Id = types.StringValue(emrStepResp.StepIds[0])

	res, err := r.client.DescribeStep(ctx, &emr.DescribeStepInput{
		ClusterId: aws.String(plan.ClusterId.ValueString()),
		StepId:    aws.String(plan.Id.ValueString()),
	})
	if err != nil {
		resp.Diagnostics.AddError("Client Error", fmt.Sprintf("Unable to describe the EMR step, got error: %s", err))
		return
	}

	status := string(res.Step.Status.State)
	plan.Step.Status = types.StringValue(status)
	tflog.Info(ctx, fmt.Sprintf("step %s has been created, status %s", plan.Id, status))
	stepId := aws.String(plan.Id.ValueString())
	clusterId := aws.String(plan.ClusterId.ValueString())

	if res.Step.Status.State != emrtypes.StepStateRunning {
		healthCheckMonitorPeriod := int(plan.Step.HealthCheckMonitorPeriod.ValueInt64())
		ticker := time.NewTicker(5 * time.Second)
		tickerDone := make(chan bool, 1)
		timeoutDone := make(chan bool, 1)

		go func() {
			defer ticker.Stop()

			for {
				select {
				case <-tickerDone:
					timeoutDone <- true
					return
				case <-ticker.C:
					status, err := r.getStepStatus(ctx, stepId, clusterId)
					if err != nil {
						tflog.Error(ctx, "could not read EMR step ID "+*stepId+": "+err.Error())
						ticker.Stop()
						tickerDone <- true
					}
					if status == string(emrtypes.StepStateRunning) {
						tflog.Info(ctx, fmt.Sprintf("step reached the %s status", string(emrtypes.StepStateRunning)))
						ticker.Stop()
						tickerDone <- true
					}

					tflog.Info(ctx, fmt.Sprintf("step status is %s, waiting to become %s", status, string(emrtypes.StepStateRunning)))
					if r.isNonRunningStatus(ctx, status) {
						tflog.Error(ctx, fmt.Sprintf("step is non running state %s, expecting %s or %s", status, emrtypes.StepStatePending, emrtypes.StepStateRunning))
						ticker.Stop()
						tickerDone <- true
					}
				}
			}
		}()
		select {
		case <-timeoutDone:
			tflog.Info(ctx, fmt.Sprintf("the step has achieved the %s status before %d seconds", emrtypes.StepStateRunning, healthCheckMonitorPeriod))
		case <-time.After(time.Duration(healthCheckMonitorPeriod) * time.Second):
			tflog.Info(ctx, fmt.Sprintf("step failed to achieve the %s status within %d seconds", emrtypes.StepStateRunning, healthCheckMonitorPeriod))
			ticker.Stop()
			tickerDone <- true
		}
		status, err := r.getStepStatus(ctx, stepId, clusterId)
		if err != nil {
			resp.Diagnostics.AddError(
				"Error describing EMR step",
				"Could not read EMR step ID "+*stepId+": "+err.Error(),
			)
			return
		}

		if status != string(emrtypes.StepStateRunning) {
			resp.Diagnostics.AddError(
				fmt.Sprintf("Error waiting for EMR step to become %s", emrtypes.StepStateRunning),
				fmt.Sprintf("The step failed to reach the %s status within %d seconds, current status is %s", emrtypes.StepStateRunning, healthCheckMonitorPeriod, status),
			)
			return
		}
		plan.Step.Status = types.StringValue(status)
	}

	// Save data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *StepResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var plan *StepResourceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &plan)...)

	if resp.Diagnostics.HasError() {
		return
	}

	res, err := r.client.DescribeStep(ctx, &emr.DescribeStepInput{
		ClusterId: aws.String(plan.ClusterId.ValueString()),
		StepId:    aws.String(plan.Id.ValueString()),
	})
	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading EMR step",
			"Could not read EMR step ID "+plan.Id.ValueString()+": "+err.Error(),
		)
		return
	}

	step := res.Step
	args, diag := types.ListValueFrom(ctx, types.StringType, step.Config.Args)
	resp.Diagnostics.Append(diag...)
	properties, diag := types.MapValueFrom(ctx, types.StringType, step.Config.Properties)
	resp.Diagnostics.Append(diag...)

	plan.Step.Name = types.StringValue(*step.Name)
	plan.Step.HadoopJarStep.Args = args
	plan.Step.HadoopJarStep.Jar = types.StringValue(*step.Config.Jar)
	if *step.Config.MainClass != "" {
		plan.Step.HadoopJarStep.MainClass = types.StringValue(*step.Config.MainClass)
	}

	if len(properties.Elements()) != 0 {
		plan.Step.HadoopJarStep.Properties = properties
	}

	plan.Step.Status = types.StringValue(string(step.Status.State))
	// Save updated data into Terraform state
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *StepResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {

}

func (r *StepResource) isNonRunningStatus(ctx context.Context, status string) bool {
	nonRunningStatuses := []emrtypes.StepState{
		emrtypes.StepStateCompleted,
		emrtypes.StepStateCancelled,
		emrtypes.StepStateFailed,
		emrtypes.StepStateInterrupted,
	}

	for _, nonRunningStatus := range nonRunningStatuses {
		if nonRunningStatus == emrtypes.StepState(status) {
			return true
		}
	}

	return false
}

func (r *StepResource) getStepStatus(ctx context.Context, stepId *string, clusterId *string) (string, error) {
	res, err := r.client.DescribeStep(ctx, &emr.DescribeStepInput{
		ClusterId: clusterId,
		StepId:    stepId,
	})
	if err != nil {
		return "", err
	}

	step := res.Step
	status := string(step.Status.State)

	return status, nil
}

func (r *StepResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var plan *StepResourceModel

	// Read Terraform prior state data into the model
	resp.Diagnostics.Append(req.State.Get(ctx, &plan)...)

	if resp.Diagnostics.HasError() {
		return
	}

	cancelStep := func(stepId *string, clusterId *string, signal emrtypes.StepCancellationOption) error {
		_, err := r.client.CancelSteps(ctx, &emr.CancelStepsInput{
			ClusterId:              clusterId,
			StepIds:                []string{*stepId},
			StepCancellationOption: signal,
		})

		return err
	}

	stepId := aws.String(plan.Id.ValueString())
	clusterId := aws.String(plan.ClusterId.ValueString())
	status, err := r.getStepStatus(ctx, stepId, clusterId)

	if err != nil {
		resp.Diagnostics.AddError(
			"Error reading EMR step",
			"Could not read EMR step ID "+plan.Id.ValueString()+": "+err.Error(),
		)
		return
	}

	if r.isNonRunningStatus(ctx, status) {
		tflog.Info(ctx, "step is in non-running state skip the cancel")
		return
	}

	if !plan.Step.PreShutdownWaitPeriod.IsNull() {
		preShutdownWaitPeriod := int(plan.Step.PreShutdownWaitPeriod.ValueInt64())
		tflog.Info(ctx, fmt.Sprintf("the pre_shutdown_wait_period option has been configured to %d seconds, wait..", preShutdownWaitPeriod))

		ticker := time.NewTicker(5 * time.Second)
		tickerDone := make(chan bool, 1)
		timeoutDone := make(chan bool, 1)
		go func() {
			for {
				select {
				case <-tickerDone:
					timeoutDone <- true
					return
				case <-ticker.C:
					status, err := r.getStepStatus(ctx, stepId, clusterId)
					if err != nil {
						tflog.Error(ctx, "could not read EMR step ID "+*stepId+": "+err.Error())
						ticker.Stop()
						tickerDone <- true
					}
					tflog.Info(ctx, fmt.Sprintf("the step %s status is %s", *stepId, status))

					if r.isNonRunningStatus(ctx, status) {
						ticker.Stop()
						tickerDone <- true
					}
				}
			}
		}()
		select {
		case <-timeoutDone:
			tflog.Info(ctx, "the step has been stopped during the pre shutdown period skip the cancel")
			return
		case <-time.After(time.Duration(preShutdownWaitPeriod) * time.Second):
			tflog.Info(ctx, "step failed to stop during the pre shutdown period, continue with the cancel")
			ticker.Stop()
			tickerDone <- true
		}
	}

	err = cancelStep(stepId, clusterId, emrtypes.StepCancellationOptionSendInterrupt)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error deleting EMR step",
			"Could not delete EMR step, unexpected error: "+err.Error(),
		)
		return
	}
	shutdownTimeout := int(plan.Step.ShutdownTimeout.ValueInt64())
	ticker := time.NewTicker(5 * time.Second)
	tickerDone := make(chan bool, 1)
	timeoutDone := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-tickerDone:
				timeoutDone <- true
				return
			case <-ticker.C:
				status, err := r.getStepStatus(ctx, stepId, clusterId)
				if err != nil {
					ticker.Stop()
					tickerDone <- true
					tflog.Error(ctx, "could not read EMR step ID "+*stepId+": "+err.Error())
				}

				if r.isNonRunningStatus(ctx, status) {
					ticker.Stop()
					tickerDone <- true
				}
			}
		}
	}()
	select {
	case <-timeoutDone:
		tflog.Info(ctx, "step has been stopped during the shutdown period")
		return
	case <-time.After(time.Duration(shutdownTimeout) * time.Second):
		tflog.Info(ctx, "step failed to stop during the shutdown period, continue with the final status check")
		ticker.Stop()
		tickerDone <- true
	}
	status, err = r.getStepStatus(ctx, stepId, clusterId)
	if !r.isNonRunningStatus(ctx, status) {
		resp.Diagnostics.AddError(
			"Error cancelling EMR step",
			"Could not cancel EMR step ID "+*stepId+": "+err.Error(),
		)
		return
	}
}
