# job-queue

## Terraform

```
export AWS_PROFILE=<your-profile>
export AWS_REGION=us-east-1
terraform init # install terraform provider and modules
terraform plan # plan the execution
terraform apply --auto-approve # execute the plan
```

Apply with input variables:

```
terraform apply --auto-approve -var="name_prefix=xiaoapp"
terraform apply --var name_prefix=xiaoapp
```
