# IDV Obligation Update Glue Job

## Purpose

This is a **temporary one-time maintenance Glue job** that updates IDV parent awards in DynamoDB. It copies `combined_obligated_amount` to `total_obligated_amount` for IDV parent awards, making them queryable by obligation amount in GSIs.

## Why This Job Exists

IDV (Indefinite Delivery Vehicle) parent awards often have `total_obligated_amount = 0` in the bulk CSV data because they don't have direct obligations - only their child awards do. However, the enrichment lambda sets `combined_obligated_amount` which represents the sum of all child award obligations.

This job updates existing IDV parent awards in the database to have the correct `total_obligated_amount` value.

## Infrastructure

The job is defined in Terraform as:
- **Module**: `module.idv_obligation_update_glue_job`
- **Job Name**: `{project_name}-idv-obligation-update-{environment}`
- **Script**: `s3://{glue-scripts-bucket}/govt_contracts/update_idv_obligations.py`

## Permissions

The job has the same permissions as the indexing job:
- Read/Write access to DynamoDB table: `cosine-usaspending-awards-index-production`
- Access to KMS keys for encryption
- Access to S3 for logs and temp files

## Usage

### 1. Deploy the Infrastructure

```bash
cd Cosine-Base-Infra/terraform
terraform apply
```

This will:
- Upload the Glue script to S3
- Create the Glue job
- Set up IAM roles and permissions

### 2. Run the Job

You can run it via:
- **AWS Console**: Go to Glue → Jobs → `{project_name}-idv-obligation-update-{environment}` → Run job
- **AWS CLI**:
  ```bash
  aws glue start-job-run --job-name {project_name}-idv-obligation-update-{environment}
  ```

### 3. Monitor Progress

The job logs progress to CloudWatch:
- Shows items scanned, updated, skipped, and errors
- Updates every 100 items processed
- Summary at the end

### 4. Destroy After Completion

Once the job has completed successfully:

```bash
cd Cosine-Base-Infra/terraform
terraform destroy -target=module.idv_obligation_update_glue_job
terraform destroy -target=aws_kms_grant.idv_update_glue_dynamodb_key_access
```

Or remove the module from `main.tf` and run `terraform apply`.

## What It Does

1. Scans the DynamoDB table for awards where `award_id` starts with `CONT_IDV`
2. Filters for parent IDVs (those with `is_idv_parent=True`, `child_awards` field, or `category='idv'`)
3. For each IDV:
   - Checks if `total_obligated_amount` is missing, None, or 0
   - If `combined_obligated_amount` exists and is > 0:
     - Updates `total_obligated_amount` = `combined_obligated_amount`
4. Reports statistics

## Expected Runtime

- Depends on number of IDV parent awards in the table
- Typically completes in 10-30 minutes for a few thousand IDVs
- Uses 2 G.1X workers (32 GB total memory)

## Notes

- This is a **one-time maintenance job** - it's safe to destroy after completion
- The job uses DynamoDB `Scan` with a filter, which is efficient for this use case
- Progress is logged to CloudWatch for monitoring
- The job is idempotent - safe to run multiple times (only updates items that need updating)

