# Migration Guide: US-EAST-1 → US-GOV-EAST-1

## Overview

This guide outlines the process for migrating the Cosine infrastructure from AWS Commercial (`us-east-1`) to AWS GovCloud (`us-gov-east-1`). This migration involves data migration of DynamoDB tables, S3 buckets, Cognito users, and other stateful resources.

## CRITICAL DATA STORES REQUIRING MIGRATION

### DynamoDB Tables (13 tables total)

1. **`cosine-user-profiles-{env}`** - User account data
   - Hash key: `user_id`
   - GSIs: EmailIndex, CreatedAtIndex

2. **`cosine-security-events-{env}`** - Security audit logs
   - Hash key: `event_id`, Range key: `timestamp`
   - GSIs: UserIndex, EventTypeIndex
   - **Has TTL enabled** (may expire, check before migration)

3. **`cosine-alerts-{env}`** - Stock alerts
   - Hash key: `alert_status`, Range key: `created_at`
   - GSIs: AlertIdIndex, UserAlertsIndex
   - **Has TTL enabled**

4. **`cosine-chat-connections-{env}`** - WebSocket connections
   - Hash key: `connection_id`
   - GSIs: UserConnectionsIndex, SessionConnectionsIndex
   - **Has TTL enabled** (expires quickly)

5. **`cosine-chat-sessions-{env}`** - Chat session data
   - Hash key: `user_id`, Range key: `session_id`
   - GSIs: CreatedAtIndex
   - **Has TTL enabled**

6. **`cosine-stock-data-{env}`** - Real-time stock data
   - Hash key: `PK`, Range key: `SK`
   - **7 GSIs**: SectorVolatilityIndex, VolatilityRangeIndex, PriceChangeRangeIndex, MarketCapRangeIndex, PriceRangeIndex, PERatioRangeIndex, DividendYieldRangeIndex

7. **`cosine-news-{env}`** - Financial news articles
   - Hash key: `PK`, Range key: `SK`
   - **5 GSIs**: GSI1-GSI5 (various search patterns)
   - **Has TTL enabled**

8. **`cosine-congress-bills-{env}`** - Congressional bills data
   - Complex structure with multiple attributes

9. **`cosine-lda-filings-{env}`** - LDA disclosure filings
   - Large table with extensive GSI usage

10. **`cosine-politician-trades-{env}`** - Politician trading data

11. **`cosine-sec-filings-cache-{env}`** - SEC filings cache

12. **`cosine-usaspending-awards-index-{env}`** - Government contracts index

13. **`cosine-sec-search-query-cache-{env}`** - SEC search cache

**All tables use:**
- KMS encryption (DynamoDB-specific key)
- Point-in-time recovery (enabled)
- DynamoDB Streams (some enabled)
- Global Secondary Indexes (varies by table)

### S3 Buckets (9 buckets)

1. **`cosine-static-hosting-{env}`** - Frontend assets, Lambda layers, Glue logs
2. **`cosine-stock-historical-{env}`** - Historical stock data
3. **`cosine-chat-files-{env}`** - User-uploaded chat files
4. **`cosine-politician-trades-{env}`** - Politician trading documents
5. **`cosine-sec-filings-{env}`** - SEC filings storage
6. **`cosine-usaspending-data-{env}`** - Government contracts data
7. **`cosine-glue-scripts-{env}`** - Glue job scripts
8. **`cosine-lda-disclosures-{env}`** - LDA disclosure data
9. **`cosine-congress-bills-data-{env}`** - Congressional bills data

**All buckets use:**
- KMS encryption (main key)
- Versioning (likely enabled)
- Lifecycle policies (varies)

### Cognito User Pools

- **`cosine-{env}`** - User authentication pool
  - Users and user attributes
  - MFA settings and devices
  - Google OAuth identity provider configuration
  - User Pool Domain (`cosine-auth-{env}`)
  - Lambda triggers (user profile creation)

### Stateful Services Requiring Attention

#### Secrets Manager
- `cosine-oauth-gaz-{env}` - Google OAuth credentials
- `cosine-newsdata-api-{env}` - NewsData.io API key
- All encrypted with KMS

#### KMS Keys
- Main key (S3, Lambda, etc.)
- DynamoDB key (tables)
- CloudWatch Logs key
- **Cannot be migrated - must be recreated**

#### SQS Queues
- `cosine-news-{env}` - News processing queue
- `cosine-usaspending-orphan-subaward-{env}` - Orphan subaward queue
- `cosine-usaspending-dlq-{env}` - Dead letter queue
- `cosine-congress-bills-bill-text-{env}` - Bill text processing queue
- `cosine-lda-pac-autocomplete-{env}` - PAC autocomplete queue
- `cosine-lda-batch-{env}` - LDA batch processing queue
- **Note**: Queue messages will be lost if not drained before migration

#### CloudWatch Logs
- Lambda function logs
- API Gateway logs
- Cognito authentication logs
- Application logs
- **Consider exporting critical logs before migration**

### Stateless Services (NO DATA MIGRATION NEEDED)

These will be recreated automatically via Terraform:
- Lambda Functions (code is in git, just redeploy)
- API Gateway (REST and WebSocket APIs)
- IAM Roles and Policies
- CloudWatch Dashboards and Alarms
- Step Functions
- EventBridge Schedulers
- Glue Jobs (scripts are in S3)
- OpenSearch (if used)

## MIGRATION STRATEGY

### Phase 1: Pre-Migration Setup (us-gov-east-1)

#### 1. Update Terraform Configuration

Update all `.auto.tfvars` files to use `us-gov-east-1`:
```hcl
aws_region = "us-gov-east-1"
```

Update backend configuration for new S3 bucket/DynamoDB table in gov cloud:
- Create new backend config files in `backend-configs/` for gov cloud
- Ensure Terraform state bucket exists in `us-gov-east-1`

#### 2. Bootstrap New Region

```bash
# Navigate to terraform directory
cd terraform/bootstrap

# Initialize with gov cloud backend
terraform init -backend-config=../backend-configs/production-gov.tfbackend

# Create bootstrap resources
terraform apply
```

Resources to create:
- S3 bucket for Terraform state in `us-gov-east-1`
- DynamoDB table for state locking in `us-gov-east-1`
- Verify KMS key creation works in gov cloud (may have restrictions)

#### 3. Infrastructure Replication

```bash
# In main terraform directory
cd terraform

# Initialize with new backend
terraform init -backend-config=backend-configs/production-gov.tfbackend

# Review planned changes
terraform plan

# Deploy empty infrastructure first (no data)
terraform apply
```

### Phase 2: Data Migration (Cross-Region Transfer)

#### DynamoDB Migration

**Option 1: AWS DMS (Database Migration Service) - Recommended for Large Tables**

Best for production with large datasets. Handles ongoing replication during cutover.

```bash
# Create DMS replication instance in us-east-1
# Create source endpoint (us-east-1 DynamoDB)
# Create target endpoint (us-gov-east-1 DynamoDB)
# Create and start replication task
```

**Considerations:**
- DMS may have different capabilities in Gov Cloud
- Verify DMS is available in `us-gov-east-1`
- Check for cross-account permissions if accounts differ

**Option 2: Manual Export/Import - For Smaller Tables or Cross-Account**

1. **Export tables to S3:**
   ```bash
   aws dynamodb export-table-to-point-in-time \
     --table-arn arn:aws:dynamodb:us-east-1:ACCOUNT:table/cosine-user-profiles-production \
     --s3-bucket cosine-dynamodb-exports \
     --s3-prefix user-profiles-export \
     --export-format DYNAMODB_JSON
   ```

2. **Transfer exports to gov cloud:**
   - Use AWS DataSync if same account
   - Use cross-account S3 copy with appropriate IAM roles
   - Or download and re-upload manually

3. **Import into gov cloud:**
   ```bash
   aws dynamodb import-table \
     --table-name cosine-user-profiles-production \
     --s3-bucket-source S3Bucket=cosine-dynamodb-exports,S3KeyPrefix=user-profiles-export/ \
     --input-format DYNAMODB_JSON
   ```

**For TTL-enabled tables:**
- Export before TTL expires (security-events, alerts, chat-connections, chat-sessions)
- Or accept that expired items won't be migrated

#### S3 Migration

**Option 1: Cross-Region Replication (Same Account)**

If both regions are in the same AWS account:
```bash
# Enable replication on source bucket
aws s3api put-bucket-replication \
  --bucket cosine-static-hosting-production \
  --replication-configuration file://replication-config.json
```

**Option 2: AWS DataSync**

Recommended for large buckets or cross-account transfers:
```bash
# Create DataSync task
aws datasync create-task \
  --source-location-arn arn:aws:s3:::cosine-static-hosting-production \
  --destination-location-arn arn:aws-us-gov:s3:::cosine-static-hosting-production \
  --options PreserveDeletedFiles=PRESERVE,PreserveDevices=NONE,VerifyMode=POINT_IN_TIME_CONSISTENT
```

**Option 3: Manual Transfer (Script)**

```bash
#!/bin/bash
SOURCE_BUCKET="cosine-static-hosting-production"
DEST_BUCKET="cosine-static-hosting-production"
REGION_SOURCE="us-east-1"
REGION_DEST="us-gov-east-1"

# Sync with proper credentials
aws s3 sync s3://${SOURCE_BUCKET} s3://${DEST_BUCKET} \
  --source-region ${REGION_SOURCE} \
  --region ${REGION_DEST} \
  --profile gov-cloud
```

**Important:**
- Preserve encryption keys (ensure KMS keys are created first)
- Preserve metadata and versioning
- Use multipart uploads for large files

#### Cognito User Migration

**1. Export Users:**

```bash
# List all users
aws cognito-idp list-users \
  --user-pool-id us-east-1_XXXXXXXXX \
  --region us-east-1 \
  > users-export.json

# Export with all attributes (requires pagination for large pools)
aws cognito-idp list-users \
  --user-pool-id us-east-1_XXXXXXXXX \
  --region us-east-1 \
  --attributes-to-get email preferred_username given_name family_name \
  > users-detailed.json
```

**2. Re-import in Gov Cloud:**

```bash
# Create import job
aws cognito-idp create-user-import-job \
  --user-pool-id us-gov-east-1_YYYYYYYYY \
  --job-name user-import-$(date +%s) \
  --cloud-watch-logs-role-arn arn:aws-us-gov:iam::ACCOUNT:role/CognitoUserImportRole \
  > import-job.json

# Upload CSV file to S3 (format required by Cognito)
# Start import job
aws cognito-idp start-user-import-job \
  --user-pool-id us-gov-east-1_YYYYYYYYY \
  --job-id <job-id>
```

**3. Update OAuth Configuration:**

- Re-create Google OAuth app for gov cloud domains
- Update Secrets Manager with new credentials:
  ```bash
  aws secretsmanager put-secret-value \
    --secret-id cosine-oauth-gaz-production \
    --secret-string '{"google_client_id":"NEW_ID","google_client_secret":"NEW_SECRET"}' \
    --region us-gov-east-1
  ```
- Update Cognito identity provider settings in Terraform
- Re-configure User Pool Domain

**Important Limitations:**
- MFA devices **cannot be migrated** - users must re-enroll
- Federated identities (Google OAuth) must be re-linked
- User passwords are hashed and cannot be migrated - users must reset passwords or use federated login

### Phase 3: Cutover

#### 1. Stop Writes to us-east-1 (Optional - Zero Downtime Strategy)

```bash
# Disable Lambda triggers
aws lambda update-event-source-mapping \
  --uuid <mapping-id> \
  --enabled false

# Drain SQS queues (process remaining messages)
# Wait for all inflight operations to complete
```

#### 2. Final Data Sync

- Perform final DMS replication sync
- Or run final S3 sync for any new files
- Verify data consistency between regions

#### 3. Switch DNS/Configuration

Update frontend configuration:
- Update API Gateway endpoints in `config.js`
- Update WebSocket URLs
- Update CloudFront origins (if using)
- Update Cognito domain URLs

#### 4. Verification

Test critical paths:
- ✅ User authentication (Cognito login/registration)
- ✅ Data queries (DynamoDB reads)
- ✅ File uploads (S3 writes)
- ✅ Lambda functions work with migrated data
- ✅ API Gateway endpoints respond correctly
- ✅ WebSocket connections establish
- ✅ Scheduled jobs (EventBridge/Step Functions) run

### Phase 4: Cleanup (us-east-1)

**⚠️ WAIT 30-90 DAYS** after successful migration before cleanup to ensure no data loss.

#### 1. Backup Verification

Confirm all critical data is in gov cloud:
- Spot check DynamoDB tables
- Verify S3 bucket contents
- Test user authentication

#### 2. Delete Old Resources

```bash
# Delete DynamoDB tables (AFTER confirming no data loss)
aws dynamodb delete-table \
  --table-name cosine-user-profiles-production \
  --region us-east-1

# Delete S3 buckets (AFTER confirming transfer)
aws s3 rb s3://cosine-static-hosting-production --force --region us-east-1

# Delete Cognito User Pool
aws cognito-idp delete-user-pool \
  --user-pool-id us-east-1_XXXXXXXXX \
  --region us-east-1

# Clean up Terraform state
terraform state rm <old-resources>
```

## CRITICAL CONSIDERATIONS FOR GOV CLOUD

### 1. Separate AWS Account

- Gov Cloud requires a **separate AWS account** from commercial AWS
- Cross-account data transfer requires appropriate IAM roles and trust relationships
- May need data pipeline/VPC peering for large transfers
- Consider using AWS Control Tower or Organizations for management

### 2. Compliance and Certifications

- Gov Cloud has different compliance requirements (FedRAMP, IL levels, etc.)
- Ensure all services used are available in `us-gov-east-1`
- Check service limitations (some services may not be available or have reduced features)
- Verify compliance certifications meet your requirements

### 3. Network Isolation

- Gov Cloud is **completely isolated** from commercial AWS
- External API calls may need different endpoints
- Third-party services (Google OAuth, NewsData.io) need gov cloud-compatible endpoints
- Internet gateway restrictions may apply

### 4. Service Availability

Verify all used services are available in Gov Cloud:
- ✅ Lambda
- ✅ DynamoDB
- ✅ S3
- ✅ Cognito
- ✅ API Gateway
- ✅ KMS
- ✅ Secrets Manager
- ✅ CloudWatch
- ✅ SQS
- ✅ Step Functions
- ✅ EventBridge
- ⚠️ DMS - Verify availability
- ⚠️ DataSync - Verify availability
- ⚠️ Glue - May have limitations
- ⚠️ OpenSearch - Verify availability

**Check AWS GovCloud Service Availability Page for current status**

### 5. Cost Considerations

- Gov Cloud pricing may differ from commercial AWS
- Data transfer costs between regions can be **substantial**:
  - $0.02 per GB for first 10 TB/month
  - Plan budget accordingly for large data volumes
- Migration tools (DMS, DataSync) incur additional costs
- Verify cost estimates before migration

### 6. Performance Differences

- Gov Cloud may have different performance characteristics
- Network latency may differ
- Some services may have lower capacity limits
- Test performance after migration

## ESTIMATED MIGRATION TIME

### Development/Staging Environment
- **1-2 weeks** (smaller data volumes, lower risk tolerance acceptable)

### Production Environment
- **4-6 weeks** (including thorough testing and verification)
- Plus **2 weeks** for post-migration verification and cleanup

### Factors Affecting Timeline
- Data volume (DynamoDB table sizes, S3 bucket sizes)
- Network bandwidth between regions
- Downtime tolerance (zero downtime requires more planning)
- Verification and testing requirements
- Compliance review processes

## RECOMMENDED TOOLS

1. **AWS DMS** - DynamoDB replication (verify Gov Cloud availability)
2. **AWS DataSync** - S3 transfer (verify Gov Cloud availability)
3. **AWS Backup** - Point-in-time backups before migration
4. **Terraform** - Infrastructure as Code for new region
5. **Custom Python/Node.js Scripts** - For Cognito user migration
6. **CloudWatch Logs Export** - For log preservation
7. **AWS CLI** - For manual operations and verification

## IMMEDIATE NEXT STEPS

1. ✅ **Verify Gov Cloud Account Access**
   - Ensure you have administrative access
   - Verify billing is set up
   - Test basic AWS CLI access: `aws sts get-caller-identity --region us-gov-east-1`

2. ✅ **Audit Current Data Volumes**
   ```bash
   # DynamoDB item counts
   aws dynamodb describe-table --table-name cosine-user-profiles-production --region us-east-1 | jq '.Table.ItemCount'
   
   # S3 bucket sizes
   aws s3 ls s3://cosine-static-hosting-production --recursive --summarize --region us-east-1
   ```

3. ✅ **Create Migration Runbook**
   - Document step-by-step procedures
   - Include rollback procedures
   - Define success criteria

4. ✅ **Set Up Terraform Backend in us-gov-east-1**
   - Create S3 bucket for state
   - Create DynamoDB table for locking
   - Configure backend config files

5. ✅ **Test Terraform Deployment**
   - Start with development environment
   - Verify all resources create successfully
   - Test a small data migration first

6. ✅ **Create Data Migration Scripts**
   - DynamoDB export/import automation
   - S3 sync scripts
   - Cognito user migration scripts

7. ✅ **Plan Maintenance Window**
   - Blue-green deployment strategy
   - Rollback plan
   - Communication plan for users

## ROLLBACK PLAN

If migration fails or issues are discovered:

1. **Immediate Rollback (< 24 hours)**
   - Switch DNS/configuration back to us-east-1
   - Re-enable us-east-1 resources
   - Keep us-gov-east-1 resources for comparison

2. **Partial Rollback**
   - Migrate specific services back
   - Hybrid deployment (some services in each region)

3. **Data Recovery**
   - Restore from backups if needed
   - Verify no data corruption
   - Test all critical paths

## POST-MIGRATION CHECKLIST

- [ ] All DynamoDB tables have correct data counts
- [ ] All S3 buckets have correct object counts
- [ ] Cognito user authentication works
- [ ] Google OAuth login works
- [ ] Lambda functions execute successfully
- [ ] API Gateway endpoints respond correctly
- [ ] WebSocket connections establish
- [ ] Scheduled jobs run on time
- [ ] CloudWatch logs are being generated
- [ ] Alarms are configured correctly
- [ ] Cost monitoring is set up
- [ ] Documentation updated with new endpoints
- [ ] Team trained on gov cloud access
- [ ] Old us-east-1 resources tagged for deletion
- [ ] Backup verification completed
- [ ] Compliance review completed

## ADDITIONAL RESOURCES

- [AWS GovCloud User Guide](https://docs.aws.amazon.com/govcloud-us/latest/UserGuide/)
- [AWS GovCloud Service Availability](https://aws.amazon.com/govcloud-us/)
- [DynamoDB Export and Import](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBPipeline.html)
- [Cognito User Import](https://docs.aws.amazon.com/cognito/latest/developerguide/cognito-user-pools-using-import-tool.html)
- [Cross-Region S3 Replication](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication.html)

## NOTES

- Last Updated: 2026-01-16
- Migration Complexity: **HIGH**
- Recommended Team Size: **2-3 engineers**
- Estimated Budget: TBD (depends on data volume and transfer costs)


