"""
LDA Execution History Exporter Lambda
Exports Step Functions execution history to S3 to avoid 25k event limit.
This Lambda should be triggered after Step Function executions complete.
"""

import json
import boto3
import os
from datetime import datetime
from typing import Dict, Optional

# AWS clients
sfn_client = boto3.client('stepfunctions')
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
STATE_MACHINE_ARN = os.environ.get('STATE_MACHINE_ARN')

def export_execution_history_to_s3(execution_arn: str) -> Optional[str]:
    """
    Export Step Functions execution history to S3.
    
    Args:
        execution_arn: ARN of the Step Functions execution
        
    Returns:
        S3 key of the exported file, or None if export failed
    """
    if not S3_BUCKET_NAME:
        print("❌ S3_BUCKET_NAME not configured")
        return None
    
    try:
        # Extract execution ID from ARN
        # Format: arn:aws:states:region:account:execution:stateMachineName:executionId
        execution_id = execution_arn.split(':')[-1]
        
        # Get execution history
        print(f"📥 Fetching execution history for {execution_id}...")
        history = []
        next_token = None
        
        # Paginate through all history events (max 25k events)
        while True:
            if next_token:
                response = sfn_client.get_execution_history(
                    executionArn=execution_arn,
                    maxResults=1000,
                    nextToken=next_token,
                    includeExecutionData=False  # Don't include input/output to reduce size
                )
            else:
                response = sfn_client.get_execution_history(
                    executionArn=execution_arn,
                    maxResults=1000,
                    includeExecutionData=False
                )
            
            history.extend(response.get('events', []))
            next_token = response.get('nextToken')
            
            if not next_token:
                break
            
            print(f"   Fetched {len(history)} events so far...")
        
        print(f"✅ Fetched {len(history)} total events")
        
        # Create S3 key: executionhistory/{executionId}/history.json
        s3_key = f"executionhistory/{execution_id}/history.json"
        
        # Prepare export data
        export_data = {
            'execution_arn': execution_arn,
            'execution_id': execution_id,
            'exported_at': datetime.utcnow().isoformat(),
            'total_events': len(history),
            'events': history
        }
        
        # Upload to S3
        print(f"📤 Uploading to s3://{S3_BUCKET_NAME}/{s3_key}...")
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(export_data, indent=2, default=str),
            ContentType='application/json'
        )
        
        print(f"✅ Successfully exported execution history to {s3_key}")
        return s3_key
        
    except Exception as e:
        print(f"❌ Error exporting execution history: {str(e)}")
        return None

def lambda_handler(event, context):
    """
    Export Step Functions execution history to S3.
    
    Input (from EventBridge/Step Functions):
    {
        "execution_arn": "arn:aws:states:us-east-1:123456789012:execution:stateMachine:executionId",
        "status": "SUCCEEDED" | "FAILED" | "TIMED_OUT" | "ABORTED"
    }
    
    Or from Step Functions CloudWatch Events:
    {
        "source": "aws.states",
        "detail": {
            "executionArn": "...",
            "status": "..."
        }
    }
    """
    print("=" * 80)
    print("🚀 LDA Execution History Exporter Lambda - Starting")
    print("=" * 80)
    
    # Extract execution ARN from event
    execution_arn = None
    
    # Check if event is from Step Functions CloudWatch Events
    if event.get('source') == 'aws.states':
        detail = event.get('detail', {})
        execution_arn = detail.get('executionArn')
        status = detail.get('status')
    else:
        # Direct invocation
        execution_arn = event.get('execution_arn')
        status = event.get('status')
    
    if not execution_arn:
        print("❌ No execution_arn found in event")
        return {
            'statusCode': 400,
            'error': 'Missing execution_arn in event'
        }
    
    print(f"📋 Execution ARN: {execution_arn}")
    print(f"📋 Status: {status}")
    
    # Export history to S3
    s3_key = export_execution_history_to_s3(execution_arn)
    
    if s3_key:
        return {
            'statusCode': 200,
            'execution_arn': execution_arn,
            's3_key': s3_key,
            'success': True
        }
    else:
        return {
            'statusCode': 500,
            'execution_arn': execution_arn,
            'error': 'Failed to export execution history',
            'success': False
        }

