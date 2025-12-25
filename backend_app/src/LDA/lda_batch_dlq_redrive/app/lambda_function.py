"""
LDA Batch DLQ Automatic Redrive Lambda
Automatically redrives messages from DLQ back to the source queue.
"""

import json
import os
import boto3
from botocore.exceptions import ClientError

# Environment variables
DLQ_QUEUE_URL = os.environ.get('DLQ_QUEUE_URL')
SOURCE_QUEUE_ARN = os.environ.get('SOURCE_QUEUE_ARN')

# AWS clients
sqs_client = boto3.client('sqs')

def lambda_handler(event, context):
    """
    Automatically redrives messages from DLQ back to source queue.
    
    Uses SQS StartMessageMoveTask API to move messages from DLQ to source queue.
    """
    print("=" * 80)
    print("🔄 LDA Batch DLQ Automatic Redrive - Starting")
    print("=" * 80)
    
    if not DLQ_QUEUE_URL or not SOURCE_QUEUE_ARN:
        error_msg = "Missing required environment variables: DLQ_QUEUE_URL or SOURCE_QUEUE_ARN"
        print(f"❌ {error_msg}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
    
    try:
        print(f"📋 DLQ Queue URL: {DLQ_QUEUE_URL}")
        print(f"📋 Source Queue ARN: {SOURCE_QUEUE_ARN}")
        
        # Get approximate number of messages in DLQ
        try:
            dlq_attributes = sqs_client.get_queue_attributes(
                QueueUrl=DLQ_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            approx_messages = int(dlq_attributes['Attributes'].get('ApproximateNumberOfMessages', 0))
            print(f"📊 Approximate messages in DLQ: {approx_messages}")
            
            if approx_messages == 0:
                print("✅ No messages in DLQ to redrive")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'No messages in DLQ to redrive',
                        'messages_redriven': 0
                    })
                }
        except ClientError as e:
            print(f"⚠️  Error getting DLQ attributes: {str(e)}")
            # Continue anyway - try to start redrive
        
        # Get source queue URL from ARN
        # ARN format: arn:aws:sqs:region:account-id:queue-name
        source_queue_name = SOURCE_QUEUE_ARN.split(':')[-1]
        source_queue_url = sqs_client.get_queue_url(QueueName=source_queue_name)['QueueUrl']
        print(f"📋 Source Queue URL: {source_queue_url}")
        
        # Redrive messages from DLQ to source queue
        # Process messages in batches (max 10 at a time)
        messages_redriven = 0
        max_messages_per_batch = 10
        max_iterations = 100  # Limit to prevent infinite loops
        
        for iteration in range(max_iterations):
            # Receive messages from DLQ
            try:
                response = sqs_client.receive_message(
                    QueueUrl=DLQ_QUEUE_URL,
                    MaxNumberOfMessages=max_messages_per_batch,
                    WaitTimeSeconds=0,  # Don't wait, just poll
                    MessageAttributeNames=['All']
                )
                
                messages = response.get('Messages', [])
                
                if not messages:
                    print(f"✅ No more messages to redrive (processed {messages_redriven} messages)")
                    break
                
                print(f"📥 Received {len(messages)} messages from DLQ, redriving to source queue...")
                
                # Redrive each message to source queue
                for message in messages:
                    try:
                        # Send message back to source queue
                        sqs_client.send_message(
                            QueueUrl=source_queue_url,
                            MessageBody=message['Body'],
                            MessageAttributes=message.get('MessageAttributes', {})
                        )
                        
                        # Delete message from DLQ
                        sqs_client.delete_message(
                            QueueUrl=DLQ_QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        
                        messages_redriven += 1
                        
                    except ClientError as e:
                        print(f"⚠️  Error redriving message: {str(e)}")
                        # Continue with next message
                        continue
                
                print(f"✅ Redrived {len(messages)} messages (total: {messages_redriven})")
                
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                if error_code == 'AWS.SimpleQueueService.NonExistentQueue':
                    print(f"❌ DLQ queue not found: {DLQ_QUEUE_URL}")
                    raise
                else:
                    print(f"⚠️  Error receiving messages: {str(e)}")
                    break
        
        print(f"✅ Redrive complete: {messages_redriven} messages redriven")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'DLQ redrive completed successfully',
                'messages_redriven': messages_redriven
            })
        }
        
    except Exception as e:
        print(f"❌ Error in DLQ redrive: {str(e)}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f'Failed to redrive messages: {str(e)}'
            })
        }

