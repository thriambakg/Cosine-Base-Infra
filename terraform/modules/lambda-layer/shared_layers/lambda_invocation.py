"""
Shared Lambda Invocation Layer
Provides common functions for invoking AWS Lambda functions from agent tools.
"""

import json
import boto3
import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional, Union
from io import BytesIO

# Configure logging
logger = logging.getLogger()

def upload_file_and_notify(
    content: Union[str, bytes],
    filename: str,
    user_id: str,
    session_id: str,
    file_type: str = "txt",
    content_type: str = None,
    metadata: Dict[str, str] = None,
    folder: str = "agent-files"
) -> str:
    """
    Unified file upload function that handles S3 upload and Lambda notification.
    
    Args:
        content: File content (string or bytes)
        filename: Name of the file
        user_id: User ID
        session_id: Session ID
        file_type: Type of file (txt, csv, png, etc.)
        content_type: MIME content type (auto-detected if None)
        metadata: Additional S3 metadata
        folder: S3 folder (agent-files or files)
        
    Returns:
        str: Success message or error message
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        bucket_name = os.environ.get('CHAT_FILES_BUCKET_NAME')
        
        if not bucket_name:
            return "Error: CHAT_FILES_BUCKET_NAME not configured"
        
        # Convert string content to bytes if needed
        if isinstance(content, str):
            file_content = content.encode('utf-8')
            if not content_type:
                content_type = 'text/plain'
        else:
            file_content = content
            if not content_type:
                content_type = 'application/octet-stream'
        
        # Set content type based on file extension if not provided
        if not content_type:
            extension = filename.split('.')[-1].lower()
            content_type_map = {
                'txt': 'text/plain',
                'csv': 'text/csv',
                'json': 'application/json',
                'png': 'image/png',
                'jpg': 'image/jpeg',
                'jpeg': 'image/jpeg',
                'pdf': 'application/pdf',
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'xls': 'application/vnd.ms-excel',
                'html': 'text/html',
                'xml': 'application/xml',
                'zip': 'application/zip'
            }
            content_type = content_type_map.get(extension, 'application/octet-stream')
        
        # Override content type for specific file types
        if file_type == 'csv':
            content_type = 'text/csv'
        elif file_type == 'png':
            content_type = 'image/png'
        elif file_type == 'json':
            content_type = 'application/json'
        
        # Create S3 key
        s3_key = f"users/{user_id}/sessions/{session_id}/{folder}/{filename}"
        
        # Prepare metadata
        upload_metadata = {
            'user_id': user_id,
            'session_id': session_id,
            'filename': filename,
            'file_type': file_type,
            'upload_timestamp': str(int(datetime.utcnow().timestamp()))
        }
        
        if metadata:
            upload_metadata.update(metadata)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=file_content,
            ContentType=content_type,
            Metadata=upload_metadata
        )
        
                logger.info(f"Successfully uploaded file: {s3_key}")
                
                # Create file metadata for return
                file_metadata = {
                    'filename': filename,
                    's3_key': s3_key,
                    's3_url': f"https://{bucket_name}.s3.amazonaws.com/{s3_key}",
                    'file_size': len(file_content),
                    'content_type': content_type,
                    'upload_timestamp': str(int(datetime.utcnow().timestamp())),
                    'file_id': filename.split('.')[0] if '.' in filename else filename,
                    'generated_by': 'agent'
                }
                
                # Notify agent files processor if this is an agent file
                if folder == "agent-files":
                    success = invoke_agent_files_processor(
                        s3_bucket_name=bucket_name,
                        s3_key=s3_key,
                        file_size=len(file_content),
                        file_metadata=file_metadata
                    )
                    
                    if not success:
                        logger.warning(f"File uploaded but failed to notify processor: {s3_key}")
                
                return f"✅ Successfully uploaded file '{filename}' to {folder} folder. The file will appear in the file menu."
        
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}")
        return f"Error uploading file: {str(e)}"

def invoke_agent_files_processor(s3_bucket_name: str, s3_key: str, file_size: int = 0, file_metadata: Dict[str, Any] = None) -> bool:
    """
    Invoke the agent files processor Lambda to update session_variables.
    
    Args:
        s3_bucket_name: Name of the S3 bucket
        s3_key: S3 object key
        file_size: Size of the file in bytes
        file_metadata: File metadata dictionary
        
    Returns:
        bool: True if invocation was successful, False otherwise
    """
    try:
        lambda_client = boto3.client('lambda')
        agent_files_processor_function = os.environ.get('AGENT_FILES_PROCESSOR_FUNCTION_NAME')
        
        if not agent_files_processor_function:
            logger.warning("AGENT_FILES_PROCESSOR_FUNCTION_NAME not set - skipping Lambda invocation")
            return False
        
        # Parse S3 key to extract user_id and session_id
        # Expected format: users/{user_id}/sessions/{session_id}/agent-files/{filename}
        key_parts = s3_key.split('/')
        if len(key_parts) < 5 or key_parts[0] != 'users' or key_parts[2] != 'sessions' or key_parts[4] != 'agent-files':
            logger.warning(f"Invalid agent file key format: {s3_key}")
            return False
        
        user_id = key_parts[1]
        session_id = key_parts[3]
        filename = key_parts[5] if len(key_parts) > 5 else 'unknown'
        
        # Create direct invocation payload
        direct_payload = {
            'type': 'direct_invocation',
            'user_id': user_id,
            'session_id': session_id,
            's3_bucket_name': s3_bucket_name,
            's3_key': s3_key,
            'file_size': file_size,
            'filename': filename,
            'file_metadata': file_metadata or {}
        }
        
        lambda_client.invoke(
            FunctionName=agent_files_processor_function,
            InvocationType='Event',
            Payload=json.dumps(direct_payload)
        )
        logger.info(f"Successfully invoked agent files processor for: {s3_key}")
        return True
    except Exception as lambda_error:
        logger.error(f"Failed to invoke agent files processor: {lambda_error}")
        return False

def invoke_websocket_processor(user_id: str, session_id: str, message_type: str, payload: Dict[str, Any]) -> bool:
    """
    Invoke the WebSocket processor Lambda to send updates to frontend.
    
    Args:
        user_id: User ID
        session_id: Session ID
        message_type: Type of message to send
        payload: Additional payload data
        
    Returns:
        bool: True if invocation succeeded, False otherwise
    """
    try:
        lambda_client = boto3.client('lambda')
        websocket_processor_function = os.environ.get('WEBSOCKET_PROCESSOR_FUNCTION_NAME')
        
        if not websocket_processor_function:
            logger.warning("WEBSOCKET_PROCESSOR_FUNCTION_NAME not set - skipping WebSocket invocation")
            return False
        
        # Create WebSocket payload
        websocket_payload = {
            'type': message_type,
            'user_id': user_id,
            'session_id': session_id,
            **payload
        }
        
        # Invoke Lambda asynchronously
        lambda_client.invoke(
            FunctionName=websocket_processor_function,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(websocket_payload)
        )
        
        logger.info(f"Successfully invoked WebSocket processor for session {session_id}")
        return True
        
    except Exception as lambda_error:
        logger.warning(f"Failed to invoke WebSocket processor: {lambda_error}")
        return False
