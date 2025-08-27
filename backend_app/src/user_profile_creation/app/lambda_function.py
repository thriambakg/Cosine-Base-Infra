import json
import boto3
import os
from datetime import datetime
from typing import Dict, Any
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get('USER_PROFILES_TABLE_NAME', 'cosine-user-profiles-production')
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    """
    Lambda handler for Cognito Post Authentication trigger
    Creates a user profile in DynamoDB when a user signs in (including external providers)
    """
    try:
        logger.info(f"Processing post authentication event: {json.dumps(event)}")
        
        # Extract user information from the event
        # Use the Cognito user ID (sub) which is what the frontend uses
        user_attributes = event['request']['userAttributes']
        user_id = user_attributes.get('sub')  # This is the Cognito user ID that frontend uses
        
        # Log the user ID being used for debugging
        logger.info(f"Using user ID: {user_id}")
        logger.info(f"User attributes: {json.dumps(user_attributes)}")
        
        # Check if user profile already exists
        try:
            existing_profile = table.get_item(Key={'user_id': user_id})
            if 'Item' in existing_profile:
                logger.info(f"User profile already exists for user: {user_id}")
                return event
        except Exception as e:
            logger.warning(f"Error checking existing profile: {str(e)}")
        
        # Extract user details
        email = user_attributes.get('email', '')
        given_name = user_attributes.get('given_name', '')
        family_name = user_attributes.get('family_name', '')
        
        # Create user profile with default dashboard
        user_profile = create_user_profile(user_id, email, given_name, family_name)
        
        # Store in DynamoDB
        table.put_item(Item=user_profile)
        
        logger.info(f"Successfully created user profile for user: {user_id}")
        
        # Return the event to continue the authentication flow
        return event
        
    except Exception as e:
        logger.error(f"Error creating user profile: {str(e)}")
        # Don't fail the authentication process, just log the error
        return event

def create_user_profile(user_id: str, email: str, given_name: str, family_name: str) -> Dict[str, Any]:
    """Create a user profile with default dashboard configuration"""
    now = datetime.utcnow().isoformat()
    
    return {
        'user_id': user_id,
        'email': email,
        'given_name': given_name,
        'family_name': family_name,
        'created_at': now,
        'updated_at': now,
        'dashboard_config': create_default_dashboard(),
        'profile_status': 'active',
        'subscription_plan': 'free',
        'subscription_status': 'active'
    }

def create_default_dashboard() -> Dict[str, Any]:
    """Create a default dashboard configuration"""
    now = datetime.utcnow().isoformat()
    
    return {
        'crypto_tiles': [
            {
                'id': 'tile_1',
                'symbol': 'BTC',
                'timeframe': '1d',
                'displayOptions': {
                    'showPrice': True,
                    'show24hChange': True,
                    'showAnnualReturn': True,
                    'showVolatility': True,
                    'showChart': True
                },
                'autoRefresh': False,
                'isPinned': False,
                'size': {'width': 350, 'height': 400},
                'position': {'x': 0, 'y': 0},
                'created_at': now
            }
        ],
        'layout': 'grid',
        'last_updated': now
    }
