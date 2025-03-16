#!/usr/bin/env python3
"""
Queue Setup - Initializes standard queues for Seshin Music Event Finder
"""

import subprocess
import logging
import argparse
import time
import pika
import json
from typing import List, Dict, Any

# Import constants
try:
    from messaging_constants import *
except ImportError:
    # If not available, define default queue names
    EVENT_NOTIFICATIONS_QUEUE = "event_notifications"
    SEARCH_REQUESTS_QUEUE = "search_requests"
    SEARCH_RESULTS_QUEUE = "search_results"
    UI_NOTIFICATIONS_QUEUE = "ui_notifications"
    USER_ACTIONS_QUEUE = "user_actions"
    DB_WRITE_QUEUE = "db_write_requests"
    DB_STATUS_QUEUE = "db_status_updates"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('queue_setup')

def ensure_rabbitmq_running():
    """
    Ensure RabbitMQ is running before proceeding.
    
    Returns:
        bool: True if running, False otherwise
    """
    try:
        # Check if service is running
        result = subprocess.run(
            "systemctl is-active rabbitmq-server", 
            shell=True, 
            stdout=subprocess.PIPE
        )
        
        if result.stdout.decode().strip() == "active":
            logger.info("RabbitMQ is running")
            return True
        
        # Try to start it
        logger.info("RabbitMQ is not running, attempting to start...")
        subprocess.run(
            "sudo systemctl start rabbitmq-server", 
            shell=True, 
            check=True
        )
        
        # Wait for it to start
        time.sleep(10)
        
        # Check again
        result = subprocess.run(
            "systemctl is-active rabbitmq-server", 
            shell=True, 
            stdout=subprocess.PIPE
        )
        
        if result.stdout.decode().strip() == "active":
            logger.info("RabbitMQ started successfully")
            return True
        else:
            logger.error("Failed to start RabbitMQ")
            return False
            
    except Exception as e:
        logger.error(f"Error checking/starting RabbitMQ: {e}")
        return False

def create_standard_queues():
    """
    Create all standard queues for the application.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        
        # Define standard queues
        standard_queues = [
            # Backend -> All Services
            EVENT_NOTIFICATIONS_QUEUE,  # New events published by backend
            
            # Backend <-> Database 
            SEARCH_REQUESTS_QUEUE,      # Search queries from backend to database
            SEARCH_RESULTS_QUEUE,       # Results from database to backend
            DB_WRITE_QUEUE,             # Write requests from backend to database
            DB_STATUS_QUEUE,            # Status updates from database to backend
            
            # Backend <-> Frontend
            UI_NOTIFICATIONS_QUEUE,     # Notifications from backend to frontend
            USER_ACTIONS_QUEUE,         # User actions from frontend to backend
        ]
        
        # Create each queue
        for queue_name in standard_queues:
            channel.queue_declare(queue=queue_name, durable=True)
            logger.info(f"Created queue: {queue_name}")
        
        # Close connection
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating standard queues: {e}")
        return False

def purge_all_queues():
    """
    Purge all standard queues.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        
        # Define standard queues
        standard_queues = [
            EVENT_NOTIFICATIONS_QUEUE,
            SEARCH_REQUESTS_QUEUE,
            SEARCH_RESULTS_QUEUE,
            DB_WRITE_QUEUE,
            DB_STATUS_QUEUE,
            UI_NOTIFICATIONS_QUEUE,
            USER_ACTIONS_QUEUE,
        ]
        
        # Purge each queue
        for queue_name in standard_queues:
            try:
                channel.queue_purge(queue=queue_name)
                logger.info(f"Purged queue: {queue_name}")
            except pika.exceptions.ChannelClosedByBroker:
                # Queue might not exist
                logger.warning(f"Queue not found: {queue_name}")
                # Reopen channel
                channel = connection.channel()
        
        # Close connection
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error purging queues: {e}")
        return False

def delete_all_queues():
    """
    Delete all standard queues.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        
        # Define standard queues
        standard_queues = [
            EVENT_NOTIFICATIONS_QUEUE,
            SEARCH_REQUESTS_QUEUE,
            SEARCH_RESULTS_QUEUE,
            DB_WRITE_QUEUE,
            DB_STATUS_QUEUE,
            UI_NOTIFICATIONS_QUEUE,
            USER_ACTIONS_QUEUE,
        ]
        
        # Delete each queue
        for queue_name in standard_queues:
            try:
                channel.queue_delete(queue=queue_name)
                logger.info(f"Deleted queue: {queue_name}")
            except pika.exceptions.ChannelClosedByBroker:
                # Queue might not exist
                logger.warning(f"Queue not found: {queue_name}")
                # Reopen channel
                channel = connection.channel()
        
        # Close connection
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error deleting queues: {e}")
        return False

def send_test_messages():
    """
    Send test messages to each standard queue.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        
        # Get current timestamp
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        
        # Test messages for each queue
        test_messages = {
            EVENT_NOTIFICATIONS_QUEUE: {
                "message_type": "test_event",
                "timestamp": timestamp,
                "data": {
                    "event_id": "test-123",
                    "name": "Test Concert",
                    "artist": "Test Artist",
                    "venue": "Test Venue"
                }
            },
            SEARCH_REQUESTS_QUEUE: {
                "request_id": "test-req-123",
                "timestamp": timestamp,
                "query": {
                    "artist": "Test Artist",
                    "tags": ["test"]
                }
            },
            SEARCH_RESULTS_QUEUE: {
                "request_id": "test-req-123",
                "timestamp": timestamp,
                "results": [
                    {
                        "event_id": "test-123",
                        "name": "Test Concert",
                        "artist": "Test Artist",
                        "venue": "Test Venue"
                    }
                ],
                "total_results": 1
            }
        }
        
        # Send messages to each queue
        for queue_name, message in test_messages.items():
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type='application/json'
                )
            )
            logger.info(f"Sent test message to queue: {queue_name}")
        
        # Close connection
        connection.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error sending test messages: {e}")
        return False

def main():
    """Main script entry point"""
    parser = argparse.ArgumentParser(description="RabbitMQ Queue Setup for Seshin Music Event Finder")
    parser.add_argument("--create", action="store_true", help="Create standard queues")
    parser.add_argument("--purge", action="store_true", help="Purge all queues")
    parser.add_argument("--delete", action="store_true", help="Delete all queues")
    parser.add_argument("--test", action="store_true", help="Send test messages to queues")
    
    args = parser.parse_args()
    
    # Ensure RabbitMQ is running
    if not ensure_rabbitmq_running():
        logger.error("Cannot proceed without RabbitMQ running")
        return False
    
    # Process commands
    if args.create:
        create_standard_queues()
    
    if args.purge:
        purge_all_queues()
    
    if args.delete:
        delete_all_queues()
    
    if args.test:
        send_test_messages()
    
    # If no arguments, create queues by default
    if not (args.create or args.purge or args.delete or args.test):
        create_standard_queues()
    
    logger.info("Queue setup complete")
    return True

if __name__ == "__main__":
    main()
