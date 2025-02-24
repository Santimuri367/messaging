#!/usr/bin/env python3
"""
Status Monitor - Utility for monitoring the status of all services
Can be run on any machine to see the status of all services in the system
"""

import pika
import json
import logging
import time
import sys
import os
from datetime import datetime
from config import RABBITMQ_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('status_monitor')

# Track service statuses
service_statuses = {
    'frontend': {'status': 'unknown', 'last_update': None, 'details': {}},
    'backend': {'status': 'unknown', 'last_update': None, 'details': {}},
    'database': {'status': 'unknown', 'last_update': None, 'details': {}},
    'messaging': {'status': 'unknown', 'last_update': None, 'details': {}}
}

def setup_rabbitmq_connection():
    """Establish connection to RabbitMQ server"""
    try:
        # Use SSL parameters for CloudAMQP
        ssl_options = {
            'verify_peer': True,
        }
        
        # Create connection parameters
        credentials = pika.PlainCredentials(
            RABBITMQ_CONFIG['username'],
            RABBITMQ_CONFIG['password']
        )
        
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_CONFIG['host'],
            port=RABBITMQ_CONFIG['port'],
            virtual_host=RABBITMQ_CONFIG['vhost'],
            credentials=credentials,
            ssl_options=ssl_options if RABBITMQ_CONFIG['use_ssl'] else None
        )
        
        connection = pika.BlockingConnection(parameters)
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        return None

def handle_status_message(ch, method, properties, body):
    """Handle incoming status messages"""
    try:
        message = json.loads(body)
        
        service_name = message.get('service')
        status = message.get('status')
        details = message.get('details', {})
        timestamp = message.get('timestamp')
        
        if service_name and service_name in service_statuses:
            # Update the status
            service_statuses[service_name] = {
                'status': status,
                'last_update': timestamp,
                'details': details
            }
            
            # Print a nice status message
            status_emoji = {
                'ready': '🟡',
                'running': '🟢',
                'stopped': '⚪',
                'error': '🔴',
                'unknown': '❓'
            }.get(status, '❓')
            
            # Format timestamp
            time_str = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
            
            print(f"{status_emoji} [{time_str}] {service_name.upper()} service is {status.upper()}")
            
            # If there are detailed messages, show them
            if details:
                for key, value in details.items():
                    print(f"  • {key}: {value}")
        
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in message: {body}")
    except Exception as e:
        logger.error(f"Error handling status message: {e}")

def start_monitor():
    """Start monitoring service statuses"""
    connection = setup_rabbitmq_connection()
    if not connection:
        logger.error("Failed to start monitor due to connection error")
        return False
        
    channel = connection.channel()
    
    # Declare exchanges
    channel.exchange_declare(
        exchange='service_status',
        exchange_type='topic',
        durable=True
    )
    
    # Declare a queue for receiving all status updates
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Bind to all service status messages
    channel.queue_bind(
        exchange='service_status',
        queue=queue_name,
        routing_key='service.#.status'
    )
    
    # Start consuming messages
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=handle_status_message,
        auto_ack=True
    )
    
    logger.info("Started monitoring service statuses")
    print("📊 Monitoring service statuses...")
    print("Press Ctrl+C to exit")
    
    # Start consuming (this blocks until interrupted)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()
        print("\nStopping status monitor...")
    
    return True

def main():
    """Main entry point for the status monitor"""
    print("Service Status Monitor")
    print("=====================")
    start_monitor()

if __name__ == "__main__":
    main()