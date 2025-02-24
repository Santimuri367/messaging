#!/usr/bin/env python3
"""
Composer Script - Main entry point for starting distributed services
This script allows starting any service in the distributed system using RabbitMQ for communication
"""

import pika
import json
import argparse
import logging
import time
import os
import sys
from config import RABBITMQ_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('composer')

def setup_rabbitmq_connection():
    """Establish connection to RabbitMQ server"""
    try:
        # Create connection parameters
        credentials = pika.PlainCredentials(
            RABBITMQ_CONFIG['username'],
            RABBITMQ_CONFIG['password']
        )
        
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_CONFIG['host'],
            port=RABBITMQ_CONFIG['port'],
            virtual_host=RABBITMQ_CONFIG['vhost'],
            credentials=credentials
        )
        
        connection = pika.BlockingConnection(parameters)
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        sys.exit(1)

def send_start_command(service_name):
    """Send command to start a specific service"""
    try:
        connection = setup_rabbitmq_connection()
        channel = connection.channel()
        
        # Ensure the exchange exists
        channel.exchange_declare(
            exchange='service_control',
            exchange_type='topic',
            durable=True
        )
        
        # Create message payload
        message = {
            'action': 'start',
            'timestamp': time.time(),
            'service': service_name
        }
        
        # Publish message to the appropriate routing key
        routing_key = f"service.{service_name}.control"
        channel.basic_publish(
            exchange='service_control',
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json'
            )
        )
        
        logger.info(f"Sent start command to {service_name}")
        connection.close()
        return True
    except Exception as e:
        logger.error(f"Failed to send start command: {e}")
        return False

def start_all_services():
    """Start all services in the system"""
    services = ['frontend', 'backend', 'database', 'messaging']
    results = {}
    
    for service in services:
        result = send_start_command(service)
        results[service] = result
        if result:
            print(f" Sent start command to {service} service")
        else:
            print(f"Failed to send start command to {service} service")
    
    return results

def main():
    """Main entry point for the composer script"""
    parser = argparse.ArgumentParser(description='Service Composer for distributed system')
    parser.add_argument('--service', help='Service to start (frontend, backend, database, messaging, or all)')
    args = parser.parse_args()
    
    if not args.service:
        print("Please specify a service to start: --service [frontend|backend|database|messaging|all]")
        return
    
    if args.service.lower() == 'all':
        start_all_services()
    elif args.service.lower() in ['frontend', 'backend', 'database', 'messaging']:
        result = send_start_command(args.service.lower())
        if result:
            print(f"Sent start command to {args.service} service")
        else:
            print(f" Failed to send start command to {args.service} service")
    else:
        print(f"Unknown service: {args.service}")
        print("Available services: frontend, backend, database, messaging, all")

if __name__ == "__main__":
    main()
