#!/usr/bin/env python3
"""
Test Script for Service Control System
This script can be used to test if the service control system is working correctly
"""

import pika
import json
import logging
import time
import sys
import os
from config import RABBITMQ_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_service_control')

def setup_rabbitmq_connection():
    """Establish connection to RabbitMQ server"""
    try:
        ssl_options = {
            'verify_peer': True,
        }
        
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
        logger.info("Successfully connected to RabbitMQ")
        return connection
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        return None

def test_connection():
    print("Testing RabbitMQ connection")
    connection = setup_rabbitmq_connection()
    if connection:
        print("Successfully connected to RabbitMQ")
        connection.close()
        return True
    else:
        print("Failed to connect to RabbitMQ")
        return False

def test_exchanges():
    print("Testing RabbitMQ exchanges...")
    connection = setup_rabbitmq_connection()
    if not connection:
        return False
    
    try:
        channel = connection.channel()
        
        # Test creating exchanges
        channel.exchange_declare(
            exchange='service_control_test',
            exchange_type='topic',
            durable=True
        )
        
        channel.exchange_declare(
            exchange='service_status_test',
            exchange_type='topic',
            durable=True
        )
        
        print("Successfully created test exchanges")
        
        # Clean up test exchanges
        channel.exchange_delete(exchange='service_control_test')
        channel.exchange_delete(exchange='service_status_test')
        
        connection.close()
        return True
    except Exception as e:
        logger.error(f"Failed to test exchanges: {e}")
        print(f"Failed to create test exchanges: {e}")
        if connection:
            connection.close()
        return False

def test_send_message():
    print("Testing sending a message...")
    connection = setup_rabbitmq_connection()
    if not connection:
        return False
    
    try:
        channel = connection.channel()
        
        # Declare the exchange
        channel.exchange_declare(
            exchange='service_control',
            exchange_type='topic',
            durable=True
        )
        
        # Create test message
        message = {
            'action': 'test',
            'timestamp': time.time(),
            'service': 'test',
            'details': {'test': True}
        }
        
        # Publish message
        channel.basic_publish(
            exchange='service_control',
            routing_key='service.test.control',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  
                content_type='application/json'
            )
        )
        
        print("Successfully sent test message")
        connection.close()
        return True
    except Exception as e:
        logger.error(f"Failed to send test message: {e}")
        print(f"Failed to send test message: {e}")
        if connection:
            connection.close()
        return False

def test_ping_service(service_name):
    print(f"Testing ping to {service_name} service...")
    connection = setup_rabbitmq_connection()
    if not connection:
        return False
    
    try:
        channel = connection.channel()
        
        # Declare the exchange
        channel.exchange_declare(
            exchange='service_control',
            exchange_type='topic',
            durable=True
        )
        
        # Create ping message
        message = {
            'action': 'ping',
            'timestamp': time.time(),
            'service': service_name
        }
        
        # Publish message
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
        
        print(f"Successfully sent ping to {service_name} service")
        connection.close()
        return True
    except Exception as e:
        logger.error(f"Failed to ping {service_name}: {e}")
        print(f"Failed to ping {service_name}: {e}")
        if connection:
            connection.close()
        return False

def main():
    """Main entry point for the test script"""
    print("Service Control System Test")
    
    # Test connection
    if not test_connection():
        print("Connection test failed.")
        return
    
    # Test exchanges
    if not test_exchanges():
        print("Exchange test failed.")
        return
    
    # Test sending a message
    if not test_send_message():
        print("Message test failed.")
        return
    
    # Test pinging each service
    services = ['frontend', 'backend', 'database', 'messaging']
    for service in services:
        test_ping_service(service)
    
    print("\nAll tests completed!")

if __name__ == "__main__":
    main()
