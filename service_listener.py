#!/usr/bin/env python3
"""
Service Listener - Component that runs on each machine to listen for service control commands
This script should be deployed and run on each team member's machine
"""

import pika
import json
import logging
import time
import sys
import os
import threading
import subprocess
from config import RABBITMQ_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('service_listener')

# Track running services
running_services = {}

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

def send_status_update(service_name, status, details=None):
    """Send status update about the service"""
    try:
        connection = setup_rabbitmq_connection()
        if not connection:
            return
            
        channel = connection.channel()
        
        # Ensure the exchange exists
        channel.exchange_declare(
            exchange='service_status',
            exchange_type='topic',
            durable=True
        )
        
        # Create message payload
        message = {
            'service': service_name,
            'status': status,
            'timestamp': time.time(),
            'details': details or {}
        }
        
        # Publish message to the appropriate routing key
        routing_key = f"service.{service_name}.status"
        channel.basic_publish(
            exchange='service_status',
            routing_key=routing_key,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json'
            )
        )
        
        connection.close()
    except Exception as e:
        logger.error(f"Failed to send status update: {e}")

def start_service(service_name):
    """Start the specified service on this machine"""
    global running_services
    
    if service_name in running_services and running_services[service_name].is_alive():
        logger.info(f"Service {service_name} is already running")
        return
    
    logger.info(f"Starting {service_name} service")
    
    # In a real implementation, this would start the actual service
    # For this example, we'll just simulate it with a function
    def service_process(name):
        try:
            # Simulate starting the service
            logger.info(f"{name.upper()} SERVICE STARTED")
            send_status_update(name, 'running')
            
            # Keep the service running until program exits
            while True:
                time.sleep(5)
                # In a real implementation, you might check if the actual service is healthy
                
        except Exception as e:
            logger.error(f"Service {name} encountered an error: {e}")
            send_status_update(name, 'error', {'error': str(e)})
            
    # Start the service in a new thread
    service_thread = threading.Thread(target=service_process, args=(service_name,))
    service_thread.daemon = True
    service_thread.start()
    
    # Store the thread reference
    running_services[service_name] = service_thread
    
    # Return success
    print(f"✅ Service {service_name} has been started successfully!")
    return True

def stop_service(service_name):
    """Stop the specified service"""
    # In a real implementation, this would stop the actual service
    # For this example, we'll just report it
    logger.info(f"Stopping {service_name} service")
    send_status_update(service_name, 'stopped')
    print(f"⏹️ Service {service_name} has been stopped.")
    return True

def handle_control_message(ch, method, properties, body):
    """Handle incoming control messages"""
    try:
        message = json.loads(body)
        logger.info(f"Received control message: {message}")
        
        service_name = message.get('service')
        action = message.get('action')
        
        # Get the service name from the routing key if not in the message
        if not service_name:
            routing_parts = method.routing_key.split('.')
            if len(routing_parts) >= 2:
                service_name = routing_parts[1]
        
        if action == 'start':
            start_service(service_name)
        elif action == 'stop':
            stop_service(service_name)
        else:
            logger.warning(f"Unknown action: {action}")
            
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in message: {body}")
    except Exception as e:
        logger.error(f"Error handling message: {e}")

def start_listener(service_name):
    """Start listening for control messages for the specified service"""
    connection = setup_rabbitmq_connection()
    if not connection:
        logger.error("Failed to start listener due to connection error")
        return False
        
    channel = connection.channel()
    
    # Declare exchanges
    channel.exchange_declare(
        exchange='service_control',
        exchange_type='topic',
        durable=True
    )
    
    # Declare a queue for this service
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Bind to service-specific control messages
    routing_key = f"service.{service_name}.control"
    channel.queue_bind(
        exchange='service_control',
        queue=queue_name,
        routing_key=routing_key
    )
    
    # Also bind to broadcast messages
    channel.queue_bind(
        exchange='service_control',
        queue=queue_name,
        routing_key='service.all.control'
    )
    
    # Start consuming messages
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=handle_control_message,
        auto_ack=True
    )
    
    logger.info(f"Started listening for control messages for {service_name}")
    print(f"🎧 Listening for control commands for {service_name} service...")
    
    # Send initial status update
    send_status_update(service_name, 'ready')
    
    # Start consuming (this blocks until interrupted)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        # Handle graceful shutdown
        for service, thread in running_services.items():
            if thread.is_alive():
                stop_service(service)
        
        channel.stop_consuming()
        connection.close()
        print("\nShutting down service listener...")
    
    return True

def main():
    """Main entry point for the service listener"""
    if len(sys.argv) < 2:
        print("Usage: python service_listener.py <service_name>")
        print("Available services: frontend, backend, database, messaging")
        return
    
    service_name = sys.argv[1].lower()
    if service_name not in ['frontend', 'backend', 'database', 'messaging']:
        print(f"Unknown service: {service_name}")
        print("Available services: frontend, backend, database, messaging")
        return
    
    print(f"Starting service listener for {service_name}...")
    start_listener(service_name)

if __name__ == "__main__":
    main()

