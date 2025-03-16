#!/usr/bin/env python3
"""
Simple Service Broker - Simplified interface for services to use RabbitMQ messaging
This module makes it easier for frontend, backend, and database services to 
communicate with each other through RabbitMQ.
"""

import json
import threading
import logging
from typing import Dict, Any, Callable, Optional, List, Union

# Import the core messaging broker
from messaging_broker import MessageBroker

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('service_broker')

class ServiceBroker:
    """
    A simplified broker for services to communicate with each other.
    Wraps the more complex MessageBroker with simpler methods.
    """
    
    def __init__(
        self, 
        service_type: str,
        host: str = '10.147.20.12',  # IP from your Seshin Music Event Finder
        auto_connect: bool = True
    ):
        """
        Initialize the service broker.
        
        Args:
            service_type: Type of service ('frontend', 'backend', or 'database')
            host: RabbitMQ server hostname or IP
            auto_connect: Whether to connect automatically on initialization
        """
        # Validate service type
        if service_type not in ['frontend', 'backend', 'database']:
            raise ValueError("Service type must be 'frontend', 'backend', or 'database'")
            
        self.service_type = service_type
        self.host = host
        
        # Create the core message broker
        self.broker = MessageBroker(service_name=service_type)
        
        # Consumer thread
        self._consumer_thread = None
        self._is_consuming = False
        
        # Connect if requested
        if auto_connect:
            self.connect()
    
    def connect(self) -> bool:
        """
        Connect to the RabbitMQ messaging service.
        
        Returns:
            bool: True if connection successful
        """
        try:
            success = self.broker.connect()
            if success:
                logger.info(f"{self.service_type.capitalize()} service connected to messaging")
            else:
                logger.error(f"Failed to connect {self.service_type} to messaging service")
            return success
        except Exception as e:
            logger.error(f"Error connecting to messaging service: {e}")
            return False
    
    def send_message(
        self, 
        to_service: str, 
        message_type: str, 
        data: Dict[str, Any]
    ) -> Optional[str]:
        """
        Send a message to another service.
        
        Args:
            to_service: Target service ('frontend', 'backend', or 'database')
            message_type: Type of message (e.g., 'notification', 'query', 'update')
            data: Message data
            
        Returns:
            str: Message ID if sent successfully, None otherwise
        """
        if to_service not in ['frontend', 'backend', 'database']:
            logger.error(f"Invalid target service: {to_service}")
            return None
            
        # Prepare the message
        message = {
            "type": message_type,
            **data
        }
        
        try:
            # Send to the appropriate service
            if to_service == 'frontend':
                return self.broker.send_to_frontend(message)
            elif to_service == 'backend':
                return self.broker.send_to_backend(message)
            elif to_service == 'database':
                return self.broker.send_to_database(message)
        except Exception as e:
            logger.error(f"Error sending message to {to_service}: {e}")
            return None
    
    def receive_messages(self, callback: Callable[[Dict[str, Any]], None]) -> bool:
        """
        Start receiving messages intended for this service.
        
        Args:
            callback: Function to call for each message with parsed data
            
        Returns:
            bool: True if started successfully
        """
        # Wrap the callback to handle the pika channel and method
        def wrapped_callback(ch, method, properties, body):
            try:
                # Parse the message
                message = json.loads(body)
                
                # Call the user's callback with just the message data
                callback(message)
                
                # Acknowledge the message
                ch.basic_ack(delivery_tag=method.delivery_tag)
                
            except json.JSONDecodeError:
                logger.error("Received invalid JSON message")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        try:
            # Start consuming in a new thread
            if self._consumer_thread is None or not self._consumer_thread.is_alive():
                self._is_consuming = True
                self._consumer_thread = threading.Thread(
                    target=self.broker.consume_with_callback,
                    args=(wrapped_callback,),
                    daemon=True
                )
                self._consumer_thread.start()
                logger.info(f"{self.service_type.capitalize()} service started consuming messages")
                return True
            else:
                logger.warning(f"{self.service_type.capitalize()} service is already consuming messages")
                return False
                
        except Exception as e:
            logger.error(f"Error starting message consumption: {e}")
            self._is_consuming = False
            return False
    
    def stop_receiving(self) -> bool:
        """
        Stop receiving messages.
        
        Returns:
            bool: True if stopped successfully
        """
        if not self._is_consuming:
            logger.warning(f"{self.service_type.capitalize()} service is not consuming messages")
            return False
            
        try:
            self.broker.stop_consuming()
            self._is_consuming = False
            logger.info(f"{self.service_type.capitalize()} service stopped consuming messages")
            return True
        except Exception as e:
            logger.error(f"Error stopping message consumption: {e}")
            return False
    
    def close(self):
        """Close the connection to the messaging service."""
        try:
            self.stop_receiving()
            self.broker.close()
            logger.info(f"{self.service_type.capitalize()} service disconnected from messaging")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")


# Example usage for backend service
if __name__ == "__main__":
    # Create a broker for the backend
    backend_broker = ServiceBroker(service_type="backend")
    
    # Define a message handler
    def handle_message(message):
        print(f"Backend received message: {message}")
        
        # Example of how to respond to different message types
        if message.get("type") == "user_action" and message.get("source_service") == "frontend":
            print("Handling user action from frontend...")
            
            # Send a response back to frontend
            backend_broker.send_message(
                to_service="frontend",
                message_type="notification",
                data={"message": "Action received"}
            )
            
            # Example of interacting with database
            backend_broker.send_message(
                to_service="database",
                message_type="query",
                data={"query": "SELECT * FROM events", "request_id": "123"}
            )
    
    # Start receiving messages
    backend_broker.receive_messages(handle_message)
    
    # Keep the program running
    try:
        print("Backend service running. Press Ctrl+C to exit...")
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        backend_broker.close()