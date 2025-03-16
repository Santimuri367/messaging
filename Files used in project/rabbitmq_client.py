#!/usr/bin/env python3
"""
RabbitMQ Client - A helper module for backend to read from and write to RabbitMQ queues
"""

import pika
import json
import time
import threading
import logging
import requests
from typing import Dict, Any, Callable, Optional, List, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('rabbitmq_client')

class RabbitMQClient:
    """
    Client for interacting with RabbitMQ messaging service.
    Provides methods to publish messages to queues and consume messages from queues.
    """
    
    def __init__(
        self, 
        host: str = '10.147.20.12',
        port: int = 5672,
        username: str = 'guest',
        password: str = 'guest',
        vhost: str = '/',
        api_port: int = 6004,
        heartbeat: int = 600,
        connection_attempts: int = 3,
        retry_delay: int = 5
    ):
        """
        Initialize the RabbitMQ client.
        
        Args:
            host: RabbitMQ server hostname or IP
            port: RabbitMQ server port (default: 5672)
            username: RabbitMQ username
            password: RabbitMQ password
            vhost: RabbitMQ virtual host
            api_port: Port for the RabbitMQ control API
            heartbeat: How often to send heartbeat to server (in seconds)
            connection_attempts: Number of connection attempts before giving up
            retry_delay: Delay between connection attempts (in seconds)
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.api_port = api_port
        self.heartbeat = heartbeat
        self.connection_attempts = connection_attempts
        self.retry_delay = retry_delay
        
        # Store connections and channels
        self._connection = None
        self._channel = None
        self._consuming = False
        self._consumer_tag = None
        self._closing = False
        
        # Store consumer threads
        self._consumer_threads: Dict[str, threading.Thread] = {}
        
        # Store callbacks
        self._callbacks: Dict[str, Callable] = {}

    def _get_connection_parameters(self) -> pika.ConnectionParameters:
        """
        Create connection parameters for RabbitMQ.
        
        Returns:
            pika.ConnectionParameters
        """
        credentials = pika.PlainCredentials(self.username, self.password)
        return pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credentials,
            heartbeat=self.heartbeat,
            connection_attempts=self.connection_attempts,
            retry_delay=self.retry_delay
        )
    
    def ensure_service_running(self) -> bool:
        """
        Ensure RabbitMQ service is running using the control API.
        
        Returns:
            bool: True if service is running or was started successfully
        """
        try:
            # Check status
            response = requests.get(f"http://{self.host}:{self.api_port}/status")
            status_data = response.json()
            
            if status_data.get("status") == "running":
                logger.info("RabbitMQ service is already running")
                return True
            
            # Try to start the service
            response = requests.get(f"http://{self.host}:{self.api_port}/start")
            start_data = response.json()
            
            logger.info(f"Started RabbitMQ service: {start_data}")
            return True
            
        except Exception as e:
            logger.error(f"Error checking/starting RabbitMQ service: {e}")
            return False
    
    def connect(self) -> bool:
        """
        Connect to RabbitMQ server.
        
        Returns:
            bool: True if connection successful
        """
        if self._connection and self._connection.is_open:
            logger.info("Already connected to RabbitMQ")
            return True
            
        try:
            # First ensure the service is running
            if not self.ensure_service_running():
                logger.error("Could not ensure RabbitMQ service is running")
                return False
                
            # Connect to RabbitMQ
            parameters = self._get_connection_parameters()
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            
            logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            self._connection = None
            self._channel = None
            return False
    
    def close(self) -> None:
        """Close the connection to RabbitMQ."""
        self._closing = True
        
        # Stop all consumers
        for queue in list(self._consumer_threads.keys()):
            self.stop_consuming(queue)
            
        # Close connection
        if self._connection and self._connection.is_open:
            logger.info("Closing connection to RabbitMQ")
            self._connection.close()
            
        self._connection = None
        self._channel = None
        self._closing = False
    
    def declare_queue(self, queue_name: str, durable: bool = True) -> bool:
        """
        Declare a queue in RabbitMQ.
        
        Args:
            queue_name: Name of the queue
            durable: Whether the queue survives broker restarts
            
        Returns:
            bool: True if successful
        """
        if not self.connect():
            return False
            
        try:
            self._channel.queue_declare(queue=queue_name, durable=durable)
            logger.info(f"Declared queue: {queue_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to declare queue {queue_name}: {e}")
            return False
    
    def publish(
        self, 
        queue_name: str, 
        message: Union[Dict[str, Any], List[Any], str],
        exchange: str = '',
        durable: bool = True,
        content_type: str = 'application/json'
    ) -> bool:
        """
        Publish a message to a queue.
        
        Args:
            queue_name: Name of the queue
            message: Message to publish (dict or string)
            exchange: Exchange to use (default is direct to queue)
            durable: Whether the queue should be durable
            content_type: Content type of the message
            
        Returns:
            bool: True if successful
        """
        if not self.connect():
            return False
            
        try:
            # Declare the queue if needed
            self.declare_queue(queue_name, durable)
            
            # Convert dict to JSON string if needed
            if isinstance(message, (dict, list)):
                message_body = json.dumps(message)
            else:
                message_body = str(message)
            
            # Publish the message
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2 if durable else 1,  # 2 = persistent
                    content_type=content_type
                )
            )
            
            logger.info(f"Published message to queue {queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish message to queue {queue_name}: {e}")
            return False
    
    def _consume_messages(
        self, 
        queue_name: str, 
        callback: Callable, 
        auto_ack: bool
    ) -> None:
        """
        Internal function to consume messages in a loop.
        
        Args:
            queue_name: Queue to consume from
            callback: Function to call for each message
            auto_ack: Whether to auto-acknowledge messages
        """
        if not self.connect():
            return
            
        try:
            # Store the callback
            self._callbacks[queue_name] = callback
            
            # Define message handler wrapper
            def message_handler(ch, method, properties, body):
                try:
                    # Parse JSON if content type is application/json
                    if properties.content_type == 'application/json':
                        try:
                            data = json.loads(body)
                        except json.JSONDecodeError:
                            data = body.decode('utf-8')
                    else:
                        data = body.decode('utf-8')
                    
                    # Call the user-provided callback
                    callback(data)
                    
                    # Acknowledge the message if not auto-ack
                    if not auto_ack:
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        
                except Exception as e:
                    logger.error(f"Error processing message from {queue_name}: {e}")
                    # Reject the message in case of error
                    if not auto_ack:
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            
            # Set up consumer
            self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=message_handler,
                auto_ack=auto_ack
            )
            
            logger.info(f"Started consuming from queue: {queue_name}")
            
            # Start consuming (will block until channel or connection is closed)
            self._channel.start_consuming()
            
        except Exception as e:
            logger.error(f"Error in consumer for queue {queue_name}: {e}")
        finally:
            # Clean up
            if queue_name in self._callbacks:
                del self._callbacks[queue_name]
                
            if queue_name in self._consumer_threads:
                del self._consumer_threads[queue_name]
                
            logger.info(f"Stopped consuming from queue: {queue_name}")
    
    def consume(
        self, 
        queue_name: str, 
        callback: Callable[[Any], None],
        auto_ack: bool = False,
        durable: bool = True
    ) -> bool:
        """
        Start consuming messages from a queue in a background thread.
        
        Args:
            queue_name: Queue to consume from
            callback: Function to call for each message
            auto_ack: Whether to auto-acknowledge messages
            durable: Whether the queue should be durable
            
        Returns:
            bool: True if consumer started successfully
        """
        # Stop any existing consumer for this queue
        if queue_name in self._consumer_threads:
            self.stop_consuming(queue_name)
        
        # Declare the queue if needed
        if not self.declare_queue(queue_name, durable):
            return False
        
        try:
            # Start a new thread for consuming
            thread = threading.Thread(
                target=self._consume_messages,
                args=(queue_name, callback, auto_ack),
                daemon=True
            )
            thread.start()
            
            # Store the thread
            self._consumer_threads[queue_name] = thread
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start consumer for queue {queue_name}: {e}")
            return False
    
    def stop_consuming(self, queue_name: str) -> bool:
        """
        Stop consuming messages from a queue.
        
        Args:
            queue_name: Queue to stop consuming from
            
        Returns:
            bool: True if successful
        """
        if queue_name not in self._consumer_threads:
            logger.warning(f"No active consumer for queue {queue_name}")
            return False
            
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
                
            thread = self._consumer_threads[queue_name]
            if thread.is_alive():
                thread.join(timeout=2.0)
                
            # Remove from our dictionaries
            del self._consumer_threads[queue_name]
            if queue_name in self._callbacks:
                del self._callbacks[queue_name]
                
            logger.info(f"Stopped consuming from queue: {queue_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error stopping consumer for queue {queue_name}: {e}")
            return False
    
    def get_message(self, queue_name: str, auto_ack: bool = False) -> Optional[Any]:
        """
        Get a single message from a queue (non-blocking).
        
        Args:
            queue_name: Queue to get message from
            auto_ack: Whether to auto-acknowledge the message
            
        Returns:
            Any: Message data or None if no message available
        """
        if not self.connect():
            return None
            
        try:
            # Attempt to get a message
            method_frame, header_frame, body = self._channel.basic_get(
                queue=queue_name,
                auto_ack=auto_ack
            )
            
            if method_frame:
                # Parse JSON if content type is application/json
                if header_frame.content_type == 'application/json':
                    try:
                        data = json.loads(body)
                    except json.JSONDecodeError:
                        data = body.decode('utf-8')
                else:
                    data = body.decode('utf-8')
                
                # Acknowledge the message if not auto-ack
                if not auto_ack:
                    self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    
                return data
            else:
                # No message available
                return None
                
        except Exception as e:
            logger.error(f"Failed to get message from queue {queue_name}: {e}")
            return None
    
    def purge_queue(self, queue_name: str) -> bool:
        """
        Delete all messages from a queue.
        
        Args:
            queue_name: Queue to purge
            
        Returns:
            bool: True if successful
        """
        if not self.connect():
            return False
            
        try:
            self._channel.queue_purge(queue=queue_name)
            logger.info(f"Purged queue: {queue_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to purge queue {queue_name}: {e}")
            return False
    
    def delete_queue(self, queue_name: str) -> bool:
        """
        Delete a queue.
        
        Args:
            queue_name: Queue to delete
            
        Returns:
            bool: True if successful
        """
        # Stop consuming if needed
        if queue_name in self._consumer_threads:
            self.stop_consuming(queue_name)
            
        if not self.connect():
            return False
            
        try:
            self._channel.queue_delete(queue=queue_name)
            logger.info(f"Deleted queue: {queue_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete queue {queue_name}: {e}")
            return False


# Example usage
if __name__ == "__main__":
    # Create a client
    client = RabbitMQClient(
        host="10.147.20.12",
        port=5672,
        username="guest",
        password="guest",
        vhost="/"
    )
    
    # Example: Publish a message
    client.publish(
        queue_name="test_queue",
        message={"event": "user_signup", "data": {"username": "john_doe", "email": "john@example.com"}}
    )
    
    # Example: Consume messages
    def process_message(message):
        print(f"Received message: {message}")
    
    # Start consuming in background
    client.consume(
        queue_name="test_queue",
        callback=process_message
    )
    
    # Or get a single message
    message = client.get_message("test_queue")
    if message:
        print(f"Got message: {message}")
    
    # Keep the program running to receive messages (for the example)
    try:
        print("Waiting for messages (press Ctrl+C to exit)...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        # Clean up
        client.close()