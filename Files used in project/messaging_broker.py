# messaging_broker.py
import pika
import json
import uuid
import os
from datetime import datetime
import logging

# Ensure logs directory exists
log_dir = os.path.expanduser("~/logs")
os.makedirs(log_dir, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, "messaging_broker.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('messaging_broker')

# VM IP addresses configuration
VM_IPS = {
    "database": "10.147.20.166",
    "frontend": "10.147.20.38",
    "messaging": "10.147.20.12",
    "backend": "10.147.20.113"
}

def setup_rabbitmq_queues():
    """
    Set up the initial RabbitMQ queues and exchanges
    """
    # Setup logging for this function
    setup_logger = logging.getLogger("rabbitmq_setup")
    
    try:
        # Connect to RabbitMQ - Use messaging IP instead of localhost
        credentials = pika.PlainCredentials(
            username='guest',
            password='guest'
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=VM_IPS['messaging'], 
                port=5672,
                credentials=credentials
            )
        )
        channel = connection.channel()
        
        # Declare queues with durable=True to match MessageBroker class
        channel.queue_declare(queue='frontend_queue', durable=True)
        channel.queue_declare(queue='backend_queue', durable=True)
        channel.queue_declare(queue='database_queue', durable=True)
        
        # Fixed exchange name to be consistent - using SERVICE_EXCHANGE throughout
        channel.exchange_declare(exchange='service_exchange', exchange_type='topic', durable=True)
        
        # Bind queues to exchanges if needed
        channel.queue_bind(
            exchange='service_exchange',
            queue='frontend_queue',
            routing_key='*.to.frontend'
        )
        channel.queue_bind(
            exchange='service_exchange',
            queue='backend_queue',
            routing_key='*.to.backend'
        )
        channel.queue_bind(
            exchange='service_exchange',
            queue='database_queue',
            routing_key='*.to.database'
        )
        
        setup_logger.info("Successfully set up RabbitMQ queues and exchanges")
        print("Successfully set up RabbitMQ queues and exchanges")
        connection.close()
        return True
    except Exception as e:
        setup_logger.error(f"Error setting up RabbitMQ queues: {e}")
        print(f"Error setting up RabbitMQ queues: {e}")
        return False

class MessageBroker:
    """
    A message broker for handling communication between services (frontend, backend, database) 
    via RabbitMQ.
    """
    
    # Queue constants
    FRONTEND_QUEUE = "frontend_queue"
    BACKEND_QUEUE = "backend_queue"
    DATABASE_QUEUE = "database_queue"
    
    # Exchange names - Fixed to be consistent with setup function
    SERVICE_EXCHANGE = "service_exchange"
    
    # Routing key patterns
    BACKEND_TO_FRONTEND = "backend.to.frontend"
    BACKEND_TO_DATABASE = "backend.to.database"
    FRONTEND_TO_BACKEND = "frontend.to.backend"
    FRONTEND_TO_DATABASE = "frontend.to.database"
    DATABASE_TO_BACKEND = "database.to.backend"
    DATABASE_TO_FRONTEND = "database.to.frontend"
    
    def __init__(self, service_name):
        """
        Initialize the message broker with proper RabbitMQ credentials.
        
        Args:
            service_name (str): The name of the service using this broker 
                               (e.g., "frontend", "backend", "database")
        """
        # Store the service identity
        self.service_name = service_name
        
        # Use your existing RabbitMQ guest user
        self.credentials = pika.PlainCredentials(
            username='guest',
            password='guest'
        )
        self.parameters = pika.ConnectionParameters(
            host=VM_IPS['messaging'],  # Using the messaging service IP
            port=5672,
            virtual_host='/',
            credentials=self.credentials,
            heartbeat=600,
            connection_attempts=3,
            retry_delay=5
        )
        
        self.connection = None
        self.channel = None
        self.setup_logging()
        
        # Consumer tags for each queue
        self.consumer_tags = {}

    def setup_logging(self):
        """Set up logging configuration."""
        # Use the existing logger with service_name suffix
        self.logger = logging.getLogger(f"messaging_broker.{self.service_name}")

    def connect(self):
        """Establish connection to RabbitMQ and set up exchanges and queues."""
        try:
            if self.connection and self.connection.is_open:
                self.logger.info(f"Already connected to RabbitMQ from {self.service_name}")
                return True
                
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            
            # Declare exchange and queues
            self.setup_exchange(self.SERVICE_EXCHANGE, "topic")
            
            # Declare all standard queues
            self.setup_queue(self.FRONTEND_QUEUE)
            self.setup_queue(self.BACKEND_QUEUE)
            self.setup_queue(self.DATABASE_QUEUE)
            
            # Bind queues to exchange with routing patterns
            self.bind_queue_to_exchange(self.FRONTEND_QUEUE, self.SERVICE_EXCHANGE, "*.to.frontend")
            self.bind_queue_to_exchange(self.BACKEND_QUEUE, self.SERVICE_EXCHANGE, "*.to.backend")
            self.bind_queue_to_exchange(self.DATABASE_QUEUE, self.SERVICE_EXCHANGE, "*.to.database")
            
            self.logger.info(f"Connected to RabbitMQ successfully from {self.service_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ from {self.service_name}: {str(e)}")
            if self.connection and not self.connection.is_closed:
                try:
                    self.connection.close()
                except:
                    pass
            self.connection = None
            self.channel = None
            return False

    def setup_queue(self, queue_name):
        """
        Declare a queue.
        
        Args:
            queue_name (str): Name of the queue to declare
        """
        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.logger.info(f"Queue {queue_name} declared successfully by {self.service_name}")
        except Exception as e:
            self.logger.error(f"Failed to declare queue by {self.service_name}: {str(e)}")
            raise

    def setup_exchange(self, exchange_name, exchange_type="direct"):
        """
        Declare an exchange.
        
        Args:
            exchange_name (str): Name of the exchange to declare
            exchange_type (str): Type of exchange (direct, topic, fanout, headers)
        """
        try:
            self.channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=True
            )
            self.logger.info(f"Exchange {exchange_name} declared successfully by {self.service_name}")
        except Exception as e:
            self.logger.error(f"Failed to declare exchange by {self.service_name}: {str(e)}")
            raise

    def bind_queue_to_exchange(self, queue_name, exchange_name, routing_key):
        """
        Bind a queue to an exchange with a routing key.
        
        Args:
            queue_name (str): Name of the queue
            exchange_name (str): Name of the exchange
            routing_key (str): Routing key pattern for binding
        """
        try:
            self.channel.queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key
            )
            self.logger.info(f"Queue {queue_name} bound to exchange {exchange_name} with routing key {routing_key} by {self.service_name}")
        except Exception as e:
            self.logger.error(f"Failed to bind queue to exchange by {self.service_name}: {str(e)}")
            raise

    def send_to_frontend(self, message):
        """
        Send a message from the current service to the frontend.
        
        Args:
            message (dict): Message to send to the frontend
        
        Returns:
            str: Message ID
        """
        routing_key = f"{self.service_name}.to.frontend"
        return self._send_targeted_message(message, routing_key)

    def send_to_backend(self, message):
        """
        Send a message from the current service to the backend.
        
        Args:
            message (dict): Message to send to the backend
        
        Returns:
            str: Message ID
        """
        routing_key = f"{self.service_name}.to.backend"
        return self._send_targeted_message(message, routing_key)

    def send_to_database(self, message):
        """
        Send a message from the current service to the database.
        
        Args:
            message (dict): Message to send to the database
        
        Returns:
            str: Message ID
        """
        routing_key = f"{self.service_name}.to.database"
        return self._send_targeted_message(message, routing_key)

    def _send_targeted_message(self, message, routing_key):
        """
        Internal method to send a targeted message via the service exchange.
        
        Args:
            message (dict): Message to send
            routing_key (str): Routing key for delivery
        
        Returns:
            str: Message ID
        """
        try:
            # Add source and target info to message
            source, _, target = routing_key.split('.')
            
            if isinstance(message, dict):
                message['source_service'] = self.service_name
                message['target_service'] = target
                message['timestamp'] = datetime.now().isoformat()
            
            message_id = str(uuid.uuid4())
            
            self.channel.basic_publish(
                exchange=self.SERVICE_EXCHANGE,
                routing_key=routing_key,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    correlation_id=message_id,
                    timestamp=int(datetime.now().timestamp()),
                    content_type='application/json',
                    headers={
                        'source_service': self.service_name,
                        'target_service': target
                    }
                ),
                body=json.dumps(message)
            )
            
            self.logger.info(f"Published message from {self.service_name} to {target} via routing key {routing_key}")
            return message_id
        except Exception as e:
            self.logger.error(f"Failed to publish message from {self.service_name}: {str(e)}")
            raise

    def start_consuming(self):
        """
        Start consuming messages intended for this service based on service_name.
        This method will consume from the appropriate queue based on the service name.
        """
        try:
            # Select the appropriate queue based on service name
            if self.service_name == "frontend":
                queue_name = self.FRONTEND_QUEUE
            elif self.service_name == "backend":
                queue_name = self.BACKEND_QUEUE
            elif self.service_name == "database":
                queue_name = self.DATABASE_QUEUE
            else:
                raise ValueError(f"Unknown service name: {self.service_name}")
            
            # Standard callback to log message receipt and acknowledge
            def default_callback(ch, method, properties, body):
                source = properties.headers.get('source_service', 'unknown') if properties.headers else 'unknown'
                message = json.loads(body)
                self.logger.info(f"{self.service_name} received message from {source}: {message}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            
            self.channel.basic_qos(prefetch_count=1)
            consumer_tag = self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=default_callback
            )
            
            # Store consumer tag for later cancellation
            self.consumer_tags[queue_name] = consumer_tag
            
            self.logger.info(f"Started consuming from queue {queue_name} by {self.service_name}")
            self.channel.start_consuming()
        except Exception as e:
            self.logger.error(f"Error in message consumption by {self.service_name}: {str(e)}")
            raise

    def consume_with_callback(self, callback):
        """
        Start consuming messages with a custom callback function.
        """
        try:
            # Select the appropriate queue based on service name
            if self.service_name == "frontend":
                queue_name = self.FRONTEND_QUEUE
            elif self.service_name == "backend":
                queue_name = self.BACKEND_QUEUE
            elif self.service_name == "database":
                queue_name = self.DATABASE_QUEUE
            else:
                raise ValueError(f"Unknown service name: {self.service_name}")
        
            # Check if channel is established
            if not self.channel:
                self.connect()  # Try reconnecting
            if not self.channel:
                self.logger.error("Cannot consume messages - no channel available")
                return
                
            def callback_wrapper(ch, method, properties, body):
                try:
                    source = properties.headers.get('source_service', 'unknown') if properties.headers else 'unknown'
                    self.logger.info(f"{self.service_name} received message from {source}")
                    callback(ch, method, properties, body)
                except Exception as e:
                    self.logger.error(f"Error in callback: {e}")
                    # Still acknowledge the message to prevent it from being requeued indefinitely
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            
            self.channel.basic_qos(prefetch_count=1)
            consumer_tag = self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback_wrapper
            )
            
            # Store consumer tag for later cancellation
            self.consumer_tags[queue_name] = consumer_tag
            
            self.logger.info(f"Started consuming from queue {queue_name} with custom callback by {self.service_name}")
            
            # Wrap consume in a try/except to handle connection issues
            try:
                self.channel.start_consuming()
            except pika.exceptions.StreamLostError as e:
                self.logger.warning(f"Connection lost during consumption: {e}")
                self.connection = None
                self.channel = None
                # Give it a short delay before returning to allow reconnection
                import time
                time.sleep(1)
                
        except Exception as e:
            self.logger.error(f"Error in message consumption by {self.service_name}: {str(e)}")

    def stop_consuming(self, queue_name=None):
        """
        Stop consuming messages from a specific queue or all queues.
        
        Args:
            queue_name (str, optional): Name of queue to stop consuming from, or None for all queues
        """
        try:
            if queue_name:
                if queue_name in self.consumer_tags:
                    self.channel.basic_cancel(self.consumer_tags[queue_name])
                    del self.consumer_tags[queue_name]
                    self.logger.info(f"Stopped consuming from queue {queue_name}")
            else:
                # Stop all consumers
                for queue, tag in self.consumer_tags.items():
                    self.channel.basic_cancel(tag)
                    self.logger.info(f"Stopped consuming from queue {queue}")
                self.consumer_tags.clear()
        except Exception as e:
            self.logger.error(f"Error stopping consumer: {str(e)}")
            raise

    def get_next_message(self):
        """
        Get the next message intended for this service without continuous consuming.
        
        Returns:
            dict or None: The next message, or None if no message is available
        """
        try:
            # Select the appropriate queue based on service name
            if self.service_name == "frontend":
                queue_name = self.FRONTEND_QUEUE
            elif self.service_name == "backend":
                queue_name = self.BACKEND_QUEUE
            elif self.service_name == "database":
                queue_name = self.DATABASE_QUEUE
            else:
                raise ValueError(f"Unknown service name: {self.service_name}")
            
            # Get a single message
            method_frame, properties, body = self.channel.basic_get(queue=queue_name, auto_ack=True)
            if method_frame:
                message = json.loads(body)
                source = properties.headers.get('source_service', 'unknown') if properties.headers else 'unknown'
                self.logger.info(f"{self.service_name} retrieved message from {source} via queue {queue_name}")
                return message
            return None
        except Exception as e:
            self.logger.error(f"Failed to get message by {self.service_name}: {str(e)}")
            raise

    def close(self):
        """Close the connection."""
        try:
            self.stop_consuming()  # Stop all consumers
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                self.logger.info(f"Connection closed by {self.service_name}")
        except Exception as e:
            self.logger.error(f"Error closing connection: {str(e)}")


# Usage examples

def backend_example():
    """Example of how the backend would use this message broker."""
    # Create and connect the broker
    broker = MessageBroker(service_name="backend")
    broker.connect()
    
    # Example: Send message to frontend
    broker.send_to_frontend({
        "type": "event_notification",
        "data": {
            "event_id": "evt-123",
            "event_name": "New Concert Added"
        }
    })
    
    # Example: Send message to database
    broker.send_to_database({
        "type": "write_request",
        "operation": "insert",
        "table": "events",
        "data": {
            "id": "evt-123",
            "name": "Jazz Festival",
            "date": "2025-05-15"
        }
    })
    
    # Example: Process incoming messages with custom callback
    def process_message(ch, method, properties, body):
        message = json.loads(body)
        print(f"Backend processing message: {message}")
        # Do something with the message
        
        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    
    # Start consuming messages (this will block)
    broker.consume_with_callback(process_message)
    
    # Cleanup
    broker.close()

def frontend_example():
    """Example of how the frontend would use this message broker."""
    # Create and connect the broker
    broker = MessageBroker(service_name="frontend")
    broker.connect()
    
    # Example: Send message to backend
    broker.send_to_backend({
        "type": "user_action",
        "action": "search",
        "parameters": {
            "artist": "Miles Davis",
            "date_range": {"start": "2025-03-01", "end": "2025-04-30"}
        }
    })
    
    # Start consuming messages (this will block)
    broker.start_consuming()
    
    # Cleanup
    broker.close()

def database_example():
    """Example of how the database would use this message broker."""
    # Create and connect the broker
    broker = MessageBroker(service_name="database")
    broker.connect()
    
    # Example: Send message to backend
    broker.send_to_backend({
        "type": "search_results",
        "request_id": "search-123",
        "results": [
            {"id": "evt-1", "name": "Jazz Night", "artist": "Miles Davis"},
            {"id": "evt-2", "name": "Blues Festival", "artist": "B.B. King"}
        ],
        "count": 2
    })
    
    # Example: Get a single message without continuous consuming
    message = broker.get_next_message()
    if message:
        print(f"Database processing message: {message}")
        # Process the message
    
    # Cleanup
    broker.close()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'setup':
        # Run setup if requested
        setup_rabbitmq_queues()
    else:
        # Example usage
        backend_example()
