import pika
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Callable
import logging

class MessageBroker:
    def __init__(self, host: str = 'localhost'):
        """Initialize the message broker with the given host."""
        self.host = host
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.responses = {}
        self.setup_logging()
        
    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def connect(self) -> None:
        """Establish connection to RabbitMQ."""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host)
            )
            self.channel = self.connection.channel()
            self.logger.info(f"Connected to RabbitMQ at {self.host}")
        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            raise

    def setup_queues(self, queues: list) -> None:
        """Declare all necessary queues."""
        for queue in queues:
            self.channel.queue_declare(queue=queue, durable=True)
            self.logger.info(f"Declared queue: {queue}")

    def publish_message(self, queue: str, message: Dict[str, Any], 
                       correlation_id: str = None) -> str:
        """Publish a message to specified queue."""
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
            
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=queue,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    correlation_id=correlation_id,
                    timestamp=int(datetime.now().timestamp()),
                    content_type='application/json'
                ),
                body=json.dumps(message)
            )
            self.logger.info(f"Published message to {queue} with ID: {correlation_id}")
            return correlation_id
        except Exception as e:
            self.logger.error(f"Failed to publish message: {str(e)}")
            raise

    def consume_messages(self, queue: str, callback: Callable) -> None:
        """Set up consumer for specified queue."""
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=queue,
                on_message_callback=callback
            )
            self.logger.info(f"Started consuming from queue: {queue}")
            self.channel.start_consuming()
        except Exception as e:
            self.logger.error(f"Error in message consumption: {str(e)}")
            raise

    def close(self) -> None:
        """Close the connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.logger.info("Connection closed")

# Example consumer implementation
class MessageConsumer:
    def __init__(self, broker: MessageBroker, queue: str):
        """Initialize consumer with broker and queue."""
        self.broker = broker
        self.queue = queue
        self.logger = logging.getLogger(__name__)

    def process_message(self, ch, method, properties, body):
        """Process received message."""
        try:
            message = json.loads(body)
            self.logger.info(f"Processing message: {message}")
            
            # Add your message processing logic here
            result = self.handle_message(message)
            
            # Acknowledge message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            return result
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            # Reject message and requeue
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def handle_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle different types of messages."""
        message_type = message.get('type')
        handlers = {
            'calculation': self._handle_calculation,
            'query': self._handle_query,
            # Add more handlers as needed
        }
        
        handler = handlers.get(message_type)
        if handler:
            return handler(message)
        else:
            raise ValueError(f"Unknown message type: {message_type}")

    def _handle_calculation(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle calculation type messages."""
        numbers = message.get('numbers', [])
        result = sum(numbers)  # Example calculation
        return {
            'task_id': message.get('task_id'),
            'result': result,
            'status': 'completed'
        }

    def _handle_query(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle query type messages."""
        # Implement query handling logic
        pass

    def start_consuming(self):
        """Start consuming messages."""
        self.broker.consume_messages(self.queue, self.process_message)

