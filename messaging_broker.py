# message_broker.py (Messaging Part)
import pika
import json
import uuid
from datetime import datetime
import logging

class MessageBroker:
    def __init__(self, host, username='myuser', password='mypassword'):
        self.credentials = pika.PlainCredentials(username, password)
        self.parameters = pika.ConnectionParameters(
            host=host,
            port=5672,
            virtual_host='/',
            credentials=self.credentials
        )
        self.setup_logging()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def connect(self):
        try:
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            self.logger.info(f"Connected to RabbitMQ at {self.parameters.host}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            return False

    def setup_queues(self, queues):
        for queue in queues:
            self.channel.queue_declare(queue=queue, durable=True)
            self.logger.info(f"Declared queue: {queue}")

    def publish_message(self, queue, message):
        try:
            correlation_id = str(uuid.uuid4())
            self.channel.basic_publish(
                exchange='',
                routing_key=queue,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    correlation_id=correlation_id,
                    timestamp=int(datetime.now().timestamp())
                ),
                body=json.dumps(message)
            )
            self.logger.info(f"Published message to {queue} with ID: {correlation_id}")
            return correlation_id
        except Exception as e:
            self.logger.error(f"Failed to publish message: {e}")
            raise

    def consume_messages(self, queue, callback):
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=queue,
                on_message_callback=callback,
                auto_ack=True
            )
            self.logger.info(f"Started consuming from queue: {queue}")
            self.channel.start_consuming()
        except Exception as e:
            self.logger.error(f"Error in message consumption: {e}")
            raise

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.logger.info("Connection closed")
