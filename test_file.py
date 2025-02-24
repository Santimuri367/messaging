# messaging_broker.py
import pika
import json
import uuid
from datetime import datetime
import logging
import ssl

class MessageBroker:
    def __init__(self):
        """Initialize the message broker with AWS Amazon MQ credentials."""
        # AWS Amazon MQ connection details
        self.credentials = pika.PlainCredentials(
            username='projectIT',
            password='group4it490section'
        )
        
        # SSL context for secure connection
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')
        
        self.parameters = pika.ConnectionParameters(
            host='b-72533e82-e173-469e-9ef2-432f4fd29309.mq.us-east-2.amazonaws.com',
            port=5671,
            virtual_host='/',
            credentials=self.credentials,
            ssl_options=pika.SSLOptions(ssl_context),
            connection_attempts=3,
            retry_delay=5
        )
        
        # Alternative connection using URL
        self.url = "amqps://projectIT:group4it490section@b-72533e82-e173-469e-9ef2-432f4fd29309.mq.us-east-2.amazonaws.com:5671/"
        
        self.connection = None
        self.channel = None
        self.setup_logging()

    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """Establish connection to AWS Amazon MQ."""
        try:
            # You can use either URL or parameters to connect
            # self.connection = pika.BlockingConnection(pika.URLParameters(self.url))
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            self.logger.info("Connected to AWS Amazon MQ successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to AWS Amazon MQ: {str(e)}")
            raise

    def setup_queue(self, queue_name):
        """Declare a queue."""
        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.logger.info(f"Queue {queue_name} declared successfully")
        except Exception as e:
            self.logger.error(f"Failed to declare queue: {str(e)}")
            raise

    def publish_message(self, queue_name, message):
        """Publish a message to a queue."""
        try:
            message_id = str(uuid.uuid4())
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    correlation_id=message_id,
                    timestamp=int(datetime.now().timestamp()),
                    content_type='application/json'
                ),
                body=json.dumps(message)
            )
            self.logger.info(f"Published message to {queue_name}: {message}")
            return message_id
        except Exception as e:
            self.logger.error(f"Failed to publish message: {str(e)}")
            raise

    def consume_messages(self, queue_name, callback):
        """Set up consumer for specified queue."""
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback
            )
            self.logger.info(f"Started consuming from queue: {queue_name}")
            self.channel.start_consuming()
        except Exception as e:
            self.logger.error(f"Error in message consumption: {str(e)}")
            raise

    def get_message(self, queue_name):
        """Get a single message from the queue."""
        try:
            method_frame, header_frame, body = self.channel.basic_get(queue=queue_name)
            if method_frame:
                message = json.loads(body)
                self.channel.basic_ack(method_frame.delivery_tag)
                self.logger.info(f"Retrieved message from {queue_name}: {message}")
                return message
            return None
        except Exception as e:
            self.logger.error(f"Failed to get message: {str(e)}")
            raise

    def close(self):
        """Close the connection."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.logger.info("Connection closed")
