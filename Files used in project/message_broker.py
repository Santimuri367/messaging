# message_broker.py
import pika
import json
from typing import Dict

class MessageBroker:
    def __init__(self):
        # Connection parameters with credentials
        self.credentials = pika.PlainCredentials('myuser', 'mypassword')
        self.parameters = pika.ConnectionParameters(
            host='192.168.x.x',  # Replace with RabbitMQ VM's IP
            port=5672,
            virtual_host='/',
            credentials=self.credentials
        )
        
    def connect(self):
        try:
            self.connection = pika.BlockingConnection(self.parameters)
            self.channel = self.connection.channel()
            print("Successfully connected to RabbitMQ")
            return True
        except Exception as e:
            print(f"Failed to connect: {e}")
            return False

    def setup_queues(self):
        # Declare queues with durability
        self.channel.queue_declare(queue='task_queue', durable=True)
        self.channel.queue_declare(queue='response_queue', durable=True)

# Test connection
def test_connection():
    broker = MessageBroker()
    if broker.connect():
        print("Connection test successful!")
        broker.setup_queues()
        print("Queues set up successfully!")
    else:
        print("Connection test failed!")

if __name__ == "__main__":
    test_connection()