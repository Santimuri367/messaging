import pika
import sys

# CloudAMQP connection details
credentials = pika.PlainCredentials('cxknnjta', 'e2T8R0v6YhAwsvHC6dpOQAVO4qq92tTB')
parameters = pika.ConnectionParameters(
    host='shark-01.rmq.cloudamqp.com',
    port=5672,
    virtual_host='cxknnjta',
    credentials=credentials
)

try:
    # Try to establish connection
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    print("Successfully connected to CloudAMQP!")
    
    # List queues (optional)
    queues = channel.queue_declare(queue='', exclusive=True)
    print(f"Queue created: {queues.method.queue}")
    
    connection.close()
    print("Connection closed successfully")
    
except pika.exceptions.AMQPConnectionError as e:
    print(f"Failed to connect to CloudAMQP: {str(e)}")
    sys.exit(1)
