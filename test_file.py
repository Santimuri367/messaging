import pika
import sys
import ssl

# AWS Amazon MQ connection details
credentials = pika.PlainCredentials('projectIT', 'group4it490section')

# SSL context for secure connection
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')

parameters = pika.ConnectionParameters(
    host='b-72533e82-e173-469e-9ef2-432f4fd29309.mq.us-east-2.amazonaws.com',
    port=5671,
    virtual_host='/',
    credentials=credentials,
    ssl_options=pika.SSLOptions(ssl_context),
    connection_attempts=3,
    retry_delay=5
)

try:
    # Try to establish connection
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    print("Successfully connected to AWS Amazon MQ!")
    
    # List queues (optional)
    queues = channel.queue_declare(queue='test_queue', durable=True)
    print(f"Queue created/confirmed: test_queue")
    
    # Send a test message
    channel.basic_publish(
        exchange='',
        routing_key='test_queue',
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ),
        body='Hello AWS Amazon MQ!'
    )
    print("Test message sent to test_queue")
    
    connection.close()
    print("Connection closed successfully")
    
except pika.exceptions.AMQPConnectionError as e:
    print(f"Failed to connect to AWS Amazon MQ: {str(e)}")
    sys.exit(1)
except Exception as e:
    print(f"An error occurred: {str(e)}")
    sys.exit(1)
