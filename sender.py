import pika
import json
import uuid
from datetime import datetime

def send_message():
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Declare queues
    channel.queue_declare(queue='task_queue')
    channel.queue_declare(queue='response_queue')
    
    # Generate a unique correlation ID
    correlation_id = str(uuid.uuid4())
    
    # Example message
    message = {
        "task_id": correlation_id,
        "type": "calculation",
        "numbers": [1, 2, 3, 4, 5],
        "timestamp": datetime.now().isoformat()
    }
    
    # Send message
    channel.basic_publish(
        exchange='',
        routing_key='task_queue',
        properties=pika.BasicProperties(
            reply_to='response_queue',
            correlation_id=correlation_id,
        ),
        body=json.dumps(message)
    )
    print(f" [x] Sent task {correlation_id}")
    
    # Wait for response
    def callback(ch, method, props, body):
        if props.correlation_id == correlation_id:
            response = json.loads(body)
            print(f" [x] Received response: {response}")
            connection.close()
    
    channel.basic_consume(
        queue='response_queue',
        on_message_callback=callback,
        auto_ack=True
    )
    
    print(" [*] Waiting for response. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    send_message()
