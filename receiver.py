import pika
import json
import time

def callback(ch, method, properties, body):
    print(" [x] Received task")
    # Parse the message
    message = json.loads(body)
    
    # Simulate some work
    time.sleep(2)
    
    # Process the numbers (example: calculate sum)
    numbers = message.get('numbers', [])
    result = sum(numbers)
    
    # Prepare response
    response = {
        "task_id": message['task_id'],
        "result": result,
        "status": "completed"
    }
    
    # Send response back
    ch.basic_publish(
        exchange='',
        routing_key=properties.reply_to,
        properties=pika.BasicProperties(
            correlation_id=properties.correlation_id
        ),
        body=json.dumps(response)
    )
    print(f" [x] Sent result: {result}")

def main():
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    # Declare queue
    channel.queue_declare(queue='task_queue')
    
    # Set up consumer
    channel.basic_consume(
        queue='task_queue',
        on_message_callback=callback,
        auto_ack=True
    )
    
    print(" [*] Waiting for tasks. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()