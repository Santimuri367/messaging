# example_usage.py
from messaging_broker import MessageBroker, MessageConsumer

def main():
    # Initialize broker
    broker = MessageBroker(host='localhost')
    broker.connect()
    
    # Set up queues
    queues = ['task_queue', 'response_queue']
    broker.setup_queues(queues)
    
    # Example: Send a message
    message = {
        "type": "calculation",
        "numbers": [1, 2, 3, 4, 5],
        "timestamp": datetime.now().isoformat()
    }
    
    correlation_id = broker.publish_message('task_queue', message)
    print(f"Sent message with ID: {correlation_id}")
    
    # Example: Set up a consumer
    consumer = MessageConsumer(broker, 'task_queue')
    consumer.start_consuming()

if __name__ == "__main__":
    main()