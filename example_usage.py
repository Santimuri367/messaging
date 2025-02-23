# example_usage.py (How to use the system)
def main():
    # Replace with your RabbitMQ VM's IP
    rabbitmq_host = '192.168.x.x'  
    
    # Initialize broker
    broker = MessageBroker(host=rabbitmq_host)
    if not broker.connect():
        print("Failed to connect to RabbitMQ")
        return
    
    # Set up queues
    queues = ['task_queue', 'response_queue']
    broker.setup_queues(queues)
    
    # Example: Send a message
    message = {
        "type": "calculation",
        "numbers": [1, 2, 3, 4, 5],
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        correlation_id = broker.publish_message('task_queue', message)
        print(f"Sent message with ID: {correlation_id}")
        
        # Set up backend handler
        backend = BackendHandler(broker)
        print("Waiting for messages. Press CTRL+C to exit.")
        broker.consume_messages('task_queue', backend.process_message)
        
    except KeyboardInterrupt:
        print("Shutting down...")
        broker.close()

if __name__ == "__main__":
    main()
