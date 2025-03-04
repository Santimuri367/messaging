#!/usr/bin/env python3
"""
RabbitMQ Test Script - Tests your RabbitMQ installation by sending and receiving messages
"""

import pika
import time
import sys
import argparse

def test_rabbitmq(host='localhost', port=5672, username='guest', password='guest', vhost='/', exchange='', queue='test_queue'):
    """
    Test RabbitMQ by sending and receiving a message.
    """
    print(f"Connecting to RabbitMQ on {host}:{port} with user '{username}' and vhost '{vhost}'...")
    
    # Create connection credentials
    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        credentials=credentials
    )
    
    try:
        # Establish connection
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        print("✅ Connection established successfully")
    except Exception as e:
        print(f"❌ Failed to connect to RabbitMQ: {e}")
        return False
    
    try:
        # Declare a queue
        channel.queue_declare(queue=queue, durable=True)
        print(f"✅ Queue '{queue}' declared successfully")
    except Exception as e:
        print(f"❌ Failed to declare queue: {e}")
        connection.close()
        return False
    
    # Define a callback function for receiving messages
    def callback(ch, method, properties, body):
        print(f"✅ Received message: {body.decode()}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    # Set up consumer
    try:
        channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=False)
        print(f"✅ Consumer set up successfully for queue '{queue}'")
    except Exception as e:
        print(f"❌ Failed to set up consumer: {e}")
        connection.close()
        return False
    
    # Send a test message
    try:
        message = f"Test message sent at {time.strftime('%Y-%m-%d %H:%M:%S')}"
        channel.basic_publish(
            exchange=exchange,
            routing_key=queue,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
        )
        print(f"✅ Sent message: {message}")
    except Exception as e:
        print(f"❌ Failed to send message: {e}")
        connection.close()
        return False
    
    print("Waiting for messages. Press CTRL+C to exit.")
    
    try:
        # Start consuming (will block)
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"❌ Error during message consumption: {e}")
        connection.close()
        return False
    finally:
        # Cleanup
        try:
            channel.queue_delete(queue=queue)
            print(f"✅ Cleaned up test queue '{queue}'")
            connection.close()
            print("✅ Connection closed")
        except Exception as e:
            print(f"❌ Error during cleanup: {e}")
            return False
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Test RabbitMQ installation by sending and receiving messages")
    parser.add_argument('--host', default='localhost', help='RabbitMQ server hostname (default: localhost)')
    parser.add_argument('--port', type=int, default=5672, help='RabbitMQ server port (default: 5672)')
    parser.add_argument('--user', default='guest', help='RabbitMQ username (default: guest)')
    parser.add_argument('--password', default='guest', help='RabbitMQ password (default: guest)')
    parser.add_argument('--vhost', default='/', help='RabbitMQ virtual host (default: /)')
    parser.add_argument('--queue', default='test_queue', help='Queue name for testing (default: test_queue)')
    
    args = parser.parse_args()
    
    success = test_rabbitmq(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        vhost=args.vhost,
        queue=args.queue
    )
    
    if success:
        print("\n✅ RabbitMQ test completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ RabbitMQ test failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()
