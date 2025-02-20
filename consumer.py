import pika  # type: ignore
import sys
import os
import json
from datetime import datetime

def main():
    try:
        # Establish connection to RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        
        # Declare queues
        channel.queue_declare(queue='event-notifications')
        channel.queue_declare(queue='artist-updates')
        channel.queue_declare(queue='user-preferences')
        
        # Event notifications handler
        def event_notification_callback(ch, method, properties, body):
            try:
                message = json.loads(body)
                print(f"Received event notification:")
                print(f"Event: {message.get('event_name')}")
                print(f"Artist: {message.get('artist_name')}")
                print(f"Location: {message.get('place_name')}")
                print(f"Genre: {message.get('genre')}")
                
                # Send acknowledgment back to backend
                response = {
                    "status": "processed",
                    "timestamp": datetime.now().isoformat(),
                    "event_id": message.get('event_id')
                }
                
                # Use reply_to queue if specified
                if properties.reply_to:
                    ch.basic_publish(
                        exchange='',
                        routing_key=properties.reply_to,
                        properties=pika.BasicProperties(
                            correlation_id=properties.correlation_id
                        ),
                        body=json.dumps(response)
                    )
                
            except json.JSONDecodeError as e:
                print(f"Failed to decode message: {e}")
                print(f"Raw message: {body}")
        
        # Artist updates handler
        def artist_update_callback(ch, method, properties, body):
            try:
                message = json.loads(body)
                print(f"Received artist update:")
                print(f"Artist: {message.get('artist_name')}")
                print(f"Update Type: {message.get('update_type')}")
                
                # Process artist update
                # Add your processing logic here
                
            except json.JSONDecodeError as e:
                print(f"Failed to decode message: {e}")
                print(f"Raw message: {body}")
        
        # User preferences handler
        def user_preference_callback(ch, method, properties, body):
            try:
                message = json.loads(body)
                print(f"Received user preference update:")
                print(f"User: {message.get('user_id')}")
                print(f"Preference: {message.get('preference_type')}")
                
                # Process user preference
                # Add your processing logic here
                
            except json.JSONDecodeError as e:
                print(f"Failed to decode message: {e}")
                print(f"Raw message: {body}")
        
        # Set up consumers for each queue
        channel.basic_consume(
            queue='event-notifications',
            on_message_callback=event_notification_callback,
            auto_ack=True
        )
        
        channel.basic_consume(
            queue='artist-updates',
            on_message_callback=artist_update_callback,
            auto_ack=True
        )
        
        channel.basic_consume(
            queue='user-preferences',
            on_message_callback=user_preference_callback,
            auto_ack=True
        )
        
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
        
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Connection Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
