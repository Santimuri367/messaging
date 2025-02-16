import pika   #type: ignore
import sys
import os
import json
from datetime import datetime

import pika.exceptions

def main():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='event-notifications')
        channel.queue_declare(queue='artist-updates')
        channel.queue_declare(queue='user-preferences')


        def event_notification_answer(ch, method, properties, body):
            try:
                message = json.loads(body)
                print(f"Event: {message.get('event_name')}")
                print(f"Artist: {message.get('artist_name')}")
                print(f"Location: {message.get('place_name')}")
                print(f"Genre: {message.get('genre')}")

            except json.JSONDecodeError as e:
                print (f"Coudn't process an answer: {e}")
                print (f"Wrong message: {body}")

        channel.basic_consume(
            queue='Notifications',
            on_message_callback=event_notification_answer,
            auto_ack=True
        )
        print(' [*] Waiting for message, exit press CTRL+C')
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