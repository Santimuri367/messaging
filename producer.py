import pika  # type: ignore
import sys
import os
import json
from datetime import datetime
import pika.exceptions


class ProducerMessage:
    def __init__(self):
        try:
            self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='localhost')
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue='event-notifications')
            self.channel.queue_declare(queue='artist-updates')
            self.channel.queue_declare(queue='user-preferences')
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error connecting to server: {e}")
            sys.exit(1)

    def publish_event(self, event_data):
        try:
            message = {
                'event_name': event_data.get('event_name'),
                'artist': event_data.get('artist'),
                'location': event_data.get('place_name'),
                'genre': event_data.get('genre'),
                'date': event_data.get('date') 
            }
            self.channel.basic_publish(
                exchange='',
                routing_key='event_notifications',
                body = "Welcome user"
            ) 
            print (f"Sent event notification for {event_data.get('event_name')}")
        except Exception as e:
            print(f"Error getting messages: {e}")

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()

if __name__=="__main__":
    producer = ProducerMessage()
    event = {
        'event_name': 'Jazz Night',
        'artist': 'name',
        'location' : 'place or club',
        'genre': 'Jazz',
        'date' : 'date'
    }

    producer.publish_event(event)
    producer.close()

