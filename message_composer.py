# message_composer.py
from messaging_broker import MessageBroker
import json
import time
import logging
from datetime import datetime

class MessageComposer:
    def __init__(self):
        self.broker = MessageBroker()
        self.logger = logging.getLogger(__name__)
        
    def setup(self):
        """Setup initial connection and queues"""
        self.broker.connect()
        
        # Declare necessary queues
        queues = ['frontend_to_backend', 'backend_to_frontend', 'events_queue']
        for queue in queues:
            self.broker.setup_queue(queue)
            
    def compose_and_send_message(self, message_type: str, data: dict, queue: str):
        """Compose and send a message with standard format"""
        message = {
            "type": message_type,
            "data": data,
            "timestamp": datetime.now().isoformat(),
            "message_id": str(uuid.uuid4())
        }
        
        return self.broker.publish_message(queue, message)
        
    def send_event_message(self, event_data: dict):
        """Send an event-related message"""
        return self.compose_and_send_message(
            message_type="event",
            data=event_data,
            queue="frontend_to_backend"
        )
        
    def send_user_message(self, user_data: dict):
        """Send a user-related message"""
        return self.compose_and_send_message(
            message_type="user",
            data=user_data,
            queue="frontend_to_backend"
        )

# Example usage
if __name__ == "__main__":
    composer = MessageComposer()
    composer.setup()
    
    # Example: Send an event message
    event_data = {
        "event_name": "Music Festival 2025",
        "date": "2025-06-15",
        "location": "Central Park",
        "capacity": 5000
    }
    
    try:
        message_id = composer.send_event_message(event_data)
        print(f"Event message sent successfully with ID: {message_id}")
    except Exception as e:
        print(f"Error sending message: {e}")
    finally:
        composer.broker.close()
