# backend_service.py
from message_broker import MessageBroker
import json

class BackendService:
    def __init__(self):
        self.broker = MessageBroker()
        if not self.broker.connect():
            raise Exception("Failed to connect to message broker")
            
    def send_task(self, data: Dict):
        try:
            self.broker.channel.basic_publish(
                exchange='',
                routing_key='task_queue',
                body=json.dumps(data),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            print(f"Task sent: {data}")
            return True
        except Exception as e:
            print(f"Failed to send task: {e}")
            return False

# Test backend connection
def test_backend():
    backend = BackendService()
    test_data = {"action": "test", "data": "Hello from backend!"}
    backend.send_task(test_data)

if __name__ == "__main__":
    test_backend()