# backend_handler.py (Backend Part)
class BackendHandler:
    def __init__(self, broker):
        self.broker = broker
        self.logger = logging.getLogger(__name__)

    def process_message(self, ch, method, properties, body):
        try:
            message = json.loads(body)
            self.logger.info(f"Processing message: {message}")
            
            # Add backend processing logic here
            result = self.handle_message(message)
            
            # Send response back
            response = {
                "task_id": message.get('task_id'),
                "result": result,
                "status": "completed"
            }
            
            self.broker.publish_message('response_queue', response)
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def handle_message(self, message):
        message_type = message.get('type')
        if message_type == 'calculation':
            numbers = message.get('numbers', [])
            return sum(numbers)
        # Add more message type handlers as needed
        return None
