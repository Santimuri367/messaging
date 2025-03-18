#!/usr/bin/env python3
# frontend_service.py

from messaging_broker import MessageBroker
import json
import time
import logging
import signal
import sys
import threading
import uuid

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('frontend_service')

class FrontendService:
    def __init__(self):
        self.broker = MessageBroker(service_name="frontend")
        # Connect to RabbitMQ
        self.broker.connect()
        logger.info("Frontend service initialized and connected to RabbitMQ")
        
        # Track pending requests
        self.pending_requests = {}
        
        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
        # Start consumer thread
        self.consumer_thread = threading.Thread(target=self.start_consuming)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        
    def handle_shutdown(self, sig, frame):
        """Handle clean shutdown on SIGINT or SIGTERM"""
        logger.info("Shutting down frontend service...")
        self.broker.close()
        sys.exit(0)
        
    def process_message(self, ch, method, properties, body):
        """Process messages from other services"""
        try:
            message = json.loads(body)
            source = properties.headers.get('source_service', 'unknown') if properties.headers else 'unknown'
            
            logger.info(f"Frontend received message from {source}: {message}")
            
            # Process based on message type
            if message.get('type') == 'event_notification':
                self.handle_event_notification(message)
            elif message.get('type') == 'search_results':
                self.handle_search_results(message)
            elif message.get('type') == 'operation_status':
                self.handle_operation_status(message)
            elif message.get('type') == 'action_received':
                self.handle_action_received(message)
            else:
                logger.warning(f"Unknown message type: {message.get('type')}")
            
            # Always acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # Still acknowledge to avoid message getting stuck in queue
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def handle_event_notification(self, message):
        """Handle event notifications from backend"""
        event_data = message.get('data', {})
        event_id = event_data.get('event_id')
        event_name = event_data.get('event_name')
        
        logger.info(f"Received event notification: {event_name} (ID: {event_id})")
        # In a real application, you would update the UI here
        print(f"UI UPDATE: New event notification - {event_name}")
    
    def handle_search_results(self, message):
        """Handle search results from backend"""
        request_id = message.get('request_id')
        results = message.get('results', [])
        count = message.get('count', 0)
        
        if request_id in self.pending_requests:
            logger.info(f"Received search results for request {request_id}: {count} items")
            # Remove from pending requests
            del self.pending_requests[request_id]
            
            # In a real application, you would update the UI here
            print(f"UI UPDATE: Displaying {count} search results")
            for i, result in enumerate(results[:3]):  # Show just first 3 for brevity
                print(f"  Result {i+1}: {result}")
            if count > 3:
                print(f"  ... and {count-3} more results")
        else:
            logger.warning(f"Received results for unknown request ID: {request_id}")
    
    def handle_operation_status(self, message):
        """Handle operation status updates from backend"""
        operation = message.get('operation')
        status = message.get('status')
        details = message.get('details', {})
        
        logger.info(f"Operation {operation} status: {status}")
        
        # In a real application, you would update the UI here
        print(f"UI UPDATE: Operation {operation} completed with status: {status}")
        if details:
            print(f"  Details: {details}")
    
    def handle_action_received(self, message):
        """Handle acknowledgement that backend received action"""
        action = message.get('action')
        status = message.get('status')
        
        logger.info(f"Action {action} received by backend with status: {status}")
        
        # In a real application, you would update the UI here
        print(f"UI UPDATE: Action {action} is now {status}")
    
    def start_consuming(self):
        """Start consuming messages in a separate thread"""
        try:
            self.broker.consume_with_callback(self.process_message)
        except Exception as e:
            logger.error(f"Error in consumer thread: {e}")
    
    def send_search_request(self, search_params):
        """Send a search request to the backend"""
        request_id = str(uuid.uuid4())
        
        message = {
            'type': 'user_action',
            'action': 'search',
            'parameters': search_params,
            'request_id': request_id
        }
        
        # Store in pending requests
        self.pending_requests[request_id] = {
            'time': time.time(),
            'params': search_params
        }
        
        # Send to backend
        self.broker.send_to_backend(message)
        logger.info(f"Sent search request {request_id} to backend")
        
        return request_id
    
    def send_update_request(self, table, data):
        """Send an update request to the backend"""
        message = {
            'type': 'user_action',
            'action': 'update',
            'table': table,
            'data': data
        }
        
        # Send to backend
        self.broker.send_to_backend(message)
        logger.info(f"Sent update request for table {table} to backend")
    
    def simulate_user_interaction(self):
        """Simulate user interactions for testing"""
        # Simulate search request
        print("\n=== User performs search for 'Jazz concerts' ===")
        self.send_search_request({
            'keyword': 'Jazz',
            'category': 'concerts',
            'date_range': {'start': '2025-03-01', 'end': '2025-04-30'}
        })
        
        # Wait a bit
        time.sleep(2)
        
        # Simulate update request
        print("\n=== User updates profile information ===")
        self.send_update_request('user_profiles', {
            'user_id': 12345,
            'name': 'Jane Smith',
            'email': 'jane.smith@example.com',
            'preferences': {
                'notifications': True,
                'theme': 'dark'
            }
        })
        
        # Keep main thread running to receive responses
        print("\n=== Waiting for responses... ===")
        
    def run(self):
        """Start the frontend service and simulate user interaction"""
        logger.info("Starting frontend service...")
        
        # Allow consumer thread to start
        time.sleep(1)
        
        # Simulate user interactions
        self.simulate_user_interaction()
        
        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nExiting frontend service...")

if __name__ == "__main__":
    frontend = FrontendService()
    frontend.run()
