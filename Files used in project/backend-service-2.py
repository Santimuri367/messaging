#!/usr/bin/env python3
# backend_service.py

from messaging_broker import MessageBroker
import json
import time
import logging
import signal
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('backend_service')

class BackendService:
    def __init__(self):
        self.broker = MessageBroker(service_name="backend")
        # Connect to RabbitMQ
        self.broker.connect()
        logger.info("Backend service initialized and connected to RabbitMQ")
        
        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
    def handle_shutdown(self, sig, frame):
        """Handle clean shutdown on SIGINT or SIGTERM"""
        logger.info("Shutting down backend service...")
        self.broker.close()
        sys.exit(0)
        
    def process_message(self, ch, method, properties, body):
        """Process messages from other services"""
        try:
            message = json.loads(body)
            source = properties.headers.get('source_service', 'unknown') if properties.headers else 'unknown'
            
            logger.info(f"Backend received message from {source}: {message}")
            
            # Process based on message type
            if message.get('type') == 'user_action':
                self.handle_user_action(message)
            elif message.get('type') == 'search_results':
                self.handle_search_results(message)
            elif message.get('type') == 'write_response':
                self.handle_write_response(message)
            else:
                logger.warning(f"Unknown message type: {message.get('type')}")
            
            # Always acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # Still acknowledge to avoid message getting stuck in queue
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def handle_user_action(self, message):
        """Handle user actions from frontend"""
        action = message.get('action')
        logger.info(f"Processing user action: {action}")
        
        if action == 'search':
            # Forward search request to database
            self.broker.send_to_database({
                'type': 'search_request',
                'parameters': message.get('parameters', {}),
                'request_id': f"req-{int(time.time())}"
            })
            
            # Send acknowledgement to frontend
            self.broker.send_to_frontend({
                'type': 'action_received',
                'action': action,
                'status': 'processing'
            })
        elif action == 'update':
            # Handle update request
            # Process data
            processed_data = self.process_update_data(message.get('data', {}))
            
            # Send to database for storage
            self.broker.send_to_database({
                'type': 'write_request',
                'operation': 'update',
                'table': message.get('table', 'default'),
                'data': processed_data
            })
            
            # Send acknowledgement to frontend
            self.broker.send_to_frontend({
                'type': 'action_received',
                'action': action,
                'status': 'processing'
            })
    
    def handle_search_results(self, message):
        """Handle search results from database"""
        # Process the results
        enhanced_results = self.enhance_search_results(message.get('results', []))
        
        # Forward to frontend
        self.broker.send_to_frontend({
            'type': 'search_results',
            'request_id': message.get('request_id'),
            'results': enhanced_results,
            'count': len(enhanced_results)
        })
    
    def handle_write_response(self, message):
        """Handle response from database after a write operation"""
        # Notify frontend of result
        self.broker.send_to_frontend({
            'type': 'operation_status',
            'operation': message.get('operation'),
            'status': message.get('status'),
            'details': message.get('details')
        })
    
    def process_update_data(self, data):
        """Process update data before sending to database"""
        # This is where you'd implement business logic for data processing
        # For example, validating, transforming or enriching the data
        processed_data = data.copy()
        
        # Add timestamp
        processed_data['updated_at'] = time.time()
        
        # Add processed flag
        processed_data['processed_by_backend'] = True
        
        return processed_data
    
    def enhance_search_results(self, results):
        """Enhance search results with additional information"""
        # This is where you'd implement business logic to enhance search results
        enhanced_results = []
        
        for result in results:
            enhanced = result.copy()
            # Example: Add computed fields
            if 'price' in enhanced and 'quantity' in enhanced:
                enhanced['total_value'] = enhanced['price'] * enhanced['quantity']
            
            enhanced['processed_by_backend'] = True
            enhanced_results.append(enhanced)
        
        return enhanced_results
    
    def run(self):
        """Start the backend service"""
        logger.info("Starting backend service...")
        print("Backend service is running and listening for messages...")
        print("Press Ctrl+C to exit")
        
        # Start consuming messages from the backend queue
        self.broker.consume_with_callback(self.process_message)

if __name__ == "__main__":
    backend = BackendService()
    backend.run()
