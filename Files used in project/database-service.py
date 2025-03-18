#!/usr/bin/env python3
# database_service.py

from messaging_broker import MessageBroker
import json
import time
import logging
import signal
import sys
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('database_service')

class DatabaseService:
    def __init__(self):
        self.broker = MessageBroker(service_name="database")
        # Connect to RabbitMQ
        self.broker.connect()
        logger.info("Database service initialized and connected to RabbitMQ")
        
        # Initialize in-memory database (in a real system, this would be a real database)
        self.init_database()
        
        # Set up signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        
    def init_database(self):
        """Initialize the in-memory database with some sample data"""
        self.db = {
            'events': [
                {"id": "evt-1", "name": "Jazz Night", "artist": "Miles Davis", "date": "2025-03-15", "price": 45.00, "venue": "Blue Note", "tickets_available": 120},
                {"id": "evt-2", "name": "Blues Festival", "artist": "B.B. King", "date": "2025-03-22", "price": 55.00, "venue": "City Park", "tickets_available": 500},
                {"id": "evt-3", "name": "Classical Concert", "artist": "London Symphony", "date": "2025-04-05", "price": 75.00, "venue": "Concert Hall", "tickets_available": 300},
                {"id": "evt-4", "name": "Rock Arena", "artist": "The Rolling Stones", "date": "2025-04-12", "price": 95.00, "venue": "Stadium", "tickets_available": 2000},
                {"id": "evt-5", "name": "Jazz Festival", "artist": "Various Artists", "date": "2025-04-18", "price": 65.00, "venue": "Jazz Club", "tickets_available": 250},
            ],
            'user_profiles': [
                {"user_id": 12345, "name": "John Doe", "email": "john.doe@example.com", "preferences": {"notifications": True, "theme": "light"}},
                {"user_id": 67890, "name": "Alice Smith", "email": "alice.smith@example.com", "preferences": {"notifications": False, "theme": "dark"}}
            ]
        }
        logger.info("In-memory database initialized with sample data")
        
    def handle_shutdown(self, sig, frame):
        """Handle clean shutdown on SIGINT or SIGTERM"""
        logger.info("Shutting down database service...")
        self.broker.close()
        sys.exit(0)
        
    def process_message(self, ch, method, properties, body):
        """Process messages from other services"""
        try:
            message = json.loads(body)
            source = properties.headers.get('source_service', 'unknown') if properties.headers else 'unknown'
            
            logger.info(f"Database received message from {source}: {message}")
            
            # Process based on message type
            if message.get('type') == 'search_request':
                self.handle_search_request(message, source)
            elif message.get('type') == 'write_request':
                self.handle_write_request(message, source)
            else:
                logger.warning(f"Unknown message type: {message.get('type')}")
            
            # Always acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            # Still acknowledge to avoid message getting stuck in queue
            ch.basic_ack(delivery_tag=method.delivery_tag)
    
    def handle_search_request(self, message, source):
        """Handle search requests from other services"""
        parameters = message.get('parameters', {})
        request_id = message.get('request_id', 'unknown')
        
        logger.info(f"Processing search request {request_id} with parameters: {parameters}")
        
        # Determine which collection to search
        collection = parameters.get('collection', 'events')
        
        # Perform search (simplified example)
        results = self.search_data(collection, parameters)
        
        # Send results back to requesting service
        if source == 'backend':
            self.broker.send_to_backend({
                'type': 'search_results',
                'request_id': request_id,
                'results': results,
                'count': len(results)
            })
        elif source == 'frontend':
            self.broker.send_to_frontend({
                'type': 'search_results',
                'request_id': request_id,
                'results': results,
                'count': len(results)
            })
    
    def handle_write_request(self, message, source):
        """Handle write requests from other services"""
        operation = message.get('operation')
        table = message.get('table')
        data = message.get('data', {})
        
        logger.info(f"Processing write request: {operation} on {table}")
        
        status = "success"
        details = {}
        
        try:
            if operation == 'insert':
                self.insert_data(table, data)
                details = {"record_id": data.get('id') or data.get('user_id')}
            elif operation == 'update':
                self.update_data(table, data)
                details = {"updated_id": data.get('id') or data.get('user_id')}
            elif operation == 'delete':
                self.delete_data(table, data)
                details = {"deleted_id": data.get('id') or data.get('user_id')}
            else:
                status = "error"
                details = {"error": f"Unknown operation: {operation}"}
        except Exception as e:
            status = "error"
            details = {"error": str(e)}
        
        # Send response back to the requesting service
        response = {
            'type': 'write_response',
            'operation': operation,
            'status': status,
            'details': details
        }
        
        if source == 'backend':
            self.broker.send_to_backend(response)
        elif source == 'frontend':
            self.broker.send_to_frontend(response)
    
    def search_data(self, collection, parameters):
        """Search data in the in-memory database"""
        if collection not in self.db:
            return []
        
        # Get all items in the collection
        items = self.db[collection]
        
        # Apply filters (simplified example)
        filtered_items = items
        
        # Filter by keyword if provided
        if 'keyword' in parameters:
            keyword = parameters['keyword'].lower()
            filtered_items = [
                item for item in filtered_items 
                if any(str(value).lower().find(keyword) >= 0 for value in item.values() if isinstance(value, (str, int, float)))
            ]
        
        # Filter by category (for events)
        if 'category' in parameters and collection == 'events':
            category = parameters['category'].lower()
            # Simplified category mapping
            if category == 'concerts':
                filtered_items = [
                    item for item in filtered_items 
                    if 'concert' in item.get('name', '').lower() or 'night' in item.get('name', '').lower()
                ]
        
        # Filter by date range (for events)
        if 'date_range' in parameters and collection == 'events':
            date_range = parameters['date_range']
            start_date = date_range.get('start')
            end_date = date_range.get('end')
            
            if start_date and end_date:
                filtered_items = [
                    item for item in filtered_items 
                    if start_date <= item.get('date', '') <= end_date
                ]
        
        return filtered_items
    
    def insert_data(self, table, data):
        """Insert data into the in-memory database"""
        if table not in self.db:
            self.db[table] = []
        
        # Check for duplicates
        id_field = 'id' if 'id' in data else 'user_id'
        record_id = data.get(id_field)
        
        # Ensure we don't have duplicates
        for existing in self.db[table]:
            if existing.get(id_field) == record_id:
                raise ValueError(f"Record with {id_field}={record_id} already exists")
        
        # Add to database
        self.db[table].append(data)
        logger.info(f"Inserted new record in {table}: {data.get(id_field)}")
    
    def update_data(self, table, data):
        """Update data in the in-memory database"""
        if table not in self.db:
            raise ValueError(f"Table {table} does not exist")
        
        # Determine ID field
        id_field = 'id' if 'id' in data else 'user_id'
        record_id = data.get(id_field)
        
        if not record_id:
            raise ValueError(f"Missing {id_field} in update data")
        
        # Find and update the record
        for i, existing in enumerate(self.db[table]):
            if existing.get(id_field) == record_id:
                # Merge existing with new data
                self.db[table][i] = {**existing, **data}
                logger.info(f"Updated record in {table}: {record_id}")
                return
        
        # Record not found
        raise ValueError(f"Record with {id_field}={record_id} not found")
    
    def delete_data(self, table, data):
        """Delete data from the in-memory database"""
        if table not in self.db:
            raise ValueError(f"Table {table} does not exist")
        
        # Determine ID field
        id_field = 'id' if 'id' in data else 'user_id'
        record_id = data.get(id_field)
        
        if not record_id:
            raise ValueError(f"Missing {id_field} in delete data")
        
        # Find and delete the record
        initial_count = len(self.db[table])
        self.db[table] = [item for item in self.db[table] if item.get(id_field) != record_id]
        
        if len(self.db[table]) == initial_count:
            raise ValueError(f"Record with {id_field}={record_id} not found")
        
        logger.info(f"Deleted record from {table}: {record_id}")
    
    def run(self):
        """Start the database service"""
        logger.info("Starting database service...")
        
        # Start consuming messages from the database queue
        self.broker.consume_with_callback(self.process_message)

if __name__ == "__main__":
    database = DatabaseService()
    database.run()
