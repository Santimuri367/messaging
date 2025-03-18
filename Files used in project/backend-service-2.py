#!/usr/bin/env python3
# backend_service.py

from messaging_broker import MessageBroker
import json
import time
import logging
import signal
import sys
import threading
import queue

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('backend_service')

# Global flag for graceful shutdown
shutdown_flag = False

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
        global shutdown_flag
        logger.info("Shutting down backend service...")
        shutdown_flag = True
        try:
            self.broker.close()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
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
    
    # [Rest of the service methods remain the same]
    
    def run(self):
        """Start the backend service"""
        logger.info("Starting backend service...")
        print("Backend service is running and listening for messages...")
        print("Press Ctrl+C to exit")
        
        while not shutdown_flag:
            try:
                # Ensure connection is up
                if not self.broker.connection or not self.broker.connection.is_open:
                    logger.info("Connection lost, reconnecting...")
                    self.broker.connect()
                
                # Start consuming messages from the backend queue
                self.broker.consume_with_callback(self.process_message)
            except Exception as e:
                logger.error(f"Error in consumer: {e}")
                # Wait before retry
                time.sleep(2)


def interactive_mode():
    """Run backend service in interactive mode, allowing manual message sending"""
    print("\n=== Backend Service Interactive Mode ===")
    print("Choose what message to send:")
    
    # Create a dedicated broker for interactive mode
    interactive_broker = None
    
    while not shutdown_flag:
        try:
            # Check or create broker
            if interactive_broker is None or not interactive_broker.connection or not interactive_broker.connection.is_open:
                if interactive_broker is not None:
                    try:
                        interactive_broker.close()
                    except:
                        pass
                print("\nConnecting to RabbitMQ...")
                interactive_broker = MessageBroker(service_name="backend_interactive")
                success = interactive_broker.connect()
                if not success:
                    print("Failed to connect to RabbitMQ. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                print("Connected to RabbitMQ")
            
            print("\nOptions:")
            print("1. Send search request to database")
            print("2. Send search results to frontend")
            print("3. Send update request to database")
            print("4. Send operation status to frontend")
            print("5. Exit interactive mode")
            
            choice = input("\nEnter your choice (1-5): ")
            
            if choice == '1':
                keyword = input("Enter search keyword: ")
                message_id = interactive_broker.send_to_database({
                    'type': 'search_request',
                    'parameters': {
                        'collection': 'events',
                        'keyword': keyword
                    },
                    'request_id': f"req-{int(time.time())}"
                })
                if message_id:
                    print(f"Search request sent to database for keyword: {keyword}")
                else:
                    print("Failed to send message")
                
            elif choice == '2':
                try:
                    result_count = int(input("How many mock results to send: "))
                    results = []
                    for i in range(result_count):
                        results.append({
                            "id": f"evt-{i+1}",
                            "name": f"Event {i+1}",
                            "artist": f"Artist {i+1}"
                        })
                    
                    message_id = interactive_broker.send_to_frontend({
                        'type': 'search_results',
                        'request_id': f"req-{int(time.time())}",
                        'results': results,
                        'count': len(results)
                    })
                    if message_id:
                        print(f"Search results sent to frontend: {result_count} items")
                    else:
                        print("Failed to send message")
                except ValueError:
                    print("Please enter a valid number")
                
            elif choice == '3':
                table = input("Enter table name (events/user_profiles): ")
                message_id = interactive_broker.send_to_database({
                    'type': 'write_request',
                    'operation': 'update',
                    'table': table,
                    'data': {
                        'id': 'evt-1',
                        'name': 'Updated Event',
                        'processed_by_backend': True
                    }
                })
                if message_id:
                    print(f"Update request sent to database for table: {table}")
                else:
                    print("Failed to send message")
                
            elif choice == '4':
                operation = input("Enter operation (update/insert/delete): ")
                message_id = interactive_broker.send_to_frontend({
                    'type': 'operation_status',
                    'operation': operation,
                    'status': 'success',
                    'details': {'record_id': 'evt-1'}
                })
                if message_id:
                    print(f"Operation status sent to frontend for: {operation}")
                else:
                    print("Failed to send message")
                
            elif choice == '5':
                print("Exiting interactive mode")
                try:
                    if interactive_broker:
                        interactive_broker.close()
                except:
                    pass
                break
                
            else:
                print("Invalid choice, please try again")
                
        except Exception as e:
            print(f"Error: {e}")
            print("Reconnecting to RabbitMQ...")
            try:
                if interactive_broker:
                    interactive_broker.close()
            except:
                pass
            interactive_broker = None
            time.sleep(2)


if __name__ == "__main__":
    try:
        # Check for command line argument
        if len(sys.argv) > 1 and sys.argv[1] == 'interactive':
            # Only run interactive mode (no consumer)
            interactive_mode()
        elif len(sys.argv) > 1 and sys.argv[1] == 'combined':
            # Run both consumer and interactive mode in separate threads
            backend = BackendService()
            
            # Start consumer in a separate thread
            consumer_thread = threading.Thread(target=backend.run)
            consumer_thread.daemon = True
            consumer_thread.start()
            
            # Wait for consumer to initialize
            time.sleep(3)
            
            # Start interactive mode in main thread
            interactive_mode()
            
        else:
            # Run in normal mode (consumer only)
            backend = BackendService()
            backend.run()
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        shutdown_flag = True


