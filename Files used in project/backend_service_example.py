#!/usr/bin/env python3
"""
Backend Service Example - Shows how to integrate backend with RabbitMQ
"""

import pika
import json
import time
import logging
import threading
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('backend_service')

# Create FastAPI app
app = FastAPI(title="Seshin Music Event Backend")

# Store for processed events and pending requests
processed_events = []
pending_requests = {}

def send_to_service(service_name, message):
    """
    Send a message to a specific service queue
    
    Args:
        service_name (str): Name of the service to send to (frontend, backend, database)
        message (dict): Message content to send
    """
    try:
        # Convert dict to JSON string if it's a dict
        if isinstance(message, dict):
            message = json.dumps(message)
        
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        
        # Declare queue (creates it if it doesn't exist)
        queue_name = f"{service_name}_queue"
        channel.queue_declare(queue=queue_name)
        
        # Send message
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message
        )
        
        logger.info(f"Sent message to {service_name} service")
        connection.close()
        return True
    except Exception as e:
        logger.error(f"Error sending message to {service_name} service: {e}")
        return False

def setup_message_consumer(callback_function):
    """
    Set up a consumer to receive messages from the backend queue
    
    Args:
        callback_function (function): Function to call when a message is received
    """
    try:
        # Setup connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        channel = connection.channel()
        
        # Declare the queue
        queue_name = "backend_queue"
        channel.queue_declare(queue=queue_name)
        
        # Define how to process messages
        def message_callback(ch, method, properties, body):
            try:
                # Convert JSON string to dict
                message = json.loads(body)
                # Process the message
                callback_function(message)
                # Acknowledge message receipt
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except json.JSONDecodeError:
                logger.error("Received invalid JSON message")
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # Set up quality of service
        channel.basic_qos(prefetch_count=1)
        
        # Start consuming
        channel.basic_consume(
            queue=queue_name,
            on_message_callback=message_callback
        )
        
        # Start consumer thread
        consumer_thread = threading.Thread(
            target=channel.start_consuming,
            daemon=True  # This ensures the thread will exit when the main program exits
        )
        consumer_thread.start()
        
        logger.info(f"Started consuming messages from backend queue")
        return connection, channel, consumer_thread
    except Exception as e:
        logger.error(f"Error setting up message consumer: {e}")
        return None, None, None

# Message handler for incoming messages
def handle_messages(message):
    """
    Process messages from other services.
    """
    try:
        logger.info(f"Received message: {message}")
        
        # Message from frontend
        if message.get("source_service") == "frontend":
            if message.get("type") == "user_action":
                handle_user_action(message)
                
        # Message from database
        elif message.get("source_service") == "database":
            if message.get("type") == "query_results":
                handle_query_results(message)
            elif message.get("type") == "update_status":
                handle_update_status(message)
                
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def handle_user_action(message):
    """Handle user action message from frontend."""
    action = message.get("action")
    
    if action == "search":
        # Store the request
        request_id = message.get("request_id")
        pending_requests[request_id] = {
            "timestamp": time.time(),
            "query": message.get("query", {})
        }
        
        # Forward search to database
        send_to_service("database", {
            "source_service": "backend",
            "type": "query",
            "request_id": request_id,
            "collection": "events",
            "query": message.get("query", {})
        })
        
        # Notify frontend that search is in progress
        send_to_service("frontend", {
            "source_service": "backend",
            "type": "search_status",
            "request_id": request_id,
            "status": "processing",
            "message": "Search request received and processing"
        })

def handle_query_results(message):
    """Handle query results from database."""
    request_id = message.get("request_id")
    
    # Store results
    if isinstance(message.get("results"), list):
        processed_events.extend(message.get("results"))
    
    # Forward results to frontend
    send_to_service("frontend", {
        "source_service": "backend",
        "type": "search_results",
        "request_id": request_id,
        "results": message.get("results", []),
        "count": len(message.get("results", [])),
        "timestamp": time.time()
    })
    
    # Remove from pending requests
    if request_id in pending_requests:
        del pending_requests[request_id]

def handle_update_status(message):
    """Handle update status from database."""
    request_id = message.get("request_id")
    success = message.get("success", False)
    
    # Notify frontend
    send_to_service("frontend", {
        "source_service": "backend",
        "type": "notification",
        "request_id": request_id,
        "success": success,
        "message": "Event update " + ("successful" if success else "failed")
    })

# Connection and channel for RabbitMQ
connection = None
channel = None
consumer_thread = None

# Connect on startup
@app.on_event("startup")
async def startup_event():
    global connection, channel, consumer_thread
    # Start consuming messages
    connection, channel, consumer_thread = setup_message_consumer(handle_messages)
    logger.info("Backend service started and connected to messaging")

# Clean up on shutdown
@app.on_event("shutdown")
async def shutdown_event():
    global connection
    if connection and not connection.is_closed:
        connection.close()
    logger.info("Backend service shutting down")


# Define models for our API
class EventMessage(BaseModel):
    event_name: str
    artist: str
    venue: str
    date: str
    description: Optional[str] = None
    ticket_url: Optional[str] = None
    tags: List[str] = []
    
class EventSearchQuery(BaseModel):
    artist: Optional[str] = None
    venue: Optional[str] = None
    date_range: Optional[Dict[str, str]] = None
    tags: List[str] = []

# API endpoint to publish an event
@app.post("/events/")
async def create_event(event: EventMessage):
    """Create a new event and notify via messaging."""
    try:
        # Convert to dict for message
        event_dict = event.dict()
        
        # Add timestamp and request ID
        event_dict["timestamp"] = time.time()
        request_id = f"evt-{time.time()}"
        
        # Send to database for storage
        send_to_service("database", {
            "source_service": "backend",
            "type": "update",
            "request_id": request_id,
            "collection": "events",
            "operation": "insert",
            "data": event_dict
        })
        
        # Notify frontend of new event
        send_to_service("frontend", {
            "source_service": "backend",
            "type": "notification",
            "message": f"New event added: {event.event_name}",
            "event": event_dict
        })
        
        return {"status": "success", "message": "Event published", "event_id": request_id}
        
    except Exception as e:
        logger.error(f"Error creating event: {e}")
        return {"status": "error", "message": str(e)}

# API endpoint to search for events
@app.post("/events/search/")
async def search_events(query: EventSearchQuery, background_tasks: BackgroundTasks):
    """Search for events using the messaging system."""
    try:
        # Convert query to dict
        query_dict = query.dict()
        
        # Add request ID
        request_id = f"req-{time.time()}"
        
        # Send query to database
        send_to_service("database", {
            "source_service": "backend",
            "type": "query",
            "request_id": request_id,
            "collection": "events",
            "query": query_dict
        })
        
        # Return pending status
        return {
            "status": "pending", 
            "request_id": request_id,
            "message": "Search request submitted"
        }
        
    except Exception as e:
        logger.error(f"Error searching events: {e}")
        return {"status": "error", "message": str(e)}

# API endpoint to get search results
@app.get("/events/results/")
async def get_search_results():
    """Get processed search results."""
    return {"results": processed_events}

# Endpoint to check messaging connection
@app.get("/health/messaging")
async def check_messaging_health():
    """Check if messaging connection is healthy."""
    global connection
    if connection and not connection.is_closed:
        return {"status": "connected", "service": "backend"}
    else:
        return {"status": "disconnected", "service": "backend"}

# Simple index endpoint
@app.get("/")
async def root():
    return {"message": "Seshin Music Event Backend API", "status": "running"}

# Run with: uvicorn backend_service_example:app --host 0.0.0.0 --port 8001
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
