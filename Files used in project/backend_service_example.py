#!/usr/bin/env python3
"""
Backend Service Example - Shows how to integrate backend with RabbitMQ
"""

from simple_service_broker import ServiceBroker
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

# Create service broker for messaging
broker = ServiceBroker(service_type="backend")

# Store for processed events and pending requests
processed_events = []
pending_requests = {}

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
        broker.send_message(
            to_service="database",
            message_type="query",
            data={
                "request_id": request_id,
                "collection": "events",
                "query": message.get("query", {})
            }
        )
        
        # Notify frontend that search is in progress
        broker.send_message(
            to_service="frontend",
            message_type="search_status",
            data={
                "request_id": request_id,
                "status": "processing",
                "message": "Search request received and processing"
            }
        )

def handle_query_results(message):
    """Handle query results from database."""
    request_id = message.get("request_id")
    
    # Store results
    if isinstance(message.get("results"), list):
        processed_events.extend(message.get("results"))
    
    # Forward results to frontend
    broker.send_message(
        to_service="frontend",
        message_type="search_results",
        data={
            "request_id": request_id,
            "results": message.get("results", []),
            "count": len(message.get("results", [])),
            "timestamp": time.time()
        }
    )
    
    # Remove from pending requests
    if request_id in pending_requests:
        del pending_requests[request_id]

def handle_update_status(message):
    """Handle update status from database."""
    request_id = message.get("request_id")
    success = message.get("success", False)
    
    # Notify frontend
    broker.send_message(
        to_service="frontend",
        message_type="notification",
        data={
            "request_id": request_id,
            "success": success,
            "message": "Event update " + ("successful" if success else "failed")
        }
    )

# Connect on startup
@app.on_event("startup")
async def startup_event():
    # Start consuming messages
    broker.receive_messages(handle_messages)
    logger.info("Backend service started and connected to messaging")

# Clean up on shutdown
@app.on_event("shutdown")
async def shutdown_event():
    broker.close()
    logger.info("Backend service shutting down")

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
        broker.send_message(
            to_service="database",
            message_type="update",
            data={
                "request_id": request_id,
                "collection": "events",
                "operation": "insert",
                "data": event_dict
            }
        )
        
        # Notify frontend of new event
        broker.send_message(
            to_service="frontend",
            message_type="notification",
            data={
                "message": f"New event added: {event.event_name}",
                "event": event_dict
            }
        )
        
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
        broker.send_message(
            to_service="database",
            message_type="query",
            data={
                "request_id": request_id,
                "collection": "events",
                "query": query_dict
            }
        )
        
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
    if broker.broker.connection and broker.broker.connection.is_open:
        return {"status": "connected", "service_type": broker.service_type}
    else:
        return {"status": "disconnected", "service_type": broker.service_type}

# Simple index endpoint
@app.get("/")
async def root():
    return {"message": "Seshin Music Event Backend API", "status": "running"}

# Run with: uvicorn backend_service_example:app --host 0.0.0.0 --port 8001
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)