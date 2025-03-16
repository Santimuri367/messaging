#!/usr/bin/env python3
"""
Backend Example - Shows how to use the RabbitMQ client in a backend application
"""

from rabbitmq_client import RabbitMQClient
import json
import time
import threading
import logging
from fastapi import FastAPI, BackgroundTasks, HTTPException, Body
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('backend')

# Create FastAPI app
app = FastAPI(title="Seshin Music Event Backend")

# Create RabbitMQ client
mq_client = RabbitMQClient(
    host="10.147.20.12",
    port=5672,
    username="guest",
    password="guest",
    vhost="/"
)

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

# Store for processed events
processed_events = []

# Queue names
EVENT_QUEUE = "event_notifications"
SEARCH_QUEUE = "search_requests"
SEARCH_RESULT_QUEUE = "search_results"

# Connect on startup
@app.on_event("startup")
async def startup_event():
    # Connect to RabbitMQ
    if not mq_client.connect():
        logger.error("Failed to connect to RabbitMQ on startup")
        return
    
    # Initialize queues
    mq_client.declare_queue(EVENT_QUEUE)
    mq_client.declare_queue(SEARCH_QUEUE)
    mq_client.declare_queue(SEARCH_RESULT_QUEUE)
    
    # Start consumer for search results
    mq_client.consume(
        queue_name=SEARCH_RESULT_QUEUE,
        callback=process_search_results,
        auto_ack=False
    )
    
    logger.info("Backend started and connected to RabbitMQ")

# Clean up on shutdown
@app.on_event("shutdown")
async def shutdown_event():
    mq_client.close()
    logger.info("Backend shutting down")

# Process search results from RabbitMQ
def process_search_results(message):
    """Process search results coming from other services"""
    try:
        logger.info(f"Received search results: {message}")
        
        # Add to our processed events
        if isinstance(message, list):
            processed_events.extend(message)
        elif isinstance(message, dict) and "results" in message:
            processed_events.extend(message["results"])
            
    except Exception as e:
        logger.error(f"Error processing search results: {e}")

# API endpoint to publish an event
@app.post("/events/")
async def create_event(event: EventMessage):
    """Create a new event and notify via RabbitMQ"""
    try:
        # Convert to dict for message
        event_dict = event.dict()
        
        # Add timestamp
        event_dict["timestamp"] = time.time()
        
        # Publish to RabbitMQ
        success = mq_client.publish(
            queue_name=EVENT_QUEUE,
            message=event_dict
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to publish event to message queue")
        
        return {"status": "success", "message": "Event published", "event_id": id(event_dict)}
        
    except Exception as e:
        logger.error(f"Error creating event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# API endpoint to search for events
@app.post("/events/search/")
async def search_events(query: EventSearchQuery, background_tasks: BackgroundTasks):
    """Search for events using the messaging system"""
    try:
        # Convert query to dict
        query_dict = query.dict()
        
        # Add request ID
        query_dict["request_id"] = f"req-{time.time()}"
        
        # Publish search request to RabbitMQ
        success = mq_client.publish(
            queue_name=SEARCH_QUEUE,
            message=query_dict
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to publish search request to message queue")
        
        # Initially return empty results (real results will be processed asynchronously)
        return {
            "status": "pending", 
            "request_id": query_dict["request_id"],
            "message": "Search request submitted"
        }
        
    except Exception as e:
        logger.error(f"Error searching events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# API endpoint to get search results
@app.get("/events/results/")
async def get_search_results():
    """Get processed search results"""
    return {"results": processed_events}

# Endpoint to check RabbitMQ connection
@app.get("/health/rabbitmq")
async def check_rabbitmq_health():
    """Check if RabbitMQ connection is healthy"""
    if mq_client.connect():
        return {"status": "connected", "host": mq_client.host, "port": mq_client.port}
    else:
        return {"status": "disconnected", "host": mq_client.host, "port": mq_client.port}

# Simple index endpoint
@app.get("/")
async def root():
    return {"message": "Seshin Music Event Backend API", "status": "running"}

# Run with: uvicorn backend_example:app --host 0.0.0.0 --port 8001
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)