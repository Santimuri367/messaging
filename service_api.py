# Messaging Service API
# Save as: service_api.py

import os
import subprocess
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

# Enable CORS to allow requests from other VMs
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Messaging Service Control API is running"}

@app.get("/start")
async def start_service():
    """Start the RabbitMQ service"""
    try:
        # Check if service is already running
        result = subprocess.run("systemctl is-active rabbitmq-server", shell=True, stdout=subprocess.PIPE)
        
        if result.stdout.decode().strip() == "active":
            return {"status": "RabbitMQ is already running"}
        
        # Start the service
        subprocess.run("sudo systemctl start rabbitmq-server", shell=True, check=True)
        return {"status": "RabbitMQ started successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start RabbitMQ: {str(e)}")

@app.get("/stop")
async def stop_service():
    """Stop the messaging service"""
    try:
        subprocess.run("sudo systemctl stop rabbitmq-server", shell=True, check=True)
        return {"status": "RabbitMQ stopped successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop RabbitMQ: {str(e)}")

@app.get("/status")
async def service_status():
    """Check if the service is running"""
    result = subprocess.run("systemctl is-active rabbitmq-server", shell=True, stdout=subprocess.PIPE)
    
    if result.stdout.decode().strip() == "active":
        return {"status": "running"}
    else:
        return {"status": "stopped"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=6004)