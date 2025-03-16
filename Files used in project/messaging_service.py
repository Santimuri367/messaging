#!/usr/bin/env python3
"""
Messaging Service - Main service wrapper for Seshin Music Event Finder messaging
This script starts both the RabbitMQ service and the control API
"""

import os
import sys
import time
import subprocess
import logging
import argparse
import signal
import requests

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/var/log/messaging_service.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('messaging_service')

# Service configurations
API_HOST = "0.0.0.0"  # Listen on all interfaces
API_PORT = 6004       # Control API port
RABBITMQ_PORT = 5672  # RabbitMQ port
MANAGEMENT_PORT = 15672  # RabbitMQ management port

# Process handles
api_process = None
rabbitmq_supervisor_process = None

def start_rabbitmq():
    """Start the RabbitMQ service"""
    logger.info("Starting RabbitMQ service...")
    try:
        # Check if service is already running
        result = subprocess.run(
            "systemctl is-active rabbitmq-server", 
            shell=True, 
            stdout=subprocess.PIPE
        )
        
        if result.stdout.decode().strip() == "active":
            logger.info("RabbitMQ is already running")
            return True
        
        # Start the service
        subprocess.run(
            "sudo systemctl start rabbitmq-server", 
            shell=True, 
            check=True
        )
        logger.info("RabbitMQ started successfully")
        
        # Wait for service to initialize
        time.sleep(5)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start RabbitMQ: {e}")
        return False
    except Exception as e:
        logger.error(f"Error starting RabbitMQ: {e}")
        return False

def start_control_api():
    """Start the control API"""
    global api_process
    
    logger.info("Starting Messaging Control API...")
    try:
        # Start the API as a subprocess
        cmd = [sys.executable, "service_api.py"]
        api_process = subprocess.Popen(cmd)
        
        # Wait for API to start
        time.sleep(2)
        
        # Check if API is running
        try:
            response = requests.get(f"http://localhost:{API_PORT}/")
            if response.status_code == 200:
                logger.info("Messaging Control API started successfully")
                return True
        except requests.RequestException:
            logger.error("Failed to connect to Messaging Control API")
            return False
    except Exception as e:
        logger.error(f"Error starting Messaging Control API: {e}")
        return False

def start_rabbitmq_supervisor():
    """Start the RabbitMQ supervisor process"""
    global rabbitmq_supervisor_process
    
    logger.info("Starting RabbitMQ supervisor...")
    try:
        # Start the supervisor as a subprocess
        cmd = [sys.executable, "rabbitmq_supervisor.py"]
        rabbitmq_supervisor_process = subprocess.Popen(cmd)
        
        # Wait for supervisor to start
        time.sleep(1)
        
        logger.info("RabbitMQ supervisor started successfully")
        return True
    except Exception as e:
        logger.error(f"Error starting RabbitMQ supervisor: {e}")
        return False

def stop_services():
    """Stop all services"""
    logger.info("Stopping all services...")
    
    # Stop the API process
    if api_process:
        logger.info("Stopping Messaging Control API...")
        api_process.terminate()
        api_process.wait(timeout=5)
    
    # Stop the supervisor process
    if rabbitmq_supervisor_process:
        logger.info("Stopping RabbitMQ supervisor...")
        rabbitmq_supervisor_process.terminate()
        rabbitmq_supervisor_process.wait(timeout=5)
    
    # Stop RabbitMQ
    logger.info("Stopping RabbitMQ service...")
    try:
        subprocess.run(
            "sudo systemctl stop rabbitmq-server", 
            shell=True, 
            check=True
        )
        logger.info("RabbitMQ stopped successfully")
    except Exception as e:
        logger.error(f"Error stopping RabbitMQ: {e}")
    
    logger.info("All services stopped")

def signal_handler(sig, frame):
    """Handle termination signals"""
    logger.info(f"Received signal {sig}, shutting down...")
    stop_services()
    sys.exit(0)

def configure_rabbitmq():
    """Configure RabbitMQ with standard settings"""
    logger.info("Configuring RabbitMQ...")
    
    try:
        # Create standard vhosts if needed
        subprocess.run(
            "sudo rabbitmqctl list_vhosts | grep -q '/' || " +
            "sudo rabbitmqctl add_vhost /",
            shell=True, check=True
        )
        
        # Ensure the default user exists with correct permissions
        subprocess.run(
            "sudo rabbitmqctl list_users | grep -q 'guest' || " +
            "sudo rabbitmqctl add_user guest guest",
            shell=True, check=True
        )
        
        # Set permissions for the default user
        subprocess.run(
            "sudo rabbitmqctl set_permissions -p / guest '.*' '.*' '.*'",
            shell=True, check=True
        )
        
        # Enable management plugin if not already enabled
        subprocess.run(
            "sudo rabbitmq-plugins list -e | grep -q 'rabbitmq_management' || " +
            "sudo rabbitmq-plugins enable rabbitmq_management",
            shell=True, check=True
        )
        
        logger.info("RabbitMQ configuration completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error configuring RabbitMQ: {e}")
        return False

def main():
    """Main service entry point"""
    parser = argparse.ArgumentParser(description="Seshin Music Event Finder Messaging Service")
    parser.add_argument("--no-supervisor", action="store_true", help="Don't start the RabbitMQ supervisor")
    args = parser.parse_args()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting Seshin Music Event Finder Messaging Service...")
    
    # Start RabbitMQ
    if not start_rabbitmq():
        logger.error("Failed to start RabbitMQ, exiting...")
        sys.exit(1)
    
    # Configure RabbitMQ
    if not configure_rabbitmq():
        logger.error("Failed to configure RabbitMQ, continuing anyway...")
    
    # Start the control API
    if not start_control_api():
        logger.error("Failed to start control API, exiting...")
        stop_services()
        sys.exit(1)
    
    # Start the supervisor (optional)
    if not args.no_supervisor:
        if not start_rabbitmq_supervisor():
            logger.error("Failed to start RabbitMQ supervisor, continuing without it...")
    
    logger.info(f"Messaging Service running - API available at http://0.0.0.0:{API_PORT}")
    logger.info(f"RabbitMQ available at port {RABBITMQ_PORT}")
    logger.info(f"RabbitMQ Management UI available at http://0.0.0.0:{MANAGEMENT_PORT}")
    
    try:
        # Keep the script running
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
    finally:
        stop_services()

if __name__ == "__main__":
    main()