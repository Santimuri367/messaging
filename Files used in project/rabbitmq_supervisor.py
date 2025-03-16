#!/usr/bin/env python3
"""
RabbitMQ Supervisor - Monitors and auto-restarts RabbitMQ if it fails
"""

import time
import subprocess
import logging
import sys
import signal

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/var/log/rabbitmq_supervisor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('rabbitmq_supervisor')

# Configuration
CHECK_INTERVAL = 30  # seconds
MAX_RESTART_ATTEMPTS = 3
RESTART_COOLDOWN = 300  # seconds

# Global state
running = True
restart_count = 0
last_restart_time = 0

def check_rabbitmq_status():
    """
    Check if RabbitMQ service is running.
    
    Returns:
        bool: True if running, False otherwise
    """
    try:
        result = subprocess.run(
            "systemctl is-active rabbitmq-server", 
            shell=True, 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return result.stdout.decode().strip() == "active"
    except Exception as e:
        logger.error(f"Error checking RabbitMQ status: {e}")
        return False

def check_rabbitmq_health():
    """
    Check if RabbitMQ is responsive and healthy.
    
    Returns:
        bool: True if healthy, False otherwise
    """
    try:
        # Check if RabbitMQ process is responding
        result = subprocess.run(
            "sudo rabbitmqctl status",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10  # Timeout if it hangs
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        logger.error("RabbitMQ health check timed out")
        return False
    except Exception as e:
        logger.error(f"Error checking RabbitMQ health: {e}")
        return False

def restart_rabbitmq():
    """
    Restart the RabbitMQ service.
    
    Returns:
        bool: True if restart successful, False otherwise
    """
    global restart_count, last_restart_time
    
    current_time = time.time()
    
    # Check if we've exceeded restart attempts within cooldown period
    if current_time - last_restart_time < RESTART_COOLDOWN:
        restart_count += 1
        if restart_count > MAX_RESTART_ATTEMPTS:
            logger.error(f"Exceeded maximum restart attempts ({MAX_RESTART_ATTEMPTS}), waiting for cooldown period")
            time.sleep(RESTART_COOLDOWN)
            restart_count = 0
    else:
        # Reset counter if outside cooldown period
        restart_count = 1
    
    last_restart_time = current_time
    
    try:
        logger.info("Restarting RabbitMQ service...")
        
        # Stop the service
        subprocess.run(
            "sudo systemctl stop rabbitmq-server",
            shell=True,
            check=True,
            timeout=30
        )
        
        time.sleep(5)  # Wait for service to fully stop
        
        # Start the service
        subprocess.run(
            "sudo systemctl start rabbitmq-server",
            shell=True,
            check=True,
            timeout=60
        )
        
        time.sleep(10)  # Wait for service to initialize
        
        # Check if service is now running
        if check_rabbitmq_status():
            logger.info("RabbitMQ restart successful")
            return True
        else:
            logger.error("RabbitMQ failed to start after restart")
            return False
            
    except Exception as e:
        logger.error(f"Error restarting RabbitMQ: {e}")
        return False

def signal_handler(sig, frame):
    """Handle termination signals"""
    global running
    logger.info(f"Received signal {sig}, shutting down supervisor...")
    running = False

def main():
    """Main supervisor loop"""
    global running
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting RabbitMQ supervisor...")
    
    # Main monitoring loop
    while running:
        # Check if RabbitMQ is running
        if not check_rabbitmq_status():
            logger.warning("RabbitMQ service is not running")
            restart_rabbitmq()
        elif not check_rabbitmq_health():
            logger.warning("RabbitMQ service is running but not healthy")
            restart_rabbitmq()
        else:
            logger.debug("RabbitMQ service is running and healthy")
        
        # Wait for next check
        for _ in range(CHECK_INTERVAL):
            if not running:
                break
            time.sleep(1)
    
    logger.info("RabbitMQ supervisor shutting down")

if __name__ == "__main__":
    main()