def start_service(service_name):
    """Start the specified service on this machine"""
    global running_services
    
    if service_name in running_services and running_services[service_name].is_alive():
        logger.info(f"Service {service_name} is already running")
        return
    
    logger.info(f"Starting {service_name} service")
    
    # Define commands to start each service type
    service_commands = {
        'frontend': "python /path/to/frontend/app.py",    # Replace with actual frontend start command
        'backend': "python /path/to/backend/server.py",   # Replace with actual backend start command
        'database': "mongod --dbpath /path/to/data",      # Replace with actual database start command
        'messaging': "python /path/to/messaging/server.py" # Replace with actual messaging start command
    }
    
    # Get the command for this service
    start_cmd = service_commands.get(service_name)
    if not start_cmd:
        logger.error(f"No start command defined for service: {service_name}")
        return False
        
    def service_process(name, command):
        try:
            # Actually start the service as a subprocess
            logger.info(f"Starting {name.upper()} SERVICE with command: {command}")
            
            # Use subprocess to start the service
            process = subprocess.Popen(
                command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Send status update
            send_status_update(name, 'running', {'pid': process.pid})
            print(f"✅ Service {name} has been started with PID {process.pid}")
            
            # Monitor the process
            stdout, stderr = process.communicate()
            
            # Process has exited
            if process.returncode != 0:
                logger.error(f"Service {name} exited with error: {stderr}")
                send_status_update(name, 'error', {'error': stderr})
            else:
                logger.info(f"Service {name} completed successfully")
                send_status_update(name, 'stopped')
                
        except Exception as e:
            logger.error(f"Service {name} encountered an error: {e}")
            send_status_update(name, 'error', {'error': str(e)})
            
    # Start the service in a new thread
    service_thread = threading.Thread(
        target=service_process, 
        args=(service_name, start_cmd)
    )
    service_thread.daemon = True
    service_thread.start()
    
    # Store the thread reference
    running_services[service_name] = service_thread
    
    # Return success
    return True
