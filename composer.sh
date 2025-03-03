#!/bin/bash

# Seshin Music Event Finder Composer Script
# This script starts all services across the VPN

# Service IP addresses in VPN
BACKEND_IP="10.147.20.113"
FRONTEND_IP="10.147.20.38"
DATABASE_IP="10.147.20.166"
MESSAGING_IP="10.147.20.12"

# Service API ports
BACKEND_PORT="6001"
FRONTEND_PORT="6002"
DATABASE_PORT="6003"
MESSAGING_PORT="6004"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to call service API
call_service_api() {
    local ip=$1
    local port=$2
    local endpoint=$3
    local service_name=$4
    
    echo -e "${YELLOW}Calling $service_name service...${NC}"
    response=$(curl -s -X GET http://$ip:$port/$endpoint)
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}$service_name service: $response${NC}"
        return 0
    else
        echo -e "${RED}Failed to call $service_name service${NC}"
        return 1
    fi
}

# Function to stop all services
stop_all_services() {
    echo -e "${BLUE}===============================================${NC}"
    echo -e "${BLUE}       Stopping All Services      ${NC}"
    echo -e "${BLUE}===============================================${NC}"
    
    # Stop backend service
    echo -e "${YELLOW}Stopping Backend Service...${NC}"
    call_service_api $BACKEND_IP $BACKEND_PORT "stop" "Backend" || {
        echo -e "${RED}Backend service stop failed${NC}"
    }
    
    # Stop frontend service
    echo -e "${YELLOW}Stopping Frontend Service (Apache/PHP)...${NC}"
    call_service_api $FRONTEND_IP $FRONTEND_PORT "stop" "Frontend" || {
        echo -e "${RED}Frontend service stop failed${NC}"
    }
    
    # Stop database service
    echo -e "${YELLOW}Stopping Database Service (MySQL/MariaDB)...${NC}"
    call_service_api $DATABASE_IP $DATABASE_PORT "stop" "Database" || {
        echo -e "${RED}Database service stop failed${NC}"
    }
    
    # Stop messaging service
    echo -e "${YELLOW}Stopping Messaging Service (RabbitMQ)...${NC}"
    call_service_api $MESSAGING_IP $MESSAGING_PORT "stop" "Messaging" || {
        echo -e "${RED}Messaging service stop failed${NC}"
    }
    
    echo -e "${BLUE}===============================================${NC}"
    echo -e "${GREEN}All services stopped${NC}"
}

# Check for command line arguments
if [ "$1" == "stop" ]; then
    stop_all_services
    exit 0
fi

# Print header
echo -e "${BLUE}===============================================${NC}"
echo -e "${BLUE}       Seshin Music Event Finder Services      ${NC}"
echo -e "${BLUE}===============================================${NC}"

# Start backend service
echo -e "${YELLOW}Starting Backend Service...${NC}"
call_service_api $BACKEND_IP $BACKEND_PORT "start" "Backend" || {
    echo -e "${RED}Backend service failed to start${NC}"
}

# Start frontend service
echo -e "${YELLOW}Starting Frontend Service (Apache/PHP)...${NC}"
call_service_api $FRONTEND_IP $FRONTEND_PORT "start" "Frontend" || {
    echo -e "${RED}Frontend service failed to start${NC}"
}

# Start database service
echo -e "${YELLOW}Starting Database Service (MySQL/MariaDB)...${NC}"
call_service_api $DATABASE_IP $DATABASE_PORT "start" "Database" || {
    echo -e "${RED}Database service failed to start${NC}"
}

# Start messaging service
echo -e "${YELLOW}Starting Messaging Service (RabbitMQ)...${NC}"
call_service_api $MESSAGING_IP $MESSAGING_PORT "start" "Messaging" || {
    echo -e "${RED}Messaging service failed to start${NC}"
}

echo -e "${BLUE}===============================================${NC}"
echo -e "${YELLOW}Checking service status...${NC}"

# Check status of all services
call_service_api $BACKEND_IP $BACKEND_PORT "status" "Backend" || echo -e "${RED}Backend service check failed${NC}"
call_service_api $FRONTEND_IP $FRONTEND_PORT "status" "Frontend" || echo -e "${RED}Frontend service check failed${NC}" 
call_service_api $DATABASE_IP $DATABASE_PORT "status" "Database" || echo -e "${RED}Database service check failed${NC}"
call_service_api $MESSAGING_IP $MESSAGING_PORT "status" "Messaging" || echo -e "${RED}Messaging service check failed${NC}"

echo -e "${BLUE}===============================================${NC}"
echo -e "${GREEN}Service startup complete.${NC}"
echo -e "Frontend should be available at: http://$FRONTEND_IP:7012"
echo -e "Backend should be available at: http://$BACKEND_IP:8001"
