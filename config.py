#!/usr/bin/env python3
"""
Configuration file for the distributed services system
Contains all necessary connection parameters for RabbitMQ
"""

# RabbitMQ Configuration (CloudAMQP)
RABBITMQ_CONFIG = {
    'host': 'shark.rmq.cloudamqp.com',
    'port': 5672,
    'vhost': 'cxknnjta',
    'username': 'cxknnjta',
    'password': 'e2T8R0v6YhAwsvHC6dpOQAVO4qq92tTB',
    'use_ssl': True  # CloudAMQP requires SSL/TLS
}

# Service Configuration
SERVICE_CONFIG = {
    'frontend': {
        'name': 'frontend',
        'description': 'Web UI Service'
    },
    'backend': {
        'name': 'backend',
        'description': 'API Service'
    },
    'database': {
        'name': 'database',
        'description': 'Database Service'
    },
    'messaging': {
        'name': 'messaging',
        'description': 'Messaging Service'
    }
}

# Define which exchanges and queues to create
EXCHANGES = [
    {
        'name': 'service_control',
        'type': 'topic',
        'durable': True
    },
    {
        'name': 'service_status',
        'type': 'topic',
        'durable': True
    }
]

# Default queues that should be created for each service
DEFAULT_QUEUES = {
    'control': {
        'exchange': 'service_control',
        'routing_key': 'service.{}.control'
    },
    'status': {
        'exchange': 'service_status',
        'routing_key': 'service.{}.status'
    }
}