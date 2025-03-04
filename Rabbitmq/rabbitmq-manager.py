#!/usr/bin/env python3
"""
RabbitMQ Manager - A tool to manage and report on RabbitMQ installation
"""

import argparse
import subprocess
import socket
import os
import json
import sys
from datetime import datetime


class RabbitMQManager:
    def __init__(self):
        self.hostname = socket.gethostname()
        self.username = os.environ.get('USER', 'unknown')

    def run_command(self, command, shell=False):
        """Run a shell command and return the output."""
        try:
            if shell:
                result = subprocess.run(command, shell=True, check=True, 
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        universal_newlines=True)
            else:
                result = subprocess.run(command, check=True, 
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        universal_newlines=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Command failed: {e}")
            print(f"Error output: {e.stderr}")
            return None

    def check_ssh_status(self):
        """Check if SSH is installed and running."""
        ssh_installed = self.run_command(["dpkg", "-l", "openssh-server"])
        ssh_status = self.run_command(["systemctl", "status", "ssh"])
        
        if ssh_installed and ssh_status:
            print("✅ SSH is installed and running")
            return True
        else:
            print("❌ SSH is not properly installed or not running")
            return False

    def check_rabbitmq_status(self):
        """Check if RabbitMQ is installed and running."""
        rabbitmq_installed = self.run_command(["dpkg", "-l", "rabbitmq-server"])
        rabbitmq_status = self.run_command(["systemctl", "status", "rabbitmq-server"])
        
        if rabbitmq_installed and rabbitmq_status:
            print("✅ RabbitMQ is installed and running")
            print("\nService details:")
            print(rabbitmq_status)
            return True
        else:
            print("❌ RabbitMQ is not properly installed or not running")
            return False

    def check_port_status(self, port):
        """Check if a specific port is open."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('127.0.0.1', port))
        sock.close()
        return result == 0

    def check_firewall_rules(self):
        """Check firewall rules for SSH and RabbitMQ ports."""
        ufw_status = self.run_command(["sudo", "ufw", "status"])
        
        print("\nFirewall Status:")
        print(ufw_status)
        
        ssh_port_open = self.check_port_status(22)
        rabbitmq_amqp_open = self.check_port_status(5672)
        rabbitmq_mgmt_open = self.check_port_status(15672)
        
        print("\nPort Status:")
        print(f"SSH (22): {'✅ Open' if ssh_port_open else '❌ Closed'}")
        print(f"RabbitMQ AMQP (5672): {'✅ Open' if rabbitmq_amqp_open else '❌ Closed'}")
        print(f"RabbitMQ Management (15672): {'✅ Open' if rabbitmq_mgmt_open else '❌ Closed'}")

    def get_rabbitmq_users(self):
        """Get list of RabbitMQ users."""
        users = self.run_command(["sudo", "rabbitmqctl", "list_users"])
        print("\nRabbitMQ Users:")
        print(users)
        return users

    def get_rabbitmq_vhosts(self):
        """Get list of RabbitMQ virtual hosts."""
        vhosts = self.run_command(["sudo", "rabbitmqctl", "list_vhosts"])
        print("\nRabbitMQ Virtual Hosts:")
        print(vhosts)
        return vhosts

    def get_rabbitmq_permissions(self):
        """Get RabbitMQ permissions."""
        permissions = self.run_command(["sudo", "rabbitmqctl", "list_permissions"])
        print("\nRabbitMQ Permissions:")
        print(permissions)
        return permissions

    def get_sudo_access(self):
        """Check if the current user has sudo access."""
        sudo_access = self.run_command("sudo -n true 2>/dev/null && echo 'Yes' || echo 'No'", shell=True)
        passwordless = self.run_command("sudo -n rabbitmqctl status >/dev/null 2>&1 && echo 'Yes' || echo 'No'", shell=True)
        
        print("\nSudo Access:")
        print(f"Has sudo: {'✅ Yes' if sudo_access == 'Yes' else '❌ No'}")
        print(f"Passwordless sudo for RabbitMQ: {'✅ Yes' if passwordless == 'Yes' else '❌ No'}")

    def start_rabbitmq(self):
        """Start RabbitMQ service."""
        result = self.run_command(["sudo", "systemctl", "start", "rabbitmq-server"])
        print("RabbitMQ service started")
        return result

    def stop_rabbitmq(self):
        """Stop RabbitMQ service."""
        result = self.run_command(["sudo", "systemctl", "stop", "rabbitmq-server"])
        print("RabbitMQ service stopped")
        return result

    def restart_rabbitmq(self):
        """Restart RabbitMQ service."""
        result = self.run_command(["sudo", "systemctl", "restart", "rabbitmq-server"])
        print("RabbitMQ service restarted")
        return result

    def generate_report(self, output_file=None):
        """Generate a complete report of the RabbitMQ setup."""
        report = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "hostname": self.hostname,
            "username": self.username,
            "ssh": {
                "installed": self.run_command(["dpkg", "-l", "openssh-server"]) is not None,
                "status": self.run_command(["systemctl", "status", "ssh"]) is not None
            },
            "rabbitmq": {
                "installed": self.run_command(["dpkg", "-l", "rabbitmq-server"]) is not None,
                "status": self.run_command(["systemctl", "status", "rabbitmq-server"]) is not None,
                "service_name": "rabbitmq-server",
                "logs_location": "/var/log/rabbitmq"
            },
            "ports": {
                "ssh_22": self.check_port_status(22),
                "rabbitmq_amqp_5672": self.check_port_status(5672),
                "rabbitmq_management_15672": self.check_port_status(15672)
            },
            "users": self.run_command(["sudo", "rabbitmqctl", "list_users"]),
            "vhosts": self.run_command(["sudo", "rabbitmqctl", "list_vhosts"]),
            "permissions": self.run_command(["sudo", "rabbitmqctl", "list_permissions"]),
            "sudo_access": {
                "has_sudo": self.run_command("sudo -n true 2>/dev/null && echo 'Yes' || echo 'No'", shell=True),
                "passwordless_sudo": self.run_command("sudo -n rabbitmqctl status >/dev/null 2>&1 && echo 'Yes' || echo 'No'", shell=True)
            }
        }
        
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            print(f"Report saved to {output_file}")
        
        # Print formatted report to console
        print("\n" + "="*50)
        print("RabbitMQ Messaging Service Report")
        print("="*50)
        print(f"Generated: {report['timestamp']}")
        print(f"Hostname: {report['hostname']}")
        print(f"Username: {report['username']}")
        print("\n--- SSH Access ---")
        print(f"SSH Installed: {'✅ Yes' if report['ssh']['installed'] else '❌ No'}")
        print(f"SSH Running: {'✅ Yes' if report['ssh']['status'] else '❌ No'}")
        print("\n--- RabbitMQ Service ---")
        print(f"RabbitMQ Installed: {'✅ Yes' if report['rabbitmq']['installed'] else '❌ No'}")
        print(f"RabbitMQ Running: {'✅ Yes' if report['rabbitmq']['status'] else '❌ No'}")
        print(f"Service Name: {report['rabbitmq']['service_name']}")
        print(f"Log Location: {report['rabbitmq']['logs_location']}")
        print("\n--- Port Status ---")
        print(f"SSH (22): {'✅ Open' if report['ports']['ssh_22'] else '❌ Closed'}")
        print(f"RabbitMQ AMQP (5672): {'✅ Open' if report['ports']['rabbitmq_amqp_5672'] else '❌ Closed'}")
        print(f"RabbitMQ Management (15672): {'✅ Open' if report['ports']['rabbitmq_management_15672'] else '❌ Closed'}")
        print("\n--- User Access ---")
        print(f"Has sudo: {'✅ Yes' if report['sudo_access']['has_sudo'] == 'Yes' else '❌ No'}")
        print(f"Passwordless sudo for RabbitMQ: {'✅ Yes' if report['sudo_access']['passwordless_sudo'] == 'Yes' else '❌ No'}")
        print("\n--- Service Control Commands ---")
        print("Check status: sudo systemctl status rabbitmq-server")
        print("Start service: sudo systemctl start rabbitmq-server")
        print("Stop service: sudo systemctl stop rabbitmq-server")
        print("Restart service: sudo systemctl restart rabbitmq-server")
        print("="*50)
        
        return report

    def interactive_menu(self):
        """Display an interactive menu for managing RabbitMQ."""
        while True:
            print("\n" + "="*50)
            print("RabbitMQ Manager - Interactive Menu")
            print("="*50)
            print("1. Check SSH status")
            print("2. Check RabbitMQ status")
            print("3. Check firewall rules")
            print("4. List RabbitMQ users")
            print("5. List RabbitMQ virtual hosts")
            print("6. List RabbitMQ permissions")
            print("7. Check sudo access")
            print("8. Start RabbitMQ service")
            print("9. Stop RabbitMQ service")
            print("10. Restart RabbitMQ service")
            print("11. Generate complete report")
            print("0. Exit")
            print("="*50)
            
            choice = input("Enter your choice: ")
            
            if choice == '1':
                self.check_ssh_status()
            elif choice == '2':
                self.check_rabbitmq_status()
            elif choice == '3':
                self.check_firewall_rules()
            elif choice == '4':
                self.get_rabbitmq_users()
            elif choice == '5':
                self.get_rabbitmq_vhosts()
            elif choice == '6':
                self.get_rabbitmq_permissions()
            elif choice == '7':
                self.get_sudo_access()
            elif choice == '8':
                self.start_rabbitmq()
            elif choice == '9':
                self.stop_rabbitmq()
            elif choice == '10':
                self.restart_rabbitmq()
            elif choice == '11':
                output_file = input("Enter filename for report (leave empty for console only): ")
                if output_file:
                    self.generate_report(output_file)
                else:
                    self.generate_report()
            elif choice == '0':
                print("Exiting RabbitMQ Manager.")
                break
            else:
                print("Invalid choice. Please try again.")
            
            input("\nPress Enter to continue...")


def main():
    parser = argparse.ArgumentParser(description='RabbitMQ Manager Tool')
    parser.add_argument('--check-ssh', action='store_true', help='Check SSH status')
    parser.add_argument('--check-rabbitmq', action='store_true', help='Check RabbitMQ status')
    parser.add_argument('--check-firewall', action='store_true', help='Check firewall rules')
    parser.add_argument('--list-users', action='store_true', help='List RabbitMQ users')
    parser.add_argument('--list-vhosts', action='store_true', help='List RabbitMQ virtual hosts')
    parser.add_argument('--list-permissions', action='store_true', help='List RabbitMQ permissions')
    parser.add_argument('--check-sudo', action='store_true', help='Check sudo access')
    parser.add_argument('--start', action='store_true', help='Start RabbitMQ service')
    parser.add_argument('--stop', action='store_true', help='Stop RabbitMQ service')
    parser.add_argument('--restart', action='store_true', help='Restart RabbitMQ service')
    parser.add_argument('--report', action='store_true', help='Generate complete report')
    parser.add_argument('--output', type=str, help='Output file for report')
    parser.add_argument('--interactive', action='store_true', help='Interactive menu mode')
    
    args = parser.parse_args()
    
    manager = RabbitMQManager()
    
    # If no arguments provided, show help
    if len(sys.argv) == 1:
        parser.print_help()
        print("\nOr run with --interactive for menu mode")
        return
    
    if args.interactive:
        manager.interactive_menu()
        return
    
    if args.check_ssh:
        manager.check_ssh_status()
    
    if args.check_rabbitmq:
        manager.check_rabbitmq_status()
    
    if args.check_firewall:
        manager.check_firewall_rules()
    
    if args.list_users:
        manager.get_rabbitmq_users()
    
    if args.list_vhosts:
        manager.get_rabbitmq_vhosts()
    
    if args.list_permissions:
        manager.get_rabbitmq_permissions()
    
    if args.check_sudo:
        manager.get_sudo_access()
    
    if args.start:
        manager.start_rabbitmq()
    
    if args.stop:
        manager.stop_rabbitmq()
    
    if args.restart:
        manager.restart_rabbitmq()
    
    if args.report:
        manager.generate_report(args.output)


if __name__ == "__main__":
    main()
