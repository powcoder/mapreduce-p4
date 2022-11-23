https://powcoder.com
代写代考加微信 powcoder
Assignment Project Exam Help
Add WeChat powcoder
"""Utils file.

This file is to house code common between the Manager and the Worker

"""
import socket
import json


def send_msg(port, msg, msg_type):
    """Do something."""
    if msg_type == "tcp":
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("localhost", port))
            # send a message
            message = json.dumps(msg)
            sock.sendall(message.encode('utf-8'))
    elif msg_type == "udp":
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("localhost", port))
            # send a message
            message = json.dumps(msg)
            sock.sendall(message.encode('utf-8'))
