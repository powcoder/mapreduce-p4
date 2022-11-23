https://powcoder.com
代写代考加微信 powcoder
Assignment Project Exam Help
Add WeChat powcoder
"""worker part."""
import os
import logging
import json
import time
import socket
import subprocess
import threading
import click
import mapreduce.utils

# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Worker:
    """Do something."""

    def __init__(self, manager_port, manager_hb_port, worker_port):
        """Do something."""
        logging.info("Starting worker:%s", worker_port)
        logging.info("Worker:%s PWD %s", worker_port, os.getcwd())
        self.worker_port = worker_port
        self.worker_pid = os.getpid()
        self.manager_port = manager_port
        self.manager_hb_port = manager_hb_port
        self.shutdown = False
        self.thread_heartbeat = threading.Thread(target=self.heartbeat)
        thread_listen = threading.Thread(target=self.listen)
        thread_listen.start()
        self.send_register()
        thread_listen.join()

    def listen(self):
        """Do something."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.worker_port))
            sock.listen()
            # Socket accept() and recv() will block for a
            # maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            while not self.shutdown:
                # Wait for a connection for 1s.  The socket
                # library avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, addy = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", addy[0])

                # Receive data, one chunk at a time.  If recv()
                # times out before we can
                # read a chunk, then go back to the top of the
                # loop and try again.
                # When the client closes the connection, recv()
                # returns empty data,
                # which breaks out of the loop.  We make a
                # simplifying assumption that
                # the client will always cleanly close the connection.
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            date = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not date:
                            break
                        message_chunks.append(date)

                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_byte = b''.join(message_chunks)
                message_str = message_byte.decode("utf-8")

                try:
                    message_dicty = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                message_type = message_dicty["message_type"]

                if message_type == "register_ack":
                    self.thread_heartbeat.start()
                elif message_type == "shutdown":
                    self.shutdown = True
                elif message_type == "new_worker_task":
                    self.new_worker_task(message_dicty)
                elif message_type == "new_sort_task":
                    self.new_sort_task(message_dicty)

    def new_sort_task(self, message_dict):
        """Do something."""
        files = []
        for in_file in message_dict["input_files"]:
            with open(in_file, "r", encoding="utf-8") as in_files:
                for infile in in_files:
                    files.append(infile)
        files.sort()
        with open(message_dict["output_file"], "w+",
                  encoding="utf-8") as out_files:
            for file in files:
                out_files.write(f"{file}")
        msg = {"message_type": "status",
               "output_file": message_dict["output_file"],
               "status": "finished",
               "worker_pid": self.worker_pid}
        mapreduce.utils.send_msg(self.manager_port, msg, "tcp")

    def new_worker_task(self, message_dict):
        """Do something."""
        output_file = []
        for in_file in message_dict["input_files"]:
            ofile = message_dict["output_directory"]
            ofile += "/" + os.path.basename(in_file)
            with open(in_file, "r", encoding="utf-8") as input_exe_file:
                with open(ofile, "w+", encoding="utf-8") as output_exe_file:
                    subprocess.run(message_dict["executable"], check=False,
                                   stdin=input_exe_file,
                                   stdout=output_exe_file)
            output_file.append(ofile)
        msg = {"message_type": "status",
               "output_files": output_file,
               "status": "finished",
               "worker_pid": self.worker_pid}
        mapreduce.utils.send_msg(self.manager_port, msg, "tcp")

    def heartbeat(self):
        """Do something."""
        while not self.shutdown:
            msg = {"message_type": "heartbeat",
                   "worker_pid": self.worker_pid}
            mapreduce.utils.send_msg(self.manager_hb_port, msg, "udp")
            time.sleep(2)

    def send_register(self):
        """Do something."""
        message = {"message_type": "register", "worker_host": "localhost",
                   "worker_port": self.worker_port,
                   "worker_pid": self.worker_pid}
        mapreduce.utils.send_msg(self.manager_port, message, "tcp")


@click.command()
@click.argument("manager_port", nargs=1, type=int)
@click.argument("manager_hb_port", nargs=1, type=int)
@click.argument("worker_port", nargs=1, type=int)
def main(manager_port, manager_hb_port, worker_port):
    """Instantiate worker object for this process."""
    Worker(manager_port, manager_hb_port, worker_port)


if __name__ == '__main__':
    main()
