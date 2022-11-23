https://powcoder.com
代写代考加微信 powcoder
Assignment Project Exam Help
Add WeChat powcoder
"""manager part."""
import os
import logging
import json
import shutil
import time
import socket
from pathlib import Path
from contextlib import ExitStack
import heapq
import threading
import glob
import datetime
import click
import mapreduce.utils


# Configure logging
logging.basicConfig(level=logging.DEBUG)


class Manager:
    """class manager docstring."""

    def __init__(self, port, hb_port):
        """Do something."""
        logging.info("Starting manager:%s", port)
        logging.info("Manager:%s PWD %s", port, os.getcwd())
        self.signals = {}
        self.signals["shutdown"] = False
        self.signals["job_counter"] = 0
        self.signals["num_reducer"] = 1
        self.signals["tasks_done"] = 0
        self.signals["total_tasks"] = 0
        self.signals["port"] = port
        self.signals["hb_port"] = hb_port
        self.workers = {}  # pid -> port, state, task, prev_heartbeat
        self.queue = []
        self.task_queue = []
        self.curr_job = {}  # manager's current job
        self.stage = None

        tmp_p = Path("tmp/")
        tmp_p.mkdir(parents=True, exist_ok=True)
        for j in tmp_p.glob('job-*'):
            shutil.rmtree(j, ignore_errors=True)

        thread_listen = threading.Thread(target=self.listen)
        thread_listen.start()
        thread_heartbeat = threading.Thread(target=self.heartbeat)
        thread_heartbeat.start()
        thread_fault = threading.Thread(target=self.fault_tolerance)
        thread_fault.start()

        thread_listen.join()
        thread_heartbeat.join()
        thread_fault.join()

    def heartbeat(self):
        """Do something."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.signals["hb_port"]))
            sock.settimeout(1)

            # No sock.listen() since UDP doesn't establish connections like TCP

            # Receive incoming UDP messages
            while not self.signals["shutdown"]:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                if message_dict["message_type"] == "heartbeat":
                    if message_dict["worker_pid"] in self.workers:
                        self.workers[message_dict["worker_pid"]][
                            "prev_heartbeat"] = datetime.datetime.now()
                        # print("new", message_dict["worker_pid"])

    def fault_tolerance(self):
        """Do something."""
        while not self.signals["shutdown"]:
            for worker in self.workers.items():
                if worker[1]["state"] != "dead":
                    curr_time = datetime.datetime.now()
                    difference = (curr_time -
                                  worker[1]["prev_heartbeat"]).seconds
                    if difference >= 10:
                        self.reassign(worker[1])
            time.sleep(1)

    def reassign(self, worker):
        """Do something."""
        if worker["state"] == "busy":
            unfinished_task = worker["task"]
            self.task_queue.append(unfinished_task)
            if self.get_avlb_workers() != 0:
                for temp in self.workers.items():
                    if temp[1]["state"] == "ready":
                        self.assign(temp[0])
                        break
        worker["state"] = "dead"

    def listen(self):
        """Do something."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("localhost", self.signals["port"]))
            sock.listen()
            # Socket accept() and recv() will block for a maximum of 1 second.
            # If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            while not self.signals["shutdown"]:
                # Wait for a connection for 1s.  The socket library
                # avoids consuming
                # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
                # Receive data, one chunk at a time.  If recv()
                # times out before we can
                # read a chunk, then go back to the top of the
                # loop and try again.
                # When the client closes the connection,
                # recv() returns empty data,
                # which breaks out of the loop.
                # We make a simplifying assumption that
                # the client will always cleanly close the connection.
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)

                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                message_type = message_dict["message_type"]

                if message_type == "shutdown":
                    self.signals["shutdown"] = True
                    self.shut_all_down()
                elif message_type == "register":
                    self.register(message_dict)
                elif message_type == "new_manager_job":
                    self.new_manager_job(message_dict)
                elif message_type == "status":
                    self.status(message_dict)

    def status(self, message_dict):
        """Do something."""
        self.workers[message_dict["worker_pid"]]["state"] = "ready"
        self.signals["tasks_done"] += 1
        if self.stage is not None and self.task_queue:
            self.assign(message_dict["worker_pid"])
        elif self.signals["total_tasks"] == self.signals["tasks_done"]:
            self.signals["tasks_done"] = 0
            self.signals["num_reducer"] = 1
            if len(self.task_queue) == 0 and self.stage == "Mapping":
                logging.info("Manager:%s end map stage", self.signals["port"])
                self.grouping()
            elif len(self.task_queue) == 0 and self.stage == "Grouping":
                self.distribute()
                logging.info("Manager:%s end group stage",
                             self.signals["port"])
                self.reducing()
            elif len(self.task_queue) == 0 and self.stage == "Reducing":
                logging.info("Manager:%s end reduce stage",
                             self.signals["port"])

                cur_p = Path(self.curr_job["output_directory"])
                cur_p.mkdir(parents=True, exist_ok=True)
                curr_job_id = self.curr_job["job_id"]
                sorted_files = glob.glob(f"tmp/job-{curr_job_id}" +
                                         "/reducer-output/*")
                index = 1
                for sorted_file in enumerate(sorted_files):
                    new_file = self.curr_job["output_directory"] + \
                                 "/outputfile"
                    new_file += str(index).zfill(2)
                    shutil.move(sorted_file[1], new_file)
                    index += 1
                if self.queue:
                    self.curr_job = self.queue.pop()
                    self.mapping()

    def new_manager_job(self, message_dict):
        """Do something."""
        num = self.signals["job_counter"]
        job_path = f"tmp/job-{num}"
        mapper = Path(job_path + "/mapper-output")
        mapper.mkdir(parents=True, exist_ok=True)
        grouper = Path(job_path + "/grouper-output")
        grouper.mkdir(parents=True, exist_ok=True)
        reducer = Path(job_path + "/reducer-output")
        reducer.mkdir(parents=True, exist_ok=True)

        has_available_worker = False
        for worker in self.workers.items():
            if worker[1]["state"] == "ready":
                has_available_worker = True
                break

        current_job = message_dict.copy()
        current_job["job_id"] = self.signals["job_counter"]
        self.signals["job_counter"] += 1
        if not has_available_worker or self.curr_job:
            self.queue.append(current_job)
        else:
            self.curr_job = current_job
            self.mapping()

    def register(self, message_dict):
        """Do something."""
        pid = message_dict["worker_pid"]
        self.workers[pid] = {}
        self.workers[pid]["state"] = "ready"
        self.workers[pid]["port"] = message_dict["worker_port"]
        self.workers[pid]["prev_heartbeat"] = datetime.datetime.now()

        msg = message_dict.copy()
        msg["message_type"] = "register_ack"
        mapreduce.utils.send_msg(self.workers[pid]["port"],
                                 msg, "tcp")

        if len(self.workers) == 1 and self.queue:
            self.mapping()
        if self.task_queue and self.stage is not None:
            self.assign(message_dict["worker_pid"])

    def shut_all_down(self):
        """Do something."""
        msg = {"message_type": "shutdown"}
        for worker in self.workers.items():
            if worker[1]["state"] != "dead":
                mapreduce.utils.send_msg(worker[1]["port"],
                                         msg, "tcp")

    def distribute(self):
        """Do something."""
        cur_job_id = self.curr_job["job_id"]
        in_files = glob.glob(f"tmp/job-{cur_job_id}" +
                             "/grouper-output/sorted*")
        with ExitStack() as stack:
            sorted_files = [stack.enter_context(open(fname, encoding="utf-8"))
                            for fname in in_files]
            reduced_files = [stack.enter_context(
                open(f"tmp/job-{cur_job_id}" +
                     "/grouper-output/reduce" + str(index).zfill(2), "w+",
                     encoding="utf-8")) for index in
                range(1, int(self.curr_job["num_reducers"]) + 1)]

            # sort by keys, round robin
            curr_key = None
            num_keys = 0
            for line in heapq.merge(*sorted_files):
                key = line.split()[0]
                if key != curr_key:
                    num_keys += 1
                    curr_key = key
                reduced_files[num_keys % self.curr_job["num_reducers"] -
                              1].write(line)

    def reducing(self):
        """Do something."""
        logging.info("Manager:%s begin reduce stage", self.signals["port"])
        self.stage = "Reducing"
        cur_job_id = self.curr_job["job_id"]
        in_files = sorted(glob.glob(f"tmp/job-{cur_job_id}" +
                                    "/grouper-output/reduce*"))
        index = 0
        self.signals["total_tasks"] = self.curr_job["num_reducers"]
        for _ in range(0, self.curr_job["num_reducers"]):
            self.task_queue.append([])
        for file in enumerate(in_files):
            self.task_queue[index %
                            self.curr_job["num_reducers"]].append(file[1])
            index += 1
        for worker in self.workers.items():
            if worker[1]["state"] == "ready" and self.task_queue:
                self.assign(worker[0])

    def grouping(self):
        """Do something."""
        logging.info("Manager:%s begin group stage", self.signals["port"])
        # partition if there are more files than workers
        self.stage = "Grouping"
        cur_job_id = self.curr_job["job_id"]
        output_dir = f"tmp/job-{cur_job_id}" + \
            "/mapper-output/*"
        sorted_files = sorted(glob.glob(output_dir))
        index = 0

        # get nbumber of available workers
        queue_length = min(self.get_avlb_workers(), len(sorted_files))
        self.signals["total_tasks"] = queue_length
        for _ in range(0, queue_length):
            self.task_queue.append([])
        for file in enumerate(sorted_files):
            self.task_queue[index % queue_length].append(file[1])
            index += 1

        # assign work
        for worker in self.workers.items():
            if worker[1]["state"] == "ready" and self.task_queue:
                self.assign(worker[0])

    def mapping(self):
        """Do something."""
        logging.info("Manager:%s begin map stage", self.signals["port"])
        self.stage = "Mapping"
        in_files = sorted(glob.glob(self.curr_job["input_directory"] + "/*"))
        index = 0
        for _ in range(0, self.curr_job["num_mappers"]):
            self.task_queue.append([])
        for file in enumerate(in_files):
            self.task_queue[index %
                            self.curr_job["num_mappers"]].append(file[1])
            index += 1
        self.signals["total_tasks"] = self.curr_job["num_mappers"]
        for worker in self.workers.items():
            if worker[1]["state"] == "ready" and self.task_queue:
                self.assign(worker[0])

    def assign(self, pid):
        """Do something."""
        curr_task = self.task_queue.pop(0)
        self.workers[pid]["task"] = curr_task
        cur_job_id = self.curr_job["job_id"]
        if self.stage == "Mapping":
            output = f"tmp/job-{cur_job_id}" + "/mapper-output"
            message = {"message_type": "new_worker_task",
                       "input_files": curr_task,
                       "executable": self.curr_job["mapper_executable"],
                       "output_directory": output,
                       "worker_pid": pid}
        elif self.stage == "Grouping":
            output = f"tmp/job-{cur_job_id}" + \
                        "/grouper-output/sorted" + \
                        str(self.signals["num_reducer"]).zfill(2)
            self.signals["num_reducer"] += 1
            message = {
                "message_type": "new_sort_task",
                "input_files": curr_task,
                "output_file": output,
                "worker_pid": pid}
        elif self.stage == "Reducing":
            output = f"tmp/job-{cur_job_id}" + "/reducer-output"
            message = {"message_type": "new_worker_task",
                       "input_files": curr_task,
                       "executable": self.curr_job["reducer_executable"],
                       "output_directory": output,
                       "worker_pid": pid}
        mapreduce.utils.send_msg(self.workers[pid]["port"], message, "tcp")
        self.workers[pid]["state"] = "busy"

    def get_avlb_workers(self):
        """Do something."""
        num_available_workers = 0
        for worker in self.workers.items():
            if worker[1]["state"] == "ready":
                num_available_workers += 1
        return num_available_workers


@click.command()
@click.argument("port", nargs=1, type=int)
@click.argument("hb_port", nargs=1, type=int)
def main(port, hb_port):
    """Do something."""
    Manager(port, hb_port)


if __name__ == '__main__':
    main()
