import socket
from _thread import *
import threading
from threading import Thread
import pickle
from classes.command import Command
import hashlib, time
from constants import DHT_BITS


class Node:

    def __init__(self, ip, port, successor):
        self.port = port
        self.ip = ip
        self.predecessor = None
        if successor is None:
            self.successor = self
        else:
            self.successor = successor

        self.name = (ip + str(port))
        # id or h
        self.id = hashfunc(self.name)
        self.finger_table = [self.successor]

    # ask node n to find the successor of id
    def find_successor(self, node_id):
        if self.id < node_id <= self.finger_table[0].id:
            # successor = {
            #     "successor": self.finger_table[0]
            # }
            return self.finger_table[0]
        else:
            max_less_than_k = self.closest_preceding_node(node_id)
            if max_less_than_k.id == self.id:
                return max_less_than_k

            successor_data = {
                'hash': node_id
            }
            successor_command = Command('FIND_SUCCESSOR', successor_data)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, max_less_than_k.ip, max_less_than_k.port, successor_command)
            receive_command_data = receive_command(s)
            return receive_command_data.data["successor"]

    def closest_preceding_node(self, node_id):
        max_less_than_k = self.finger_table[0]
        for i in range(DHT_BITS):
            if i < len(self.finger_table) and self.finger_table[i].id > max_less_than_k.id and max_less_than_k.id < node_id:
                max_less_than_k = self.finger_table[i]
        return max_less_than_k

    @staticmethod
    # create a new Chord ring
    def create(ip, port, successor=None):
        node = Node(ip, port, successor)
        listening_server(node)
        return node

    @staticmethod
    # join a Chord ring containing node n
    def join(my_ip, my_port, n_ip, n_port):

        my_name = my_ip + str(my_port)
        successor_data = {
            "name": my_name
        }
        successor_command = Command('FIND_SUCCESSOR', successor_data)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_command(s, n_ip, n_port, successor_command)

        successor_command_receive = receive_command(s)

        successor = successor_command_receive.data["successor"]
        node = Node.create(my_ip, my_port, successor)
        return node

    # called periodically. verifies n’s immediate
    # successor, and tells the successor about n.

    def stabilize(self):
        if self.successor and self.successor.predecessor:
            predecessor = self.successor.predecessor
            if self.id < predecessor.id < self.successor.id:
                self.successor = predecessor
        if self.successor.id == self.id:
            self.notify(self)
            return
        notify_data = {
            "node": self
        }
        notify_command = Command('NOTIFY', notify_data)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_command(s, self.successor.ip, self.successor.port, notify_command)

    # n' thinks it might be our predecessor
    def notify(self, existing_node):
        if self.predecessor is None or self.predecessor.id < existing_node.id < self.id:
            self.predecessor = existing_node

    # called periodically. refreshes finger table entries.
    # next stores the index of the next finger to fix.

    def fix_fingers(self):
        for i in range(DHT_BITS):
            node_id = self.id + (2 ** i)
            successor = self.find_successor(node_id)
            try:
                self.finger_table[i] = successor
            except IndexError:
                self.finger_table.append(successor)

        for finger in self.finger_table:
            print("Hash", finger.id)

    # called periodically. checks whether predecessor has failed.
    def check_predecessor(self):
        pass


def stabilize_thread(node):
    while True:
        node.stabilize()
        time.sleep(2)


def fix_fingers_thread(node):
    while True:
        node.fix_fingers()
        time.sleep(4)


def send_command(s, ip_to_send_to, port_to_send_to, command_to_send):
    command_to_send = pickle.dumps(command_to_send)
    address = (ip_to_send_to, int(port_to_send_to))
    s.connect(address)

    s.send(command_to_send)


def receive_command(s):
    received_command = pickle.loads(s.recv(1024*10))
    return received_command


def listening_server(node):
    start_new_thread(fix_fingers_thread, (node,))
    start_new_thread(stabilize_thread, (node,))
    start_new_thread(threaded_listen, (node,))


def threaded_listen(node):
    server = node.ip
    port = node.port

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        s.bind((server, port))
    except socket.error as e:
        print(str(e))

    s.listen(5)

    while True:
        print("Waiting for a connection, Server Started")

        conn, addr = s.accept()

        print("Connected to:", addr)

        command = pickle.loads(conn.recv(1024*10))

        if not command:
            print("no data received in command")
            break

        if command.type == 'FIND_SUCCESSOR':
            print('Receiving FIND_SUCCESSOR Command')
            if "name" in command.data:
                node_id = hashfunc(command.data["name"])
            else:
                node_id = command.data["hash"]
            n_successor = node.find_successor(node_id)
            successor_data = {
                "successor": n_successor
            }
            successor_command_send = Command('SENDING_SUCCESSOR', successor_data)
            conn.send(pickle.dumps(successor_command_send))

        if command.type == 'NOTIFY':
            print('Receiving NOTIFY Command')
            _node = command.data['node']
            node.notify(_node)



def hashfunc(name):
    index = (int((hashlib.sha1(name.encode())).hexdigest(), 16)) % (2 ** DHT_BITS)
    print("Hash: {0}", index)
    return index

