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
            successor = {
                "successor": self.finger_table[0]
            }
            return successor
        else:
            max_less_than_k = self.closest_preceding_node(node_id)
            successor_data = {
                'name': max_less_than_k.name
            }
            successor_command = Command('FIND_SUCCESSOR', successor_data)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, max_less_than_k.ip, max_less_than_k.port, successor_command)
            return receive_command(s)

    def closest_preceding_node(self, node_id):
        max_less_than_k = self.finger_table[0]
        for i in range(DHT_BITS):
            if i < len(self.finger_table) and self.finger_table[i].id > max_less_than_k.id and max_less_than_k.id < node_id:
                max_less_than_k = self.finger_table[i]
        return max_less_than_k

    @staticmethod
    # create a new Chord ring
    def create(ip, port):
        node = Node(ip, port, None)
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
        start_new_thread(stabilize_thread, (self,))

    # n' thinks it might be our predecessor
    def notify(self, existing_node):
        if self.predecessor is None or self.predecessor.id < existing_node.id < self.id:
            self.predecessor = existing_node

    # called periodically. refreshes finger table entries.
    # next stores the index of the next finger to fix.

    def fix_fingers(self):
        start_new_thread(fix_fingers_thread, (self,))

    # called periodically. checks whether predecessor has failed.
    def check_predecessor(self):
        pass


def stabilize_thread(node):
    while True:
        time.sleep(2)
        if node.successor and node.successor.predecessor:
            predecessor = node.successor.predecessor
            if node.id < predecessor.id < node.successor.id:
                node.successor = predecessor
        notify_data = {
            "node": node
        }
        notify_command = Command('NOTIFY', notify_data)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_command(s, node.ip, node.port, notify_command)


def fix_fingers_thread(node):
    while True:
        time.sleep(4)
        for i in range(DHT_BITS):
            successor_data = {
                "hash": node.id + (2 ** i)
            }
            successor_command = Command('FIND_SUCCESSOR', successor_data)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, node.ip, node.port, successor_command)
            successor_command_receive = receive_command(s)
            successor = successor_command_receive.data["successor"]
            node.finger_table[i] = successor

        for finger in node.finger_table:
            print("Hash", finger.id)


def send_command(s, ip_to_send_to, port_to_send_to, command_to_send):
    command_to_send = pickle.dumps(command_to_send)
    address = (ip_to_send_to, int(port_to_send_to))
    s.connect(address)

    s.send(command_to_send)


def receive_command(s):
    received_command = pickle.loads(s.recv(1024))
    return received_command


def listening_server(node):
    node.stabilize()
    node.fix_fingers()
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

        command = pickle.loads(conn.recv(1024))

        if not command:
            print("no data received in command")
            break

        if command.type == 'FIND_SUCCESSOR':
            print('Receiving FIND_SUCCESSOR Command')
            node_id = command.data["hash"]
            if "name" in command.data:
                node_id = hashfunc(command.data["name"])
            n_successor = node.find_successor(node_id)
            successor_data = {
                "successor": n_successor
            }
            successor_command_send = ('SENDING_SUCCESSOR', successor_data)
            conn.send(pickle.dumps(successor_command_send))

        if command.type == 'NOTIFY':
            print('Receiving NOTIFY Command')
            _node = command.data['node']
            node.notify(_node)



def hashfunc(name):
    index = (int((hashlib.sha1(name.encode())).hexdigest(), 16)) % (2 ** DHT_BITS)
    print("Hash: {0}", index)
    return index

