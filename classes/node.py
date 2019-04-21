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
        self.files = []

    # ask node n to find the successor of id
    def find_successor(self, node_id):
        if self.id < node_id <= self.successor.id:  # or self.successor.id <= node_id:
            # successor = {
            #     "successor": self.finger_table[0]
            # }
            return self.successor
        else:
            max_less_than_k = self.closest_preceding_node(node_id)
            if max_less_than_k.id == self.id:
                return max_less_than_k.successor

            successor_data = {
                'hash': node_id
            }
            successor_command = Command('FIND_SUCCESSOR', successor_data)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, max_less_than_k.ip, max_less_than_k.port, successor_command)
            receive_command_data = receive_command(s)
            return receive_command_data.data["successor"]

    def closest_preceding_node(self, node_id):
        i = 0
        while i < len(self.finger_table) and i < DHT_BITS:
            if between(self.id, self.finger_table[i].id, node_id):
                return self.finger_table[i]
            i += 1
        return self

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

    def store_file(self, filename, value):
        filename_data = {

            "name": filename
        }

        filename_successor_command = Command('FIND_SUCCESSOR', filename_data)

        n_ip = self.successor.ip
        n_port = self.successor.port

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_command(s, n_ip, n_port, filename_successor_command)

        filename_successor_command_receive = receive_command(s)

        filedict = {

            filename: value

        }

        file_successor = filename_successor_command_receive.data["successor"]

        if file_successor.id == self.id:
            print('file successor:')
            print(file_successor.id)
            self.files.append(filedict)


        append_file_command = Command('APPEND', filedict)

        if file_successor.predecessor.id:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, file_successor.predecessor.ip, file_successor.predecessor.port, append_file_command)
        if file_successor.predecessor.predecessor.id:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, file_successor.predecessor.predecessor.ip, file_successor.predecessor.predecessor.port, append_file_command)


        else:
            print('file successor:')
            print(file_successor.id)



            append_file_command = Command('APPEND', filedict)

        if file_successor.id:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, file_successor.ip, file_successor.port , append_file_command)
        if file_successor.predecessor.id:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, file_successor.predecessor.ip, file_successor.predecessor.port, append_file_command)
        if file_successor.predecessor.predecessor.id:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, file_successor.predecessor.predecessor.ip, file_successor.predecessor.predecessor.port, append_file_command)

    def download_file(self, filename_to_download):

        filename_data = {

            "name": filename_to_download
        }

        filename_successor_command = Command('FIND_SUCCESSOR', filename_data)

        n_ip = self.successor.ip
        n_port = self.successor.port

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_command(s, n_ip, n_port, filename_successor_command)

        filename_successor_command_receive = receive_command(s)

        file_successor = filename_successor_command_receive.data["successor"]

        get_file_data = {
            "file_name" : filename_to_download
        }

        file_download_command = Command('GET_FILE', get_file_data)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_command(s, file_successor.predecessor.ip, file_successor.predecessor.port, file_download_command)

        file_download_command_recieve = receive_command(s)

        file_value = file_download_command_recieve.data[filename_to_download]

        return file_value

    # called periodically. verifies n’s immediate
    # successor, and tells the successor about n.

    def stabilize(self):
        # if self.successor.predecessor:
        #
        #     # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     # send_command(self.successor.ip, self.successor.port, )
        #
        #     predecessor = self.successor.predecessor
        #     if self.id < predecessor.id < self.successor.id or predecessor.id > self.successor.id:
        #         self.successor = predecessor

        if self.id == self.successor.id:
            predecessor = self.successor.predecessor
        else:
            command = Command('PREDECESSOR', {})
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s, self.successor.ip, self.successor.port, command)
            receive_predecessor_data = receive_command(s)
            predecessor = receive_predecessor_data.data["predecessor"]
            self.successor.predecessor = predecessor
        if predecessor and between(self.id, predecessor.id, self.successor.id):
            self.successor = predecessor

        if self.successor.id == self.id:
            # self.notify(self)
            return
        notify_data = {
            "node": self
        }
        notify_command = Command('NOTIFY', notify_data)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_command(s, self.successor.ip, self.successor.port, notify_command)

        #print("After Stabilize #")
        #print(self.id, self.successor.id, self.predecessor.id if self.predecessor else None)
        #print("After Stabilize #")

    # n' thinks it might be our predecessor
    def notify(self, existing_node):
        if self.predecessor is None or between(self.predecessor.id, existing_node.id,
                                               self.id):  # or self.predecessor.id == self.id:
            self.predecessor = existing_node

        # elif self.predecessor and existing_node and self.predecessor.id == existing_node.id:
        #     self.predecessor.predecessor = existing_node.predecessor

    # called periodically. refreshes finger table entries.
    # next stores the index of the next finger to fix.

    def fix_fingers(self):
        for i in range(DHT_BITS):
            node_id = self.id + (2 ** i) % (2 ** DHT_BITS)
            successor = self.find_successor(node_id)
            try:
                self.finger_table[i] = successor
            except IndexError:
                self.finger_table.append(successor)

        # for finger in self.finger_table:
        #     print("Hash", finger.id)

        #print("After Fix Fingers #")
        #print(self.id, self.successor.id, self.predecessor.id if self.predecessor else None)
        #print("After Fix Fingers #")

    # called periodically. checks whether predecessor has failed.
    def check_predecessor(self):
        if self.predecessor != None:
            try:
                alive_data = {
                    "alive": "Are you alive?"
                }
                alive_command = Command('ALIVE', alive_data )
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                send_command(s,self.predecessor.ip, self.predecessor.port, alive_command)
                receive_command(s)
            except Exception as e:
                print(e)
                self.predecessor = None




def stabilize_thread(node):
    while True:
        try:
            node.stabilize()
            time.sleep(3)
        except:
            pass

def fix_fingers_thread(node):
    while True:
        try:
            node.fix_fingers()
            time.sleep(6)
        except:
            pass
def check_predecessor_thread(node):
    while True:
        try:
            node.check_predecessor()
            time.sleep(3)
        except:
            pass

def send_command(s, ip_to_send_to, port_to_send_to, command_to_send):
    command_to_send = pickle.dumps(command_to_send)
    address = (ip_to_send_to, int(port_to_send_to))
    s.connect(address)

    s.send(command_to_send)


def receive_command(s):
    received_command = pickle.loads(s.recv(1024 * 2000))
    return received_command


def listening_server(node):
    start_new_thread(stabilize_thread, (node,))
    start_new_thread(fix_fingers_thread, (node,))
    start_new_thread(check_predecessor_thread, (node,))
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
        # print("Waiting for a connection, Server Started")

        conn, addr = s.accept()

        # print("Connected to:", addr)

        command = pickle.loads(conn.recv(1024 * 2000))

        if not command:
            print("no data received in command")
            break

        if command.type == 'FIND_SUCCESSOR':
            # print('Receiving FIND_SUCCESSOR Command')
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
            # print('Receiving NOTIFY Command')
            _node = command.data['node']
            node.notify(_node)

        if command.type == 'PREDECESSOR':
            # print('Receiving NOTIFY Command')
            predecessor = node.predecessor
            predecessor_data = {
                "predecessor": predecessor
            }
            predecessor_command_send = Command('SENDING_PREDECESSOR', predecessor_data)
            conn.send(pickle.dumps(predecessor_command_send))

        if command.type == 'ALIVE':
            # print('Receiving ALIVE Command')

            alive_response_data = {
                "alive": True
            }
            alive_response_send = Command('AM ALIVE', alive_response_data)
            conn.send(pickle.dumps(alive_response_data))

        if command.type == 'APPEND':
            print('Receiving APPEND Command')

            node.files.append(command.data)
            node.files = [dict(t) for t in {tuple(d.items()) for d in node.files}]

        if command.type == 'GET_FILE':
            print('Receiving GET_FILE Command')
            download_file_dict = {}
            recieved_filename = command.data["file_name"]
            for i in range(len(node.files)):
                if recieved_filename in node.files[i]:
                    download_file_dict = node.files[i]
                    break


            download_file_send_back_command = Command('GETTING_FILE', download_file_dict)
            conn.send(pickle.dumps(download_file_send_back_command))




def hashfunc(name):
    index = (int((hashlib.sha1(name.encode())).hexdigest(), 16)) % (2 ** DHT_BITS)
    return index


def between(node1_id, node2_id, node3_id):
    if node1_id < node3_id:
        return node1_id < node2_id and node2_id < node3_id
    else:
        return node1_id < node2_id or node2_id < node3_id
