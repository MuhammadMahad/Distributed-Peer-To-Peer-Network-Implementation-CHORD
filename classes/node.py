import socket
from _thread import *
import threading
from threading import Thread
import pickle
from classes.command import Command
import hashlib
from constants import DHT_BITS

class Node:

    def __init__(self, ip, port, predecessor, successor):
        self.port = port
        self.ip = ip
        self.predecessor = None
        self.successor = self
        self.name = (ip + str(port))
        self.id = hashfunc((self.name)) #id or h
        self.finger_table = []

    #// ask node n to find the successor of id
    def find_successor(self, id):
        pass

    #// search the local table for the highest predecessor of id
    def closest_preceding_node(self, id):
        pass

    @staticmethod
    #// create a new Chord ring
    def create(self, ip, port):
        node = Node(ip, port, None, self)
        listening_server(node)

    #// join a Chord ring containing node n
    def join(self, existing_node, id): #existing node = n'
        predecessor = None
        successor = existing_node.find_successor(id)
        #call create node and start node's server
        # node = node.create(node, ip, port, )

    #// called periodically. verifies n’s immediate
    #// successor, and tells the successor about n.

    def stabilize(self): #in thread
        pass

    #// n' thinks it might be our predecessor
    def notify(self, existing_node): #existing node = n'
        pass

    #// called periodically. refreshes finger table entries.
    #// next stores the index of the next finger to fix.

    def fix_fingers(self): #in thread
        pass

    #// called periodically. checks whether predecessor has failed.
    def check_predecessor(self): #in thread
        pass




def listening_server(node):



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
            print("no data received")
            break

        if(command.type == 'JOIN'):
            print('Recieving Join Command')



            # indexOnDHT = hashfunc(command.data['name'])
            # print(indexOnDHT)
            # #print(command.data['name'])
            # #print(int(hash.hexdigest(),16))
            # successor_predecessor = node.responseForJoin(indexOnDHT)
            # print(successor_predecessor)
            #
            # s_p = Command(None, successor_predecessor)
            # conn.send(pickle.dumps(s_p))
            # print('sent back')


def hashfunc(name):
    index = (int((hashlib.sha1(name.encode())).hexdigest(),16)) % (2 ** DHT_BITS)
    print('hash of the node that you just made:')
    print(index)
    return index