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
        successor = self.finger_table[0]
        if (id > self.id  and id <= successor[0] ):
            successor = {


                "successor": successor[2]

            }
            return successor
            # return [self, successor[2]]
        else:
            maxLessThanK = successor[0]
            maxLessThanKNode = successor[2]
            for i in range(DHT_BITS):
                if (self.finger_table[i][0] > maxLessThanK and maxLessThanK < id):
                    maxLessThanK = self.finger_table[i][0]
                    maxLessThanKNode = self.finger_table[i][2]
            if (maxLessThanK == successor[0]):
                successor = {


                    "successor": self

                }
                return successor



            successor_data = {

                'name': maxLessThanKNode.name
            }

            successor_command = Command('FIND_SUCCESSOR', successor_data)

            s2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_command(s2, maxLessThanKNode.ip, maxLessThanKNode.port,successor_command)

            return recieve_command(s2)

    #// search the local table for the highest predecessor of id
    # def closest_preceding_node(self, id):
    #     pass

    @staticmethod
    #// create a new Chord ring
    def create(self, ip, port, predecessor, successor):
        node = Node(ip, port, predecessor, successor)
        listening_server(node)

    #// join a Chord ring containing node n
    def join(self, my_ip, my_port, n_ip, n_port): #existing node = n'

        my_name = my_ip + str(my_port)
        successor_data = {

            "name": my_name

        }
        successor_command = Command('FIND_SUCCESSOR', successor_data)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        send_command(s, n_ip, n_port, successor_command)

        successor_command_recv = recieve_command(s)

        successor = successor_command_recv.data["successor"]
        predecessor = None

        node = Node.create(my_ip, my_port, predecessor, successor)
        # id =
        # predecessor = None
        # successor = existing_node.find_successor(id)
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
        start_new_thread(fix_fingers_thread, (self,))


    #// called periodically. checks whether predecessor has failed.
    def check_predecessor(self): #in thread
        pass


def fix_fingers_thread(node):
    pass

    #next = next + 1 ;
    #if (next > m)
    #next = 1 ;
    #finger[next] = find successor(n + 2
    #next−1
    #);



def send_command(s, ip_to_send_to, port_to_send_to, command_to_send):

    command_to_send = pickle.dumps(command_to_send)
    # node_port = randint(1000, 9999)



    addr = (ip_to_send_to, int(port_to_send_to))
    s.connect(addr)

    s.send(command_to_send)

def recieve_command(s):

    recieved_command = pickle.loads(s.recv(1024))

    return recieved_command



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

        if(command.type == 'FIND_SUCCESSOR'):
            print('Recieving FIND_SUCCESSOR Command')

            id = hashfunc(command.data["name"])
            n_successor = node.find_successor(id)

            successor_data = {

                "successor": n_successor

            }

            successor_command_send = ('SENDING_SUCCESSOR', successor_data)

            conn.send(pickle.dumps(successor_command_send))


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
    print('hash:')
    print(index)
    return index