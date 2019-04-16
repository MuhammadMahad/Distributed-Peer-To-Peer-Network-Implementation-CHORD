from classes.node import Node
import socket
from _thread import *
import threading
from threading import Thread
import pickle
import sys
import hashlib
from classes.command import Command
from random import randint



my_ip = sys.argv[1]
my_port = int(sys.argv[2])

node = None


#interface
created = False
joined = False
while True:
    print('1. Create')
    print('2. Join')
    print('3. Download')

    choice = input('Enter command number to proceed:\n')

    if(choice == '1' and created == False):

        node = Node.create(node, my_ip, int(my_port), None, node)
        #node = Node.create(node, my_ip, int(my_port), None, None)
        #create a new node/ring. call create
        created = True
    elif(choice == '2' and joined == False):

        n_ip = input('Enter IP of existing node in ring:\n')
        n_port = input('Enter port of existing node in ring:\n')
        #name = ip.join(str(port))
        #name = my_ip + str(my_port)

        node = Node.join(node, my_ip, my_port, n_ip, n_port)
        # join_data = {
        #
        #     'name': name
        #
        # }
        #
        # command = pickle.dumps(Command('JOIN', join_data))
        # #node_port = randint(1000, 9999)
        #
        # socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #
        # addr = (ip, int(port))
        # socket.connect(addr)
        #
        # socket.send(command)
        #
        # s_p = pickle.loads(socket.recv(1024))
        # #node_port = randint(1000, 9999)
        # #print(my_port)
        # node = Node.create(node, my_ip, my_port,s_p.data['predecessor'],s_p.data['successor']  )
        # print('node created')
        # print(s_p.data)
        # created = True
        # """
        # receive on socket
        # response with be successor and predecessor node
        # Node(with response successor and precessor)
        # fix fingers
        # """

        joined = True
    elif(choice == '3'):
        pass


