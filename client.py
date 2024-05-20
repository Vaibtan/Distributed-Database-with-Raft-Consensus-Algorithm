import threading
import time
from random import randint, random
import zmq
import signal
import sys
import time
from threading import Thread, Lock
import os
import utils
import datetime
from queue import Queue
import traceback

signal.signal(signal.SIGINT, signal.SIG_DFL)

#ip = sys.argv[1]
#port = sys.argv[2]
#ip_address = ip + ":" + port
partitions = "[['127.0.0.1:5001', '127.0.0.1:5002', '127.0.0.1:5003','127.0.0.1:5004','127.0.0.1:5005']]"
partitions = eval(partitions)

nodes = [] 
#for each node in partition[0] create and store a socket in nodes
for node in partitions[0]:
    try:
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://localhost:{node.split(':')[1]}")
        nodes.append(socket)
    except Exception as e:
        print(f"Error occurred while connecting to node {node}: {e}")
        continue

leader_info = ""

def connect_to_node(ip, port):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(f"tcp://localhost:{port}")
    #try for 5s to connect to the node if it fails, return 0
    #store current time
    '''t = time.time()
    while(time.time() < t+2):
        try:
            socket.connect(f"tcp://localhost:{port}")
            return socket
        except Exception as e:
            traceback.print_exc(limit=1000)
            continue'''
    return socket

def get_leader_info(leader_info):
    '''if leader_info == "":
        port = partitions[0][0].split(":")[1]
    else:
        port = leader_info[1]
    socket = connect_to_node("localhost", port)
    socket.send_multipart([b'LEADER', b'INFO'])
    message = socket.recv_multipart()
    leader_index = int(message[0].decode('utf-8'))
    leader_ip = partitions[0][leader_index].split(":")[0]
    leader_port = partitions[0][leader_index].split(":")[1]
    leader_info = (leader_ip, leader_port)
    return leader_ip, leader_port'''

    '''context = zmq.Context()

    for node in partitions[0]:
        socket = context.socket(zmq.REQ)
        print("context")
        socket.setsockopt(zmq.RCVTIMEO, 2000)
        print("timeou1")
        socket.setsockopt(zmq.SNDTIMEO, 2000)
        print("timeout2")
        port = node.split(":")[1]
        print(port)
        try:
            print(f"Trying to connect to port {port}...")
            socket.connect(f"tcp://localhost:{port}")
            print(f"Connected to port {port}")
        except zmq.error.Again:
            print(f"Connection to port {port} timed out. Trying next port...")
            continue
        except Exception as e:
            print(f"Error occurred while connecting to port {port}: {e}")
            continue

        try:
            socket.send_multipart([b'LEADER', b'INFO'])
            message = socket.recv_multipart()
            leader_index = int(message[0].decode('utf-8'))
            leader_ip = partitions[0][leader_index].split(":")[0]
            leader_port = partitions[0][leader_index].split(":")[1]
            leader_info = (leader_ip, leader_port)
            return leader_ip, leader_port
        except zmq.error.Again:
            print(f"Receive operation on port {port} timed out. Trying next port...")
            continue
        except Exception as e:
            print(f"Error occurred while receiving message from port {port}: {e}")
            continue
        finally:
            print("close")
            socket.close()'''
    
    for node in nodes:
        node.setsockopt(zmq.RCVTIMEO, 2000)
        node.setsockopt(zmq.SNDTIMEO, 2000)
        try:
            node.send_multipart([b'LEADER', b'INFO'])
            message = node.recv_multipart()
            leader_index = int(message[0].decode('utf-8'))
            leader_ip = partitions[0][leader_index].split(":")[0]
            leader_port = partitions[0][leader_index].split(":")[1]
            leader_info = (leader_ip, leader_port)
            return leader_ip, leader_port
        except zmq.error.Again:
            #print(f"Receive operation on port timed out. Trying next port...")
            continue
        except Exception as e:
            #print(f"Error occurred while getting leader info: {e}")
            continue


while True:
    print("1) Get Leader")
    print("2) SET Operation")
    print("3) GET Operation")
    print("4) Exit")
    choice = int(input("Enter choice: "))
    if choice == 1:
        leader_ip, leader_port = get_leader_info(leader_info)
        print(f"Leader IP: {leader_ip}")
        print(f"Leader Port: {leader_port}")
        
    elif choice == 2:
        key = input("Enter key: ")
        value = input("Enter value: ")
        leader_ip, leader_port = get_leader_info(leader_info)
        socket = connect_to_node(leader_ip, leader_port)
        socket.send_multipart([b'SET', key.encode('utf-8'), value.encode('utf-8')])
        message = socket.recv()
        print(f"Received: {message}")
        
    elif choice == 3:
        key = input("Enter key: ")
        leader_ip, leader_port = get_leader_info(leader_info)
        socket = connect_to_node(leader_ip, leader_port)
        socket.send_multipart([b'GET', key.encode('utf-8')])
        message = socket.recv()
        print(f"Received: {message}")
    elif choice == 4:
        break
    else:
        print("Invalid Choice")
        continue

