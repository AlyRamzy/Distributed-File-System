import zmq
import random
import sys
import time

ip = sys.argv[1]
port = sys.argv[2]

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://" + ip + ":" + port)

while True:
    # print(ip,port)
    socket.send_pyobj({"ip" : ip})
    time.sleep(1)
