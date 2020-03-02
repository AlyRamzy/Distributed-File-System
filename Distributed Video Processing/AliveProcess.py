import zmq
import random
import sys
import time

ip = "127.0.0.1"
port = int(sys.argv[1])

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://" + ip + ":%s" % port)

while True:
    socket.send_pyobj({"ip" : ip})
    time.sleep(1)