import zmq
import sys


def client_upload():
    context = zmq.Context()
    print("Connecting to server...")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://192.168.43.118:%s" % 5000)
    print("Sending request ")

    send_dict = {"operation": "Upload"}

    socket.send_pyobj(send_dict)
    #  Get the reply.
    message = socket.recv_pyobj()
    print("Received reply", message)
    if message["status"] == True:
        socket2 = context.socket(zmq.PAIR)
        socket2.connect("tcp://" + message["ip"] + ":%s" % message["port"])
        file_name = "small.mp4"
        with open(file_name,'rb') as File:
            msg = File.read()
        send_dict = {"operation": "Upload", "file_name": file_name, "file" : str(msg)}
        socket2.send_pyobj(send_dict)
        socket2.close()

def client_download():
    context = zmq.Context()
    print("Connecting to server...")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://192.168.43.118:%s" % 5000)
    print("Sending request ")

    send_dict = {"operation": "Download", "file_name": "small.mp4"}

    socket.send_pyobj(send_dict)
    #  Get the reply.
    message = socket.recv_pyobj()
    print("Received reply", message)
    if message["status"] == True:
        socket2 = context.socket(zmq.PAIR)
        socket2.connect("tcp://" + message["ip"] + ":%s" % message["port"])
        send_dict = {"operation": "Download", "file_name": "small.mp4"}
        socket2.send_pyobj(send_dict)
        send_dict = socket2.recv_pyobj()
        print("recieved file")
        print(send_dict)
        socket2.close()

client_upload()
client_download()
