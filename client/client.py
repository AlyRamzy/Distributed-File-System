import zmq
import sys

file_tracker_ip = sys.argv[1]
file_tracker_port = sys.argv[2]
operation = sys.argv[3]
file_name = sys.argv[4]

def client_upload():
    context = zmq.Context()
    print("Connecting to server...")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://" + file_tracker_ip + ":" + file_tracker_port)
    print("Sending request ")

    send_dict = {"operation": "Upload"}

    socket.send_pyobj(send_dict)
    #  Get the reply.
    message = socket.recv_pyobj()
    print("Received reply", message)
    if message["status"] == True:
        socket2 = context.socket(zmq.REQ)
        socket2.connect("tcp://" + message["ip"] + ":" + message["port"])
        with open(file_name,'rb') as File:
            msg = File.read()
        send_dict = {"operation": "Upload", "file_name": file_name, "file" : msg}
        socket2.send_pyobj(send_dict)

        dummy_message = socket2.recv_pyobj()

        socket2.close()

def client_download():
    context = zmq.Context()
    print("Connecting to server...")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://" + file_tracker_ip + ":" + file_tracker_port)
    print("Sending request ")

    send_dict = {"operation": "Download", "file_name": file_name}

    socket.send_pyobj(send_dict)
    #  Get the reply.
    message = socket.recv_pyobj()
    print("Received reply", message)
    if message["status"] == True:
        socket2 = context.socket(zmq.REQ)
        socket2.connect("tcp://" + message["ip"] + ":" + message["port"])
        send_dict = {"operation": "Download", "file_name": file_name}
        socket2.send_pyobj(send_dict)
        send_dict = socket2.recv_pyobj()
        with open(file_name,'wb') as File:
            File.write(send_dict["file"])
        socket2.close()

if operation == "Upload":
    client_upload()
elif operation == "Download":
    client_download()
