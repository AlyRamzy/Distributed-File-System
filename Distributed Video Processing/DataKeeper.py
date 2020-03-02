import cv2
import sys
import numpy as np 
import zmq

ip = "192.168.43.94"
tracker_ip = "192.168.43.118"

pair_port = "5000"    #sys.argv[1]
done_port = "10000"   #sys.argv[2]

context = zmq.Context()

socket2 = context.socket(zmq.PUSH)
socket2.connect("tcp://" + tracker_ip + ":%s" %done_port)

while True:
    socket = context.socket(zmq.PAIR)
    socket.bind("tcp://" + ip + ":%s" % pair_port)

    message = socket.recv_pyobj()
    if (message["operation"] == "Upload"):
        with open(message["file_name"],"w+") as File:
            File.write(message["file"])
        socket2.send_pyobj({"ip" : ip , "port" : pair_port , "status" : "Done_Uploading" , "file_name" : message["file_name"]})

    elif (message["operation"] == "Download"):
        with open(message["file_name"],'rb') as File:
            new_message = File.read()
            socket.send_pyobj({"file" : str(new_message)})
        socket2.send_pyobj({"ip" : ip , "port" : pair_port , "status" : "Done_Downloading"})
    
    elif (message["operation"] == "Replicate"):
        temp_socket = context.socket(zmq.PAIR)
        temp_socket.connect("tcp://"+ message["ip"] + ":%s" %message["port"])   #port and ip of the distination
        with open(message["file_name"],'rb') as File:
            new_message = File.read()
        temp_socket.send_pyobj({"operation" : "Upload" , "file_name" : message["file_name"] , "file" : new_message})
        socket2.send_pyobj({"ip" : ip , "port" : pair_port , "status" : "Done_Replication"})
        
    socket.close()


