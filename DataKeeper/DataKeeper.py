import cv2
import sys
import numpy as np 
import zmq

ip = sys.argv[1]
file_tracker_ip = sys.argv[2]
pair_port = sys.argv[3]
done_port = sys.argv[4]

context = zmq.Context()

socket2 = context.socket(zmq.PUSH)
socket2.connect("tcp://" + file_tracker_ip + ":" + done_port)

socket = context.socket(zmq.REP)
socket.bind("tcp://" + ip + ":" + pair_port)

while True:

    message = socket.recv_pyobj()

    if (message["operation"] == "Upload"):
        print("DK Uploading..")
        with open(message["file_name"],'wb') as File:
            File.write(message["file"])
        socket.send_pyobj({"status":"OK"})
        socket2.send_pyobj({"ip" : ip , "port" : pair_port , "status" : "Done_Uploading" , "file_name" : message["file_name"]})
        print("DK Done Uploading..")

    elif (message["operation"] == "Download"):
        print("DK Downloading..")
        with open(message["file_name"],'rb') as File:
            new_message = File.read()
        socket.send_pyobj({"file" : new_message})
        socket2.send_pyobj({"ip" : ip , "port" : pair_port , "status" : "Done_Downloading"})
        print("DK Done Downloading..")
    
    elif (message["operation"] == "Replicate"):
        print("DK Replicating..")
        temp_socket = context.socket(zmq.REQ)
        temp_socket.connect("tcp://"+ message["ip"] + ":" + message["port"])   #port and ip of the distination
        with open(message["file_name"],'rb') as File:
            new_message = File.read()
        temp_socket.send_pyobj({"operation" : "Upload" , "file_name" : message["file_name"] , "file" : new_message})
                
        dummy_massage = temp_socket.recv_pyobj()
        socket.send_pyobj({"status":"OK"})

        print("DK Done Replicating..")
        
    
