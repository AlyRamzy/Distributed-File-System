import multiprocessing
from multiprocessing import Process,Manager
import configparser
import numpy as np
import zmq
import time
#import signal, os

conf_parser = configparser.RawConfigParser()
conf_parser.read(r'conf.txt')

number_of_tracker_processes = int(conf_parser.get('conf', 'number_of_tracker_processes'))
number_of_Data_keepers = int(conf_parser.get('conf', 'number_of_Data_keepers'))
number_of_Data_keeper_ports = int(conf_parser.get('conf', 'number_of_Data_keeper_ports'))
start_address_of_ports = int(conf_parser.get('conf', 'start_address_of_ports'))
file_tracker_port_address = int(conf_parser.get('conf', 'file_tracker_port_address'))
heartBeat_port_address = int(conf_parser.get('conf', 'heartBeat_port_address'))
replica_period = int(conf_parser.get('conf', 'replica_period'))
replica_factor = int(conf_parser.get('conf', 'replica_factor'))


def heartBeat(ip_table,lock_ip,heartBeat_port_number):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    
    
    for ip in ip_table.keys():
        socket.connect("tcp://"+ip+":"+heartBeat_port_number)
    while True:
        
        time.sleep(1)
        lock_ip.acquire()
        for machine in ip_table.keys():
                   temp=ip_table
        message = socket.recv_pyobj()
        
        # Update The Table After Choosing port
        temp = ip_table[ip]
        temp[1] -= 1
        temp[2][available_port_index] = 0
        ip_table[ip] = temp
        break 
def file_Tracker(ip_table, file_table, lock_ip, lock_file,file_tracker_port_address):
    print("File Tracker Started..")
    context = zmq.Context()
    zmq_socket = context.socket(zmq.PULL)
    zmq_socket.bind("tcp://192.168.43.118:"+str(file_tracker_port_address))# Take From Configuration File 
    message = zmq_socket.recv_pyobj()
    if "status" in message:
        ip=message["ip"]
        port=message["port"]
        status = message["status"]
        if(status == "Done_Uploading"):
            file_name=message["file_name"]
            lock_file.acquire()
            if file_name not in file_table.keys():
                file_table[file_name]=[]
            if ip not in file_table[file_name]:
                tmp=file_table[file_name]
                tmp.append(ip)
                file_table[file_name]=tmp
            lock_file.release()
        lock_ip.acquire()
        tmp=ip_table[ip]
        tmp[1]+=1
        tmp[2][int(port)-start_address_of_ports]=1
        ip_table[ip]=tmp
        lock_ip.release()
        print(ip_table)
        print(file_table)
        
    
        
        


def tracker(ip_table, file_table, lock_ip, lock_file, port):
    context = zmq.Context()
    server = context.socket(zmq.REP)
    server.bind("tcp://192.168.43.118:"+port) # Take From Configuration File 
    while True:
        result = server.recv_pyobj()  # Dict {operation : Download || Upload}
        send_dict = {}
        if 'operation' in result:
           
            operation = result["operation"]
            if operation == "Upload":  # START UPLOAD PROCESS
                print("Uploading..")
                found = False
                lock_ip.acquire()
                # Search For available Port
                for ip, status in ip_table.items():
                    if (status[0] == 0) or (status[1] == 0):  # This Machine is not alive or doesnt have any free ports
                        continue
                    ports = status[2]
                    available_port_index = np.argmax(ports)
                    sendip = ip
                    senddport = str(available_port_index+start_address_of_ports	)
                    found = True
                    # Update The Table After Choosing port
                    temp = ip_table[ip]
                    temp[1] -= 1
                    temp[2][available_port_index] = 0
                    ip_table[ip] = temp
                    break 
                # Construct The Message To Send   {status=True,ip='127.0.0.1',port=5000,message="Failed because .."}
                send_dict["status"] = found
                if found:
                    send_dict["ip"] = sendip
                    send_dict["port"] = senddport
                # server.send_pyobj(send_dict)
                lock_ip.release()
                print("Finished Uploading !")

            elif operation == "Download":
                print("Downloading..")
                found = False
                download_ips = []
                lock_file.acquire()
                try:
                    download_ips = file_table[result["file_name"]]
                except KeyError:
                    print("File not found !")
                    send_dict["status"] = False

                else:
                    lock_ip.acquire()
                    found = False
                    for ip in download_ips:
                        ports = ip_table[ip]
                        if ports[1] > 0:
                            for itr, av in enumerate(ports[2]):
                                if av == 1:
                                    send_dict["ip"] = ip
                                    send_dict["port"] = str(start_address_of_ports + itr)  
                                    found = True
                                    break
                        if found:
                            break
                    send_dict["status"] = found
                    if not found:
                        print("No Empty Ports for Downloading.")
                    else:
                        entry = ip_table[ip]
                        entry[1] -= 1
                        entry[2][int(send_dict["port"])-start_address_of_ports] = 0
                        ip_table[ip] = entry
                    lock_ip.release()

                lock_file.release()
                print("Finished Downloading !")

            else:
                send_dict["status"] = False
                send_dict["message"] = "Received Wrong operation Expected :  Download || Upload"
                print("Received Wrong operation Expected :  Download || Upload")
        else:
            send_dict["status"] = False
            send_dict["message"] = "Received Wrong Format Message Expected : {operation : Download||Upload}"

            print("Received Wrong Format Message Expected : {operaion : Download||Upload}")
        server.send_pyobj(send_dict)

    # write the tracker logic here
    # test updating the dictionaries
    # lock_ip.acquire()
    # update tables here
    # lock_ip.release()


def replicator(ip_table, file_table, lock_ip, lock_file):
    
    context = zmq.Context()
    while True:
        time.sleep(replica_period)
        lock_file.acquire()  # this sequence is made to avoid Dead Locks.
        lock_ip.acquire()
        # Getting Instance Count of files.
        for fname, ips in file_table.items():
            count = 0
            src_machine_ip = None
            src_machine_port = None
            machines = list()
            found = False

            for ip in ips:
                ports = ip_table[ip]
                if ports[0] == 1:
                    count += 1
                    if ports[1] > 0:
                        src_machine_ip = ip
                        for itr, av in enumerate(ports[2]):
                            if av == 1:
                                src_machine_port = str(start_address_of_ports + itr)
                                found = True

            remaining = replica_factor - count
            if not found and (remaining > 0):
                print("Can't get a source machine at the moment for " + fname)
                continue

            if found and (remaining > 0):
                all_machines = list(ip_table.keys())
                machines_to_copy = list(set(all_machines).difference(ips))
                for m in machines_to_copy:
                    send_dict = dict()
                    ports = ip_table[m]
                    if ports[0] == 1:
                        if ports[1] > 0:
                            for itr, av in enumerate(ports[2]):
                                if av == 1:
                                    send_dict["operation"] = "Replicate"
                                    send_dict["ip"] = m
                                    send_dict["port"] = str(start_address_of_ports+itr)
                                    send_dict["file_name"] = fname
                                    remaining -= 1
                                    socket = context.socket(zmq.PAIR)
                                    socket.connect("tcp://" + src_machine_ip + ":" + src_machine_port)
                                    socket.send_pyobj(send_dict)

                                    entry = ip_table[m]
                                    entry[1] -= 1
                                    entry[2][int(send_dict["port"]) - start_address_of_ports] = 0
                                    ip_table[m] = entry

                                    entry = ip_table[src_machine_ip]
                                    entry[1] -= 1
                                    entry[2][int(src_machine_port) - start_address_of_ports] = 0
                                    ip_table[src_machine_ip] = entry
                                    break
                    if remaining == 0:
                        break
        lock_file.release()
        lock_ip.release()


if __name__ == "__main__":
    manager = Manager()
    ip_table = manager.dict()       # this dictionary is {ip : [alive , #available ports ,  ports' status (list) ]}
    file_table = manager.dict()     # this dictionary is {file_name : list_of_ip's}
    lock_ip = multiprocessing.Lock()   # semaphore lock
    lock_file = multiprocessing.Lock()

    for i in range(1, number_of_Data_keepers+1):
        machine = 'machine' + str(i)
        ip = conf_parser.get(machine, 'ip')
        ports = []
        for j in range(1, number_of_Data_keeper_ports+1):
            port = 'port' + str(j)
            ret = int(conf_parser.get(machine, port))
            ports.append(1)
        ip_table[ip] = [1, number_of_Data_keeper_ports, ports]
    
        # to update the dict take the whole instance, update it then put it back
        # print(ip_table[ip])
        # temp = ip_table[ip]
        # temp[0] = 5
        # ip_table[ip]=temp
        # print(ip_table[ip])
    
    # creating processes
    p = []
    for i in range(number_of_tracker_processes):
        p.append(Process(target=tracker, args=(ip_table, file_table, lock_ip, lock_file, str(start_address_of_ports+i))))
        p[i].start()

    p.append(Process(target=file_Tracker,args=(ip_table,file_table,lock_ip,lock_file,file_tracker_port_address)))
    p[len(p) - 1].start()
    p.append(Process(target=Replicator, args=(ip_table,file_table,lock_ip,lock_file)))
    p[len(p) - 1].start()
    
    for i in range(len(p)):
        p[i].join()
        
    # Add Replicator Here.