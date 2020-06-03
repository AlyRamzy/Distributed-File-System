import multiprocessing
from multiprocessing import Process,Manager
import configparser
import numpy as np
import zmq
import time

conf_parser = configparser.RawConfigParser()
conf_parser.read(r'conf.txt')

number_of_tracker_processes = int(conf_parser.get('conf', 'number_of_tracker_processes'))
number_of_Data_keepers = int(conf_parser.get('conf', 'number_of_Data_keepers'))
number_of_Data_keeper_ports = int(conf_parser.get('conf', 'number_of_Data_keeper_ports'))
start_address_of_ports = int(conf_parser.get('conf', 'start_address_of_ports'))
start_address_of_DataKeepers = int(conf_parser.get('conf', 'start_address_of_DataKeepers'))
file_tracker_port_address = int(conf_parser.get('conf', 'file_tracker_port_address'))
heartBeat_port_address = int(conf_parser.get('conf', 'heartBeat_port_address'))
replica_period = int(conf_parser.get('conf', 'replica_period'))
replica_factor = int(conf_parser.get('conf', 'replica_factor'))

#ip's 
file_tracker_ip = conf_parser.get('conf', 'file_tracker_ip')

def heartBeat(ip_table,lock_ip):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    for ip in ip_table.keys():
        socket.connect("tcp://"+ip+":"+str(heartBeat_port_address))
    socket.subscribe("")
    socket.RCVTIMEO= 1000//len(ip_table)
    while True:
        startTime=time.time()
        lock_ip.acquire()
        for machine_ip in ip_table.keys():
            tmp=ip_table[machine_ip]
            tmp[0] = 0 # Make All Machines Not Alive 
            ip_table[machine_ip]=tmp
        for i in range (len(ip_table)):
            try:
                message = socket.recv_pyobj()
                if "ip" in message:
                    rec_ip = message["ip"]
                    tmp = ip_table[rec_ip]
                    tmp[0] = 1 
                    ip_table[rec_ip]=tmp 
                else:
                    print("Error in Message Format in heartBeat Expected : {ip:IP}")
            except:
                #print("Message in heart Beat Time Out")
                pass
        
        print(ip_table)
        lock_ip.release()
        endTime=time.time()
        if(endTime-startTime<1):
            time.sleep(1-(endTime-startTime))
        #testMin = time.time()
        # print(testMin-startTime)

def file_Tracker(ip_table, file_table, lock_ip, lock_file,file_tracker_port_address):
    print("File Tracker Started..")
    context = zmq.Context()
    zmq_socket = context.socket(zmq.PULL)
    zmq_socket.bind("tcp://" + file_tracker_ip + ":"+str(file_tracker_port_address))# Take From Configuration File 
    while True:
        message = zmq_socket.recv_pyobj()
        if "status" in message:
            print("tracker recieved",message)
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
            #print("before",ip_table)
            tmp=ip_table[ip]
            tmp[1]+=1
            tmp[2][int(port)-tmp[3]]=1
            ip_table[ip]=tmp
            #print("after",ip_table)
            lock_ip.release()
            #print(file_table)
        

def tracker(ip_table, file_table, lock_ip, lock_file, port):
    context = zmq.Context()
    server = context.socket(zmq.REP)
    server.bind("tcp://" + file_tracker_ip + ":"+port) # Take From Configuration File 
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
                    senddport = str(available_port_index+status[3])
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
                        if ports[1] > 0 and ports[0]:
                            for itr, av in enumerate(ports[2]):
                                if av == 1:
                                    send_dict["ip"] = ip
                                    send_dict["port"] = str(ports[3] + itr)  
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
                        entry[2][int(send_dict["port"])-entry[3]] = 0
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
        # Getting Instance Count of files.
        lock_file.acquire()  # this sequence is made to avoid Dead Locks.
        for fname, ips in file_table.items():
            lock_ip.acquire()

            count = 0
            src_machines = {}
            found = False

            for ip in ips:
                ports = ip_table[ip]
                if ports[0] == 1:
                    count += 1
                    if ports[1] > 0:
                        available_ports = []
                        for itr, av in enumerate(ports[2]):
                            if av == 1:
                                available_ports.append(str(ports[3] + itr))
                                found = True
                        src_machines[ip] = available_ports

            remaining = replica_factor - count
            print("Remaining ",remaining)
            if not found and (remaining > 0):
                print("Can't get a source machine at the moment for " + fname)
                continue
        
            print(src_machines)
            if found and (remaining > 0):
                machine = 0
                port = 0
                all_machines = list(ip_table.keys())
                machines_to_copy = list(set(all_machines).difference(ips))
                #print("machines",machines_to_copy)
                for m in machines_to_copy:
                    send_dict = dict()
                    ports = ip_table[m]
                    # for x in ips:
                    #     print("src machine",x,ip_table[x])
                    # print("Replicate to machine",m,"before with",ports)
                    if ports[0] == 1:
                        if ports[1] > 0:
                            for itr, av in enumerate(ports[2]):
                                if av == 1:

                                    if machine == len(src_machines.keys()):
                                        break

                                    send_dict["operation"] = "Replicate"
                                    send_dict["ip"] = m
                                    send_dict["port"] = str(ports[3]+itr)
                                    send_dict["file_name"] = fname
                                    remaining -= 1

                                    print("from replicator",send_dict)

                                    current_machine = list(src_machines.keys())[machine]
                                    current_port = src_machines[current_machine][port]

                                    socket = context.socket(zmq.REQ)
                                    socket.connect("tcp://" + current_machine + ":" + current_port)
                                    socket.send_pyobj(send_dict)

                                    entry = ip_table[m]
                                    entry[1] -= 1
                                    entry[2][int(send_dict["port"]) - entry[3]] = 0
                                    ip_table[m] = entry

                                    status = socket.recv_pyobj()
                                    
                                    if "status" not in status:
                                        print("Error in replication")
                                        break

                                    port += 1
                                    if port == len(src_machines[current_machine]):
                                        port = 0
                                        machine += 1
                                    
                                    print("Replicate to machine",m,"after with",ip_table)
                                    break

                    if remaining == 0:
                        break
            lock_ip.release()
        lock_file.release()
        print("Finished Replicas !")


if __name__ == "__main__":
    manager = Manager()
    ip_table = manager.dict()       # this dictionary is {ip : [alive , #available ports ,  ports' status (list) , start_port]}
    file_table = manager.dict()     # this dictionary is {file_name : list_of_ip's}
    lock_ip = multiprocessing.Lock()   # semaphore lock
    lock_file = multiprocessing.Lock()

    for i in range(1, number_of_Data_keepers+1):
        machine = 'machine' + str(i)
        ip = conf_parser.get(machine, 'ip')
        ports = []
        for j in range(1, number_of_Data_keeper_ports+1):
            ports.append(1)
        ip_table[ip] = [1, number_of_Data_keeper_ports, ports,i*start_address_of_DataKeepers]
    
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

    p.append(Process(target=heartBeat,args=(ip_table,lock_ip)))
    p[len(p) - 1].start()
    p.append(Process(target=file_Tracker,args=(ip_table,file_table,lock_ip,lock_file,file_tracker_port_address)))
    p[len(p) - 1].start()
    p.append(Process(target=replicator, args=(ip_table,file_table,lock_ip,lock_file)))
    p[len(p) - 1].start()
    
    for i in range(len(p)):
        p[i].join()
        
    # Add Replicator Here.
