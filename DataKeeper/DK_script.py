import os
import sys
import configparser

conf_parser = configparser.RawConfigParser()
conf_parser.read(r'conf.txt')

ip = sys.argv[1]
Heart_Beat_port = conf_parser.get('conf', 'heartBeat_port_address')                         #heart beat port in tracker
start_address_of_DataKeepers = int(conf_parser.get('conf', 'start_address_of_DataKeepers')) #first port in data keeper
tracker_ip = conf_parser.get('conf', 'file_tracker_ip')                                     
tracker_port = conf_parser.get('conf', 'file_tracker_port_address')
number_of_Data_keeper_ports = int(conf_parser.get('conf', 'number_of_Data_keeper_ports'))   #number of processes in data keepers

#alive signal sender
os.system("python AliveProcess.py "+ip+" "+Heart_Beat_port+" &")

for i in range(number_of_Data_keeper_ports):
    os.system("python DataKeeper.py "+ip+" "+tracker_ip+" "+str(start_address_of_DataKeepers+i)+" "+tracker_port+" &")