#!/usr/local/bin/python3
import os
import sys
import time
import argparse
import threading
import random
from queue import Queue
from itertools import cycle
from os import walk
from boxsdk import JWTAuth
from boxsdk import Client
from py_essentials import hashing as hs

parser = argparse.ArgumentParser(
    description='SDK script for NAS Home Directory migration to BOX',
)

parser.add_argument('-p', '--home_path',  required = True, help='Home Drive Path')

args = vars(parser.parse_args())

print(args['home_path'])

if args['home_path']:
    pass
else:
    print("Usage : dyn_home_box.py -p <path>")
    print("-p <home directory path>/")
    sys.exit()

pathL = args['home_path'].split('/')
racf_id = pathL[-1].replace('$','')

home_index = len(pathL)-1

#################################################################
####### Configure JWT(Java Web Token) authorization object ######
##################  (MyVistraBoxApplication App) ################
#################################################################

sdk = JWTAuth.from_settings_file('/usr/local/bin/box/vistraboxapp.json')

client  = Client(sdk)
user    = client.user(user_id='3895464641') # Shyam
folderD = {}
fileD   = {}

#################################################################
############ Sub-Function to upload files to BOX ################
#################################################################

def upload_file_to_box(result,parent_id,lock):
    print(result,parent_id)
    client.as_user(user).folder(parent_id).upload(result,upload_using_accelerator=True,preflight_check=False)
    with lock:
        print("Thread Name : " + threading.current_thread().name)

#################################################################
############## Worker function for managing queues ##############
#################################################################

def worker(parent_id,lock,q):
    while True:
        upload_file_to_box(q.get(),parent_id,lock)
        q.task_done()

#################################################################
################### Function to create folder ###################
#################################################################

def create_folder(parent_id,directory):
    create_folder = client.as_user(user).folder(folder_id=parent_id).create_subfolder(directory)
    folderD[directory] = create_folder.id
    return(create_folder.id)

#################################################################
################### Function if folder exists  ##################
#################################################################

def folder_exists(parent_id,directory):
    object_id = None
    objectD = {}
    
    for items in client.as_user(user).folder(parent_id).get_items(limit=1000):
        object_id   = items.id
        object_name = items.name
        object_type = items.type
        objectD[object_name] = object_id
    
    try:
        return(objectD[directory])
    except KeyError:
        (parent_id) = create_folder(parent_id,directory)
        return(parent_id)

#################################################################
########## Function to upload files to BOX - Base ###############
#################################################################

def base_upload_to_box():
    file_dictionary = {}
    directoriesL    = []
    for dirname, dirnames, filenames in os.walk(args['home_path']):
        parent_id  = 0
        dirname = dirname.replace("\\", "/")
        directoryL   = dirname.split('/')
        for index in range(home_index,len(directoryL)):
            path_name = directoryL[index].strip()
            (parent_id) = folder_exists(parent_id,path_name)

        max_threads = 60
        q = Queue()
        lock = threading.Lock()

        for filename in filenames:
            full_path = (dirname + '/' + filename)
            q.put(full_path)
        print ("Starting New Upload :")
        max_threads = len(filenames)
        for i in range(max_threads):
            t = threading.Thread(target=worker,args = (parent_id,lock,q))
            t.daemon = True
            t.start()
        
        start = time.perf_counter()
        q.join()

#################################################################
########################### Function MAIN #######################
#################################################################

if __name__ == "__main__":
    base_upload_to_box()
