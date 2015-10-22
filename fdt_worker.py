#!/usr/bin/python

import thread
import time
import os
import glob
import shutil, json
from Queue import Queue
from threading import Thread
import subprocess

DIR = "./install/asyncstageout/AsyncTransfer/dropbox/outputs/FDTTransfers"
MaxWorkersPerThread = 1


def workerT(i, q):
    """ This is the worker thread function.
    It processes items in the queu on after another
    """
    while True:
        print '%s: Looking for the next transfer' % i
        transferFile = q.get()
        print 'Processing %s file and transferring...' % transferFile
        # ./install/asyncstageout/AsyncTransfer/dropbox/outputs/FDTTransfers/T2_US_UmichFDT_T3_CH_CernFDT1/ACQUIRED/1445519021.81.json
        json_out = []
        #open file and read json
        with open(transferFile, 'r') as fd:
            json_out = json.load(fd)
        # mv file from acquired to ongoing
        fdtcpFileName = "%s.fdtcp-copy" % transferFile.split('/')[-1]
        logsFile = "%s.fdtcp-logFile" % transferFile.split('/')[-1]
        LOG_DIR = "%s/ONGOING/LOGS/" % "/".join(transferFile.split("/")[:-2])
        ONG_DIR = "%s/ONGOING" % "/".join(transferFile.split("/")[:-2])
        newFilePath = "%s/%s" % (ONG_DIR, transferFile.split('/')[-1])
        shutil.move(transferFile, newFilePath)
        #print "Out %s" % json_out
        # Read json out and prepare file for fdtcp command
        with open("%s/%s" %(ONG_DIR, fdtcpFileName), 'w') as fd:
            for item in json_out:
                tempI = item.split(" ")
                fd.write("%s %s\n" % (tempI[0], tempI[1]))
        # execute fdtcp with pointing to that file
        command = "env -i HOME=/tmp/fdtcp/ fdtcp -f %s -l %s" % ("%s/%s" %(ONG_DIR, fdtcpFileName), "%s/%s" % (LOG_DIR, logsFile))
        ret = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in ret.stdout.readlines():
            print line
        retval = ret.wait()
        print 'Return code %s for transfer %s ' % (retval, fdtcpFileName)
        # whenever command finishes, report back to dashboard about success
        time.sleep(i + 2)
        q.task_done()


# Define a function for the thread
def execute_thread( threadName, delay):
    privateThreadQueue = Queue()
    for i in range(MaxWorkersPerThread):
        worker = Thread(target=workerT, args=(i, privateThreadQueue,))
        worker.setDaemon(True)
        worker.start()
    NEW_JOB_DIR = "%s/%s" % (DIR, threadName)
    ACQ_DIR = "%s/ACQUIRED" % NEW_JOB_DIR
    FIN_DIR = "%s/FINISHED" % NEW_JOB_DIR
    ONG_DIR = "%s/ONGOING" % NEW_JOB_DIR
    LOG_DIR = "%s/ONGOING/LOGS" % NEW_JOB_DIR
    createDirs(ACQ_DIR)
    createDirs(FIN_DIR)
    createDirs(ONG_DIR)
    createDirs(LOG_DIR)
    while True:
        time.sleep(delay)
        files = read_directory_files(NEW_JOB_DIR)
        print "New files in link between %s: %s" % (threadName, len(files))
        for filename in files:
            fullFilename = "%s/%s" % (NEW_JOB_DIR, filename)
            newFilename = "%s/%s" % (ACQ_DIR, filename)
            shutil.move(fullFilename, newFilename)
            privateThreadQueue.put(newFilename)
        if files:
            print "All new files moved to acq between %s: %s" % (threadName, len(files))

def createDirs(dirName):
    if not os.path.exists(dirName):
        os.makedirs(dirName)


def read_directory_files(dirName):
    """ """
    files = []
    for root, dirnames, filenames in os.walk(dirName):
        files = filenames
        break
    return files


# Create two threads as follows
#try:
#    thread.start_new_thread( print_time, ("Thread-1", 2, ) )
#    thread.start_new_thread( print_time, ("Thread-2", 4, ) )
#except:
#    print "Error: unable to start thread"

running_threads = []
while 1:
    threads = []
    for root, dirnames, filenames in os.walk('%s/' % DIR):
        threads = dirnames
        break
    if not threads:
        print 'I don`t have any request to transfer with FDT'
    for thread_name in threads:
        if thread_name not in running_threads:
            print 'Starting new thread for transfers %s' % thread_name
            thread.start_new_thread( execute_thread, (thread_name, 2, ))
            running_threads.append(thread_name)
    time.sleep(5)
    pass
