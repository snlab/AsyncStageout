#!/usr/bin/python

import thread
import time
import os
import glob
import shutil, json
from Queue import Queue, Empty
from threading import Thread
import subprocess
import logging

DIR = "./install/asyncstageout/AsyncTransfer/dropbox/outputs/FDTTransfers"
MaxWorkersPerThread = 1
MaxWorkersIncreasedThreads = 5
UseCircuitTreshold = 1
DELAY = 5 # In seconds between each thread execution for finding all files.
NEW_THREAD_DELAY = 3*60 # After 10 minutes new thread will be created.
END_THREAD_DELAY = 5*60 # After five minutes thread will go away. Except first MaxWorkersPerThread

def workerT(i, threadName, q, logger, useCircuit=True):
    """ This is the worker thread function.
    It processes items in the queu on after another
    logic:
/FID    N min workers per thread are not using circuit. (Will change it to specify through ASO if want to or not)
    Circuit is not used if queue size is not bigger than circuit treshold;
    Additional threads would use circuit, because normal transfers are not progressing.
    """
    threadCounter = 0
    if useCircuit:
        threadCounter += 1
    #Remove thread whenever queue is not big.
    counterForRemoveThread = 0
    circuitData = readCircuits()
    while True:
        filesToMove = []
        logger.debug("%s-%s: Looking for the next transfer..." % ( threadName, i))
        transferFile = None
#        check_if_new_thread_needed
        try:
            if i < MaxWorkersPerThread:
                transferFile = q.get()
            else:
                transferFile = q.get_nowait()
                counterForRemoveThread = 0
        except Empty as ex:
            logger.debug("%s-%s: Queue seems to be empty. No need to keep more this thread." % (threadName, i))
            counterForRemoveThread += i+5
            time.sleep(i+5)
            if END_THREAD_DELAY <= counterForRemoveThread:
                logger.debug("%s-%s: Worker reached thread removal timeout. Returning..." % (threadName, i))
                return
            continue
        logger.info("%s-%s: New work got for transfer %s" % ( threadName, i, transferFile))
        #Should I use circuit double check and check if possible;
        #circuitData = {}
        useCircuit = False
        if q.qsize() >= UseCircuitTreshold and threadCounter > 0: #and i >= MaxWorkersPerThread:
            circuitData = readCircuits()
            if threadName in circuitData:
                useCircuit = True
                logger.info("%s-%s: Circuit is available, will use these ip`s %s for transfer" % (threadName, i, circuitData[threadName]))
            logger.debug("%s-%s: All available circuits: %s" % (threadName, i, circuitData))
        else:
            useCircuit = False
        # ./install/asyncstageout/AsyncTransfer/dropbox/outputs/FDTTransfers/T2_US_UmichFDT_T3_CH_CernFDT1/ACQUIRED/1445519021.81.json
        #open file and read json
        report, json_out = jsonRead(transferFile, logger)
        if not report:
            logger.debug("Failed to read json. Take new work!!! FATAL")
            q.task_done()
            continue
        # mv file from acquired to ongoing
        fdtcpFileName = "%s.fdtcp-copy" % transferFile.split('/')[-1]
        logsFile = "%s.fdtcp-logFile" % transferFile.split('/')[-1]
        LOG_DIR = "%s/ONGOING/LOGS/" % "/".join(transferFile.split("/")[:-2])
        ONG_DIR = "%s/ONGOING" % "/".join(transferFile.split("/")[:-2])
        FIN_DIR = "%s/FINISHED/" % "/".join(transferFile.split("/")[:-2])
        print 'AA1 : %s' % FIN_DIR
        print 'AA2 : %s' % transferFile
        newFilePath = "%s/%s" % (ONG_DIR, transferFile.split('/')[-1])

        fdtcpFileName = "%s/%s" % (ONG_DIR, fdtcpFileName)
        logsFile = "%s/%s" % (LOG_DIR, logsFile)
        shutil.move(transferFile, newFilePath)
        filesToMove.append(newFilePath)
        #print "Out %s" % json_out
        # Read json out and prepare file for fdtcp command
        with open(fdtcpFileName, 'w') as fd:
            for item in json_out:
                tempI = item.split(" ")
                fd.write("%s %s\n" % (tempI[0], tempI[1]))
        # execute fdtcp with pointing to that file
        #print 'Executing cp command'
        if useCircuit:
            print circuitData
            command = "env -i HOME=/tmp/fdtcp/ fdtcp -f %s -l %s -i %s -y %s" % (fdtcpFileName, logsFile, circuitData[threadName][0], circuitData[threadName][1])
            logger.info("%s-%s: Thread worker started fdtcp copy. Full command %s" % (threadName, i, command))
        else:
            command = "env -i HOME=/tmp/fdtcp/ fdtcp -f %s -l %s" % (fdtcpFileName, logsFile)
            logger.info("%s-%s: Thread worker started fdtcp copy. Full command %s" % (threadName, i, command))
        ret = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in ret.stdout.readlines():
            logger.debug("%s-%s: stdout: %s" % (threadName, i, line))
        retval = ret.wait()
        logger.info("%s-%s: fdtcp command finished. exit code: %s" % ( threadName, i, retval))
        filesToMove.append(logsFile)
        if retval == 1 and useCircuit:
            filesToMove.append("%s/%s" % (LOG_DIR, logsFile))
            # Return value is always 1 which I don`t like and this has to be different.
            # Take a look after sc15 how to return better value.
            logger.warning("%s-%s: fdtcp failed with using circuit. Is it up?! Lets try without circuit." % (threadName, i))
            logsFile += '-withoutCircuit'
            command = "env -i HOME=/tmp/fdtcp/ fdtcp -f %s -l %s" % (fdtcpFileName, logsFile)
            logger.debug("%s-%s: New fdtcp copy command %s" % (threadName, i, command))
            ret = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in ret.stdout.readlines():
                logger.debug("%s-%s: stdout: %s" % (threadName, i, line))
            retval = ret.wait()
            logger.info("%s-%s: fdtcp command finished. exit code: %s" % ( threadName, i, retval))
            filesToMove.append(logsFile)
#            filesToMove.append(fdtcpFileName)
        filesToMove.append(fdtcpFileName)
        move_files(filesToMove, FIN_DIR)
# ./install/asyncstageout/AsyncTransfer/dropbox/outputs/FDTTransfers/T2_US_UmichFDT_T2_US_CaltechFDT/ONGOING/LOGS//1447054140.86_43.json.fdtcp-logFile .%s/FINISHED/install%s/FINISHED/asyncstageout%s/FINISHED/AsyncTransfer%s/FINISHED/dropbox%s/FINISHED/outputs%s/FINISHED/FDTTransfers%s/FINISHED/T2_US_UmichFDT_T2_US_CaltechFDT Justas

        # whenever command finishes, report back to dashboard about success
        time.sleep(i + 2)
        threadCounter +=1
        q.task_done()
        # Marking ASO jobs as DONE.
        useCircuit = False
        circuitData = {}
        logger.debug("%s-%s: Marking ASO jobs as done." % (threadName, i))

def jsonRead(fileName, logger):
    """ """
    try:
        with open(fileName, 'r') as fd:
            json_out = json.load(fd)
            return True, json_out
    except IOError as ex:
        logger.debug("Got IOError. Skipping this read.")
        return False, []


def move_files(files, dest_dir):
    """Move logs from acq to finished"""
    try:
        createDirs(dest_dir)
        for fileName in files:
            print fileName, dest_dir, 'Justas'
            shutil.move(fileName, dest_dir)
    except:
        print 'Got a failure moving files'
    return

def new_thread_needed(qsize, nWorkers, prevQueueSize, delay, waitTime):
    """ """
    if prevQueueSize <= qsize and qsize > MaxWorkersPerThread:
        waitTime += delay
        if waitTime >= NEW_THREAD_DELAY:
            return True, qsize, 0
        return False, qsize, waitTime
    return False, qsize, 0


# Define a function for the thread
def execute_thread( threadName, delay, logger):
    privateThreadQueue = Queue()
    logger.debug("%s: Started %s thread and will start %s workers for this thread" % (threadName, threadName, MaxWorkersPerThread))
    totalWorkers = 0
    for i in range(MaxWorkersPerThread):
        worker = Thread(target=workerT, args=(i, threadName, privateThreadQueue, logger))
        worker.setDaemon(True)
        worker.start()
    totalWorkers = MaxWorkersPerThread
    logger.debug("%s: Creating directories for %s thread " % ( threadName, threadName))
    NEW_JOB_DIR = "%s/%s" % (DIR, threadName)
    ACQ_DIR = "%s/ACQUIRED" % NEW_JOB_DIR
    FIN_DIR = "%s/FINISHED" % NEW_JOB_DIR
    ONG_DIR = "%s/ONGOING" % NEW_JOB_DIR
    LOG_DIR = "%s/ONGOING/LOGS" % NEW_JOB_DIR
    createDirs(ACQ_DIR)
    createDirs(FIN_DIR)
    createDirs(ONG_DIR)
    createDirs(LOG_DIR)
    new_thread_counter = 0
    prev_queue_size = 0
    numFiles = 0
    totalSize = 0
    while True:
        time.sleep(delay)
        logger.info("%s: %s %s" % (threadName, totalWorkers, MaxWorkersIncreasedThreads))
        if totalWorkers < MaxWorkersIncreasedThreads:
            threadStart, prev_queue_size, new_thread_counter = new_thread_needed(privateThreadQueue.qsize(), totalWorkers, prev_queue_size, DELAY, new_thread_counter)
            if threadStart:
                logger.debug("%s: Creating new worker for this thread. Queue is too big and not progressing." % threadName)
                worker = Thread(target=workerT, args=(totalWorkers, threadName, privateThreadQueue, logger, True))
                worker.setDaemon(True)
                worker.start()
                totalWorkers += 1
            else:
                logger.debug("%s: Not creating new thread yet." % (threadName))

        logger.debug("%s: Checking for new files..." % threadName)
        files = read_directory_files(NEW_JOB_DIR)
        sizeOfFiles1 = 0
        numFiles1 = 0
        currentStats = {'sizeFiles': 0, 'numFiles': 0, 'AvgSpeed': 0, 'CompletionTimeInS': 0}
        if files:
            logger.debug("%s: New %s files for transfer" % ( threadName, len(files)))
        for filename in files:
            fullFilename = "%s/%s" % (NEW_JOB_DIR, filename)
            newFilename = "%s/%s" % (ACQ_DIR, filename)
            sizeOfFiles1, numFiles1 = makeQueueStatistics(logger, fullFilename)
            logger.debug("%s: Got new %s files, which size in total is %s." % ( threadName, numFiles1, sizeOfFiles1))
            #queueStatistics(currentStats, sizeOfFiles1, numFiles1, False, logger)
            #Print queue statistics and how lond new transfer will have to wait...
            shutil.move(fullFilename, newFilename)
            privateThreadQueue.put(newFilename)
        #queueStatistics(currentStats, 0, 0, True, logger)
        #Print queue statistics...
        if files:
            logger.info("%s: All new %s files moved from NEW to ACQUIRED" % ( threadName, len(files)))
        else:
            logger.debug("%s: There is no new files for transfers... Go back to the loop..." % threadName)

def queueStatistics(currentStats, sizeFiles, numFiles, mainStat, logger):
    """If main stat do not add sizeFiles and numFiles to the final counter...
       Also it should read what workers replied for particular transfers. Is it done with exit 0, how long it took in seconds, and how much data transferred.
       Communication goes through files and this function deletes these files after that."""
    if not mainStat:
        currentStats['sizeFiles'] += sizeFiles
        currentStats['numFiles'] += numFiles
        if currentStats['AvgSpeed'] != 0 and currentStats['CompletionTimeInS'] != 0:
            # In this case we know average speed of the link. Of course this is know only for one thread. but it is ok, because this is what we care...
            # Average speed is always in MB/s
            # Here is logger which prints when task will be finished...
            a = 1
        else:
            b = 1      # Seem there is no transfers finished and we can`t predict
    else:
        c = 1
    #it is not main stat, so print the status of all transfers....
    # Double check if files are available and save in json
       #Need to work more here TODO!!!!
    return 

def makeQueueStatistics(logger, fullFilename):
    logger.debug("Open File, read all output sizes and increment file numb.")
    report, json_out = jsonRead(fullFilename, logger)
    size = 0
    numFiles = 0
    if report:
        for item in json_out:
            numFiles += 1
            itemL = item.split(" ")
            if len(itemL) > 3:
                # 4 element is size
                size += int(itemL[3])
    return size, numFiles

def createDirs(dirName):
    if not os.path.exists(dirName):
        os.makedirs(dirName)

def readCircuits(filename='/data/srv/asyncstageout/current/circuits.json'):
    """Read circuits file for available circuits."""
    data = {}
    try:
        with open(filename, 'r') as fd:
            data = json.load(fd)
    except:
        print 'Got a failure reading circuits file.'
        #Should be not generic except!
    return data

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

logger = logging.getLogger('fdt_queue')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

running_threads = []
circuitsResync = int(30 * 60 / DELAY)
counter = 0
while 1:
    threads = []
    logger.info("Active threads: %s" % len(running_threads))
    logger.debug("Active thread names: %s" % running_threads)
    for root, dirnames, filenames in os.walk('%s/' % DIR):
        threads = dirnames
        break
    if not threads:
        logger.debug("There is 0 new threads to start for transfers.")
    for thread_name in threads:
        if thread_name not in running_threads:
            logger.debug("Starting new thread %s for transfers" % thread_name)
            thread.start_new_thread( execute_thread, (thread_name, DELAY, logger, ))
            running_threads.append(thread_name)
    time.sleep(DELAY)
    counter += 1
    if counter > circuitsResync:
        counter = 0
        logger.debug("Query github and try to get available circuits")
    pass
