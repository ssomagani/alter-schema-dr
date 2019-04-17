import datetime
import time
import subprocess
from voltdbclient import *

SERVER1 = sys.argv[1]
SERVER2 = sys.argv[2]
PORT = 21211
MAX_TRIES = 5

def connect(server, client, count):
    try:
        client = FastSerializer(server, PORT)
        curTime = datetime.datetime.now()
        print(F"1. Connected to {server} successfully")
        return client
    except Exception as e: 
        if(count > MAX_TRIES):
            print(F"1. !! Unable to connect after {count} attempts")
            exit(1)
        time.sleep(5)
        connect(count+1)

def pauseCluster(client, count):
    try:
        proc = VoltProcedure( client, "@Pause" )
        response = proc.call()
        if(response.status != 1):
            raise Exception(response.statusString)
        print(F"2. Paused server successfully")
    except Exception as e: 
        if(count > MAX_TRIES):
            print(F"2. !!Quitting trying to pause cluster after {count} attempts ")
            exit(1)
        time.sleep(5)
        pauseCluster(client, count+1)
        
def resumeCluster(client, count):
    try:
        proc = VoltProcedure( client, "@Resume" )
        response = proc.call()
        if(response.status != 1):
            raise Exception(response.statusString)
        print(F"7. Resumed cluster successfully")
    except Exception as e: 
        if(count > MAX_TRIES):
            print(F"Exiting after {MAX_TRIES} attempts ")
            exit(1)
        time.sleep(5)
        resumeCluster(client, count+1)

def checkProducerDrained(client, count):
    try:
        proc = VoltProcedure( client, "@Statistics", [FastSerializer.VOLTTYPE_STRING])
        response = proc.call([F"DRPRODUCER", 0])
        queue_depth = response.tables[1].tuples[0][9]
        if(response.status != 1):
            raise Exception(response.statusString)
        if queue_depth == 0:
            print(F"3. Producer Drained.")
        else:
            checkProducerDrained(client, count+1)
    except Exception as e: 
        if(count > MAX_TRIES):
            print(F"3. !!Producer not drained after {MAX_TRIES} attempts. You can resume the cluster to revert")
            exit(1)
        time.sleep(5)
        checkProducerDrained(client, count+1)

def checkConsumerPaused(client, count):
    try:
        proc = VoltProcedure( client, "@Statistics", [FastSerializer.VOLTTYPE_STRING])
        response = proc.call([F"DRCONSUMER", 0])
        if(response.status != 1):
            raise Exception(response.statusString)
        isPaused = response.tables[1].tuples[0][10]
        if isPaused is False:
            checkConsumerPaused(client, count+1)
        print(F"5. consumer is paused")
    except Exception as e: 
        if(count > MAX_TRIES):
            print(F"5. !!Consumer did not pause after {MAX_TRIES} attempts ")
            exit(1)
        time.sleep(5)
        checkConsumerPaused(client, count+1)

def main():
    startTime = datetime.datetime.now()
    client1 = 'nil'
    client2 = 'nil'
 
# 1. ensure being able to connect to all clusters
    client1=connect(SERVER1, client1, 0)
    client2=connect(SERVER2, client2, 0)

# 2. pause all secondary clusters
    pauseCluster(client2, 0)
  
# 3. wait for all secondary producers to drain
    checkProducerDrained(client2, 0)

# 4. apply schema updates on primary
    ddlfile = open('ddl.sql', 'r')
    ddl = " ".join(ddlfile.readlines())
    subprocess.call([F"sqlcmd", F"--servers={SERVER1}", F"--port={PORT}", F"--query={ddl}"])
    print(F"4. Updated schema on {SERVER1}")

# 5. wait for secondary consumers to pause
    checkConsumerPaused(client2, 0)

# 6. apply schema updates on all secondary clusters
    subprocess.call([F"sqlcmd", F"--servers={SERVER2}", F"--port={PORT}", F"--query={ddl}"])
    print(F"6. Updated schema on {SERVER2}")

# 7. resume all secondary clusters
    resumeCluster(client2, 0)

if __name__ == "__main__":
    main()