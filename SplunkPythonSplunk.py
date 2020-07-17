# Splunk (raw JSON) -> Python backend -> Splunk (cleansed JSON)
# Script written by Jack Longmuir.

# Imports below:

import json
import requests
import splunklib.client as client
import splunklib.results as results
import logging
from splunk_hec_handler import SplunkHecHandler

# Global variables:
# Splunk data for the Python SDK:

HOST = "10.120.163.87"
PORT = 8089
USERNAME = "csarakasidis"
PASSWORD = "Happy4594"

logger = logging.getLogger('SplunkHecHandlerExample')
logger.setLevel(logging.DEBUG)

splunk_handler = SplunkHecHandler('sc1uxpremn81.prod.williamhill.plc',
                                  'd2fb257f-e8e8-455d-8e8e-ff1accacffa0',
                                  port=8088, proto='https', ssl_verify=False,
                                  source='jacks-python-script')
logger.addHandler(splunk_handler)

baseDict = {
            "log_timestamp" : "Timestamp",
            "sddc_id" : "SDDCID",
            "source" : "SourceIP",
            "priority" : "Priority",
            "log_type" : "LogType",
            "hostname" : "Hostname",
            "event_type" : "EventType",
            "appname" : "AppName",
            "ingest_timestamp" : "IngestTimestamp",
            "id" : "ID",
            "text" : "Text",
            "timestamp" : "Timestamp"
}

class DataBlock():
    """ Objects for one block of JSON data """
    def __init__(self, rawData):
        """ 'Constructor' for the class """
        self.rawData = rawData
        print(rawData)
        self.buildingDict = baseDict
        self.ExtractJSONBlock()

    def CalculateLeftString(self, inputString, data, start, end):
        """ Calculates the index of the last part of a substring, returns first char after substring """
        print("Searching for " + inputString, start, end)
        try:
            hitBodyLocation = data.index(inputString, start, end)
            leftBodyCharAfter = hitBodyLocation + len(inputString)
            actualHit = data.index("\":", leftBodyCharAfter, end)
            actualLeft = actualHit + 1
        except ValueError:
            return -1
        return actualLeft
    
    def ExtractJSONBlock(self):
        """ Crunches the raw data, returns calculated dictionary as JSON """
        # First, we need to find where the block of data is:
        T1 = "\"body\":"
        T2 = "\\\\\"isBase64Encoded\\\\\":"
        try:
            if T1 in self.rawData:
                body = self.CalculateLeftString(T1, self.rawData, 0, len(self.rawData)-1)
                if T2 in self.rawData:
                    end = self.CalculateLeftString(T2, self.rawData, 0, len(self.rawData))
                    if T1 != -1 and T2 != -1:
                        # Iterate the dictionary and populate if we have found & bounded the data:
                        print ("*** LAUNCHING INTO THE POPULATING FOR LOOP! ***")
                        for key in self.buildingDict:
                            print ("Now attempting to grab data for key " + key)
                            dataOut = self.CrunchData(self.rawData, key, body, end)
                            self.buildingDict[key] = dataOut
                            print ("Data returned by CrunchData for " + key + ": " + dataOut + "\n")
        except ValueError:
            print ("T1/T2 not found, processing failed.")
            #dataOut = self.CrunchData(self.rawData, "timestamp", body, end)
            #self.buildingDict[key] = dataOut
            #print ("Data returned by CrunchData for timestamp: "+dataOut)
            return "" 

    def CrunchData(self, data, keySearchingFor, body, end):
        """ Takes data we're searching for in body -> returns that data """
        print("CrunchData function.")
        print("Key searching for: "+keySearchingFor)
        hitData = self.CalculateLeftString(keySearchingFor, data, body, end)
        findAssignment = hitData
        self.rawData = self.rawData.replace(keySearchingFor, "", 1)
        try:
            # Attempt to handle it as an integer input:
            tempVal = int(data[findAssignment+1])
            endPoint = data.index(",",findAssignment,end)
            return data[findAssignment+1:endPoint]
        except ValueError:
            # Otherwise, this input is of type str:
            beginning = data.index("\"", findAssignment, end) + 1
            final = data.index("\"", beginning, end)
            secondFinal = data.index("\\", beginning, end)
            if secondFinal < final:
                return data[beginning:secondFinal]
            else:
                return data[beginning:final]
        #actualDataHit = findAssignment + 3
        #leftActualDataHit = data.index("\"", actualDataHit, end) - 2
        #return data[actualDataHit:leftActualDataHit]

def PythonSDK():
    """ Opens pipe to Python SDK, pulls back events from Splunk """
    holder = ""
    service = client.connect(host=HOST, port=PORT, username=USERNAME, password=PASSWORD)
    rr = results.ResultsReader(service.jobs.export("search index=wh_vmc_integration | head 1"))
    for result in rr:
        if isinstance(result, results.Message):
            holder = holder + str(result.message)
        elif isinstance(result, dict):
            holder = holder + str(result)
    assert rr.is_preview == False
    return holder

def SendToSplunk(node):
    """ Opens a pipe to Splunk via Hec, sends event data """
    EVENT_DATA = {"sourcetype": "wh_vmc_integration_refined", "host": "wh_vmc_python", "time": node.buildingDict["timestamp"], "event": {"log_timestamp": node.buildingDict["log_timestamp"], "sddc_id": node.buildingDict["sddc_id"], "source": node.buildingDict["source"], "priority": node.buildingDict["priority"], "log_type": node.buildingDict["log_type"], "hostname": node.buildingDict["hostname"], "event_type": node.buildingDict["event_type"], "appname": node.buildingDict["appname"], "ingest_timestamp": node.buildingDict["ingest_timestamp"], "id": node.buildingDict["id"], "text": node.buildingDict["text"], "timestamp": node.buildingDict["timestamp"]}}
    # Post this via HEC to the Splunk indexer, catching errors if necessary:
    logger.info(json.dumps(EVENT_DATA))
    # We have had a successful return code from Splunk so record the count of records.
    print ("Records loaded into Splunk")

def main():
    """ Runs the program """
    # Local variable assignment:
    trigger = "{\\\\\"text\\\\\":"
    # Focus on reading in the data:
    data = PythonSDK()
    # Place the data into an object and process:
    while trigger in data: 
        DB = DataBlock(data)
        # Pipe the data to Splunk:
        SendToSplunk(DB)
        data = data.replace(trigger, "", 1)
        for key in baseDict:
            data = data.replace(key, "", 1)

main()