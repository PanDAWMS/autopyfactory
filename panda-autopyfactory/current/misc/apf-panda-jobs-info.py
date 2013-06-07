#!/usr/bin/python

import commands
import getopt
import sys


#
#  Script to get info from PanDA.
#  Some documentation:
#       https://twiki.cern.ch/twiki/bin/viewauth/Atlas/PandaPlatform#CLI_API 
#  Inputs:
#       --tstart <start time>
#       --tend <end time>
#       --site <site>
#  Example:
#       ./apf-panda-jobs-info.py  --start=2013-06-06+00:00:00 --end=2013-06-07+00:00:00 --site=ANALY_BNL_SHORT


jobfields = ['PandaID',
             'assignedPriority',
             'AtlasRelease',
             'attemptNr',
             'batchID',
             'brokerageErrorCode',
             'brokerageErrorDiag',
             'cloud',
             'cmtConfig',
             'commandToPilot',
             'computingElement',
             'computingSite',
             'coreCount',
             'countryGroup',
             'cpuConsumptionTime',
             'cpuConsumptionUnit',
             'cpuConversion',
             'creationHost',
             'creationTime',
             'currentPriority',
             'ddmErrorCode',
             'ddmErrorDiag',
             'destinationDBlock',
             'destinationSE',
             'destinationSite',
             'dispatchDBlock',
             'endTime',
             'exeErrorCode',
             'exeErrorDiag',
             'grid',
             'homepackage',
             'inputFileBytes',
             'INPUTFILEPROJECT',
             'inputFileType',
             'ipConnectivity',
             'jobDefinitionID',
             'jobDispatcherErrorCode',
             'jobDispatcherErrorDiag',
             'jobExecutionID',
             'JOBMETRICS',
             'jobName',
             #'jobParameters',
             'jobsetID',
             'jobStatus',
             'lockedby',
             'maxAttempt',
             'maxCpuCount',
             'maxCpuUnit',
             'maxDiskCount',
             'maxDiskUnit',
             #'metadata',
             'minRamCount',
             'minRamUnit',
             'modificationHost',
             'modificationTime',
             'nEvents',
             'nInputDataFiles',
             'nInputFiles',
             'NOUTPUTDATAFILES',
             'OUTPUTFILEBYTES',
             #'PandaID',
             'parentID',
             'pilotErrorCode',
             'pilotErrorDiag',
             'pilotID',
             'pilotTiming',
             'processingType',
             'prodDBlock',
             'prodDBUpdateTime',
             'prodSeriesLabel',
             'prodSourceLabel',
             'prodUserID',
             'prodUserName',
             'relocationFlag',
             'schedulerID',
             'sourceSite',
             'specialHandling',
             'startTime',
             'stateChangeTime',
             'supErrorCode',
             'supErrorDiag',
             'taskBufferErrorCode',
             'taskBufferErrorDiag',
             'taskID',
             'transExitCode',
             'transferType',
             'transformation',
             'VO',
             'workingGroup',
             ]


class info(object):

    def __init__(self): 

        opts, args = getopt.getopt(sys.argv[1:], "", ["start=", "end=", "site="]) 
        for opt, arg in opts:
            if opt == "--start":
                start = arg
            if opt == "--end":
                end = arg
            if opt == "--site":
                site = arg


        self.cmd ="curl -s 'http://pandamon.cern.ch/jobinfo?jobparam="
        attrs = ','.join(jobfields[1:])
        self.cmd += attrs
        self.cmd += "&computingSite="
        self.cmd += site
        self.cmd += "&tstart="
        self.cmd += start
        self.cmd += "&tend="
        self.cmd += end
        self.cmd += "&hours=&days=&dump=yes&limit=10000'"
        # example
        #   &computingSite=BNL_ATLAS_RCF&tstart=2013-05-18+00:00:00&tend=2013-05-20+00:00&hours=&days=&dump=yes&limit=10000'"


    def query(self):

        self.out = commands.getoutput(self.cmd)
        null  = None  # map JSON 'null'  to the python "None" (see: http://docs.python.org/2/library/json.html#json-to-py-table for details )
        false = False # map JSON 'false' to the python "False"
        true  = True  # map JSON 'true'  to the python "True"
        jtxt = eval(self.out)
        jdict =jtxt['pm']
        jobs1 = jdict[0]
        jobs2 = jobs1['json']['info']
        self.header = jobs1['json']['header']
        
        self.jobs = []
        for j in jobs2:
            self.jobs.append( dict(zip(self.header,j)) )

    def display(self):
        for j in self.jobs:
            msg = ""
            tokens=[]
            for field in self.fields:
                tokens.append("%s=%s" %(field, j[field]))
            msg = "|".join(tokens)
            print msg


if __name__ == '__main__':
    i = info()
    i.query()
    i.display()

    #for j in i.jobs:
    #    print j['pilotErrorDiag']





