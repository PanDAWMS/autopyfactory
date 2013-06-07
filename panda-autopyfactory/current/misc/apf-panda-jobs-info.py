#!/usr/bin/python

import commands
import getopt
import sys


#
#  Script to get info from PanDA.
#  Some documentation:
#       https://twiki.cern.ch/twiki/bin/viewauth/Atlas/PandaPlatform#CLI_API 
#  Inputs:
#       --tstart <start time> [MANDATORY] 
#       --tend <end time>     [MANDATORY]    
#       --site <site>         [MANDATORY] 
#       --fields <split by comma list of fields> 
#
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

        self._parseargs()
        self._createcmd()

    def _parseargs(self):

        self.tstart = None
        self.tend = None
        self.site = None
        self.fields = None

        opts, args = getopt.getopt(sys.argv[1:], "", ["start=", "end=", "site=", "fields="]) 
        for opt, arg in opts:
            if opt == "--start":
                self.tstart = arg
            if opt == "--end":
                self.tend = arg
            if opt == "--site":
                self.site = arg
            if opt == "--fields":
                self.fields = arg

    def _createcmd(self):

        fields = self._getfields()

        self.cmd ="curl -s 'http://pandamon.cern.ch/jobinfo?jobparam="
        self.cmd += fields
        self.cmd += "&computingSite="
        self.cmd += self.site
        self.cmd += "&tstart="
        self.cmd += self.tstart
        self.cmd += "&tend="
        self.cmd += self.tend
        self.cmd += "&hours=&days=&dump=yes&limit=10000'"
        # example
        #   &computingSite=BNL_ATLAS_RCF&tstart=2013-05-18+00:00:00&tend=2013-05-20+00:00&hours=&days=&dump=yes&limit=10000'"


    def _getfields(self):

        if self.fields:
            return self.fields
        else:
            return ','.join(jobfields[1:])


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





