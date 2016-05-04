#!/bin/env python
#
# AutoPyfactory batch status plugin for OpenStack
#

import subprocess
import logging
import os
import time
import threading
import traceback
import xml.dom.minidom

from autopyfactory.factory import BatchStatusInterface
from autopyfactory.factory import BatchStatusInfo
from autopyfactory.factory import QueueInfo
from autopyfactory.factory import Singleton, CondorSingleton
from autopyfactory.info import InfoContainer
from autopyfactory.info import BatchQueueInfo

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class EucaBatchStatusPlugin(threading.Thread, BatchStatusInterface):
    '''
    -----------------------------------------------------------------------
    This class is expected to have separate instances for each PandaQueue object. 
    The first time it is instantiated, 
    -----------------------------------------------------------------------
    Public Interface:
            the interfaces inherited from Thread and from BatchStatusInterface
    -----------------------------------------------------------------------
    '''
    
    __metaclass__ = Singleton 
    
    def __init__(self, apfqueue, **kw):

        self._valid = True
        try:
            threading.Thread.__init__(self) # init the thread
            
            self.log = logging.getLogger("main.batchstatusplugin[singleton created by %s]" %apfqueue.apfqname)
            self.log.debug('BatchStatusPlugin: Initializing object...')
            self.stopevent = threading.Event()

            # to avoid the thread to be started more than once
            self.__started = False

            self.apfqueue = apfqueue
            self.apfqname = apfqueue.apfqname
            self.sleeptime = self.apfqueue.fcl.generic_get('Factory', 'batchstatus.euca.sleep', 'getint', default_value=60, logger=self.log)
            self.ec2_access_key = self.apfqueue.fcl.generic_get('Factory', 'batchstatus.euca.ec2_access_key', logger=self.log)
            self.ec2_secret_key = self.apfqueue.fcl.generic_get('Factory', 'batchstatus.euca.ec2_secret_key', logger=self.log)
            self.currentinfo = None              

            # variable to record when was last time info was updated
            # the info is recorded as seconds since epoch
            self.lasttime = 0
            self.log.info('BatchStatusPlugin: Object initialized.')
        except:
            self._valid = False

    def valid(self):    
        return self._valid 

    def getInfo(self, maxtime=0):
        '''
        Returns a BatchStatusInfo object populated by the analysis 
        over the output of a euca-describe-images command

        Optionally, a maxtime parameter can be passed.
        In that case, if the info recorded is older than that maxtime,
        None is returned, as we understand that info is too old and 
        not reliable anymore.
        '''           
        self.log.debug('getInfo: Starting with maxtime=%s' % maxtime)
        
        if self.currentinfo is None:
            self.log.debug('getInfo: Not initialized yet. Returning None.')
            return None
        elif maxtime > 0 and (int(time.time()) - self.currentinfo.lasttime) > maxtime:
            self.log.debug('getInfo: Info too old. Leaving and returning None.')
            return None
        else:                    
            self.log.debug('getInfo: Leaving and returning info of %d entries.' % len(self.currentinfo))
            return self.currentinfo


    def start(self):
        '''
        We override method start() to prevent the thread
        to be started more than once
        '''

        self.log.debug('start: Starting')

        if not self.__started:
                self.log.debug("Creating Condor batch status thread...")
                self.__started = True
                threading.Thread.start(self)

        self.log.debug('start: Leaving.')

    def run(self):
        '''
        Main loop
        '''

        self.log.debug('run: Starting')
        while not self.stopevent.isSet():
            try:
                self._update()
            except Exception, e:
                self.log.error("Main loop caught exception: %s " % str(e))
            self.log.debug("Sleeping for %d seconds..." % self.sleeptime)
            time.sleep(self.sleeptime)
        self.log.debug('run: Leaving')

    def _update(self):
        '''        
        Query OpenStack for image status, validate ?, and populate BatchStatusInfo object.
        command is    euca-describe-images        

        '''

        self.log.debug('_update: Starting.')
       
        try:
            strout = self._queryopenstack()
            if not strout:
                self.log.warning('_update: output of _querycondor is not valid. Not parsing it. Skip to next loop.') 
            else:
                newinfo = self._parseoutput(strout)
                self.log.info("Replacing old info with newly generated info.")
                self.currentinfo = newinfo
        except Exception, e:
            self.log.error("_update: Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())            

        self.log.debug('__update: Leaving.')

    def _queryopenstack(self):
        '''
        '''

        self.log.debug('_querycondor: Starting.')
        querycmd = 'euca-describe-images '

        # FIXME: Just a temporary solution
        querycmd += ' -A %s' %self.ec2_access_key
        querycmd += ' -S %s' %self.ec2_secret_key

        self.log.debug('_queryopenstack: Querying cmd = %s' %querycmd.replace('\n','\\n'))

        before = time.time()          
        p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)     
        out = None
        (out, err) = p.communicate()
        delta = time.time() - before
        self.log.debug('_queryopenstack: it took %s seconds to perform the query' %delta)
        self.log.info('OpenStack query: %s seconds to perform the query' %delta)
        if p.returncode == 0:
            self.log.debug('_queryopenstack: Leaving with OK return code.') 
        else:
            self.log.warning('_queryopenstack: Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
            out = None
        self.log.debug('_queryopenstack: Leaving. Out is %s' % out)
        return out


    def _parseoutput(self, output):
        '''
        output looks like
           IMAGE   ami-00000016    None (sl6-x86_64-wn-bnlcloud-1.0-raw)       available   public          machine         instance-store
           IMAGE   ami-0000000d    None (centos5-atlas-t3-scalr-1.0-ami)       available   public          machine         instance-store
           IMAGE   ami-0000000c    None (centos6-scalr-0.0-ami)        available   public          machine         instance-store
           IMAGE   ami-0000000b    None (sl6_1)        available   public          machine         instance-store
           IMAGE   ami-00000001    local (sl6)     available   public      x86_64  machine         instance-store

        For the time being we assume the name of the image, e.g. sl6-x86_64-wn-bnlcloud-1.0-raw
        is the name of the APF Queue.
        '''

        batchstatusinfo = InfoContainer('batch', BatchQueueInfo())
        
        # analyze output of euca command
        for line in output.split('\n'):
            key = line.split()[3][1:-1]
            if not batchstatusinfo.has_key(key):
                batchstatusinfo[key] = BatchQueueInfo()
                batchstatusinfo[key].running = 1
            else:
                batchstatusinfo[key].running = +1
        
        return batchstatusinfo

    #def _listnodesfromxml(self, xmldoc, tag):
    #    pass
    #def _node2dict(self, node):
    #    pass
    #def _aggregateinfo(self, input):
    #    pass
    #def _map2info(self, input):
    #    pass

    def join(self, timeout=None):
        ''' 
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        ''' 

        self.log.debug('join: Starting with input %s' %timeout)
        self.stopevent.set()
        self.log.debug('Stopping thread....')
        threading.Thread.join(self, timeout)
        self.log.debug('join: Leaving')


def test():
    pass
    
if __name__=='__main__':
    pass



