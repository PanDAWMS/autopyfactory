#! /usr/bin/env python
#

"""

        NOTE:   Most probably this module 
                will be deleted
                since all functionalities 
                are included in Activated Plugin
              
"""


from autopyfactory.factory import SchedInterface
import logging

__author__ = "John Hover, Jose Caballero"
__copyright__ = "2011 John Hover, Jose Caballero"
__credits__ = []
__license__ = "GPL"
__version__ = "2.0.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class SchedPlugin(SchedInterface):
    
    def __init__(self, wmsqueue):
        self.wmsqueue = wmsqueue                
        self.log = logging.getLogger("main.schedplugin[%s]" %wmsqueue.apfqueue)

        self.default = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.simple.default')
        self.log.info('SchedPlugin: default value = %s' %self.default)

        self.maxpendingpilots = None
        self.maxpilotpercycle = None

        if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'sched.simple.maxpendingpilots'):
            self.maxpendingpilots = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.simple.maxpendingpilots')
            self.log.debug('SchedPlugin: there is a maxPendingPilots number setup to %s' %self.maxPendingPilots)

        if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'sched.simple.maxpilotspercycle'):
            self.maxpilotspercycle = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'sched.simple.maxpilotspercycle')
            self.log.debug('SchedPlugin: there is a maxpilotspercycle number setup to %s' %maxpilotspercycle)

        self.log.info("SchedPlugin: Object initialized.")

    def calcSubmitNum(self):
        """ 
        is number of actived jobs == 0 ?
           yes -> return 0
           no  ->
              is number of activated > number of idle?
                 no  -> return 0
                 yes -> 
                    is maxPendingPilots defined?
                       yes -> return (maxPendingPilots - nbpilots) if needed
                    is maxPilotsPerCycle defined?
                    ## FIX THIS 
                       yes -> return min(nbjobs - nbpilots, maxPilotsPerCycle)
                       no  -> return (nbjobs - nbpilots)
        """

        self.log.debug('calcSubmitNum: Starting')

        # giving an initial value to some variables
        # to prevent the logging from crashing
        nbjobs = 0
        pending_pilots = 0
        running_pilots = 0

        wmsinfo = self.wmsqueue.wmsstatus.getInfo(maxtime = self.wmsqueue.wmsstatusmaxtime)
        batchinfo = self.wmsqueue.batchstatus.getInfo(maxtime = self.wmsqueue.batchstatusmaxtime)

        if wmsinfo is None:
            self.log.warning("wsinfo is None!")
            out = self.default
        elif batchinfo is None:
            self.log.warning("batchinfo is None!")
            out = self.default
        elif not wmsinfo.valid() and batchinfo.valid():
            out = self.default
            self.log.warn('calcSubmitNum: a status is not valid, returning default = %s' %out)
        else:
            nbjobs = wmsinfo.jobs[self.wmsqueue.apfqueue]['activated']                     
            pending_pilots = batchinfo[self.wmsqueue.apfqueue].pending
            running_pilots = batchinfo[self.wmsqueue.apfqueue].running
            nbpilots = pending_pilots
            
            # note: the following if-else algorithm can be written
            #       in a simpler way, but in this way is easier to 
            #       read and to understand what it does and why.
            if nbjobs == 0:
                out = 0
            else:
                if nbjobs > nbpilots:
                        out = nbjobs - nbpilots
                else:
                        out = 0

        # check if there is a maximum number of pending pilots 
        # and submit as many pilots as needed to complete that maximum
        if self.maxpendingpilots:
            if self.maxpendingpilots > nbpilots:
                self.log.debug('calcsubmitnum: there is a maxpendingpilots number setup to %s and it is being used' %self.maxpendingpilots)
                out = self.maxpendingpilots - nbpilots
                                
        # check if the config file has attribute maxpilotspercycle
        if self. maxpilotspercycle:
            self.log.debug('calcsubmitnum: there is a maxpilotspercycle number setup to %s' %self.maxpilotspercycle)
            out = min(out, self.maxpilotspercycle)

        self.log.debug('calcSubmitNum (activated_jobs=%s; pending_pilots=%s) : Leaving returning %s' %(nbjobs, pending_pilots, out))
        return out
