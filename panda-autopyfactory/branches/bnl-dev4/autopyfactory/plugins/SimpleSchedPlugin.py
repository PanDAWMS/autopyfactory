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
                self.log.info("SchedPlugin: Object initialized.")

        def calcSubmitNum(self, status):
                """ 
                is number of actived jobs == 0 ?
                   yes -> return 0
                   no  ->
                      is number of activated > number of idle?
                         no  -> return 0
                         yes -> 
                            is maxPendingPilots defined?
                               yes -> return (minPilotsPerCycle - nbpilots) if needed
                            is maxPilotsPerCycle defined?
                            ## FIX THIS 
                               yes -> return min(nbjobs - nbpilots, maxPilotsPerCycle)
                               no  -> return (nbjobs - nbpilots)
                """

                self.log.debug('calcSubmitNum: Starting with input %s' %status)

                # giving an initial value to some variables
                # to prevent the logging from crashing
                nbjobs = 0
                pending_pilots = 0
                running_pilots = 0

                if not status:
                        out = 0
                elif not status.valid():
                        out = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'defaultnbpilots')
                        self.log.info('calcSubmitNum: status is not valid, returning default = %s' %out)
                else:
                        nbjobs = status.jobs.get('activated', 0)
                        # '1' means pilots in Idle status
                        # '2' means pilots in Running status
                        pending_pilots = status.batch.get('1', 0)
                        running_pilots = status.batch.get('2', 0)
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
                if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'maxPendingPilots'):
                        minPilotsPerCycle = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'maxPendingPilots')
                        if maxPendingPilots > nbpilots:
                                self.log.debug('calcSubmitNum: there is a maxPendingPilots number setup to %s and it is being used' %maxPendingPilots)
                                out = maxPendingPilots - nbpilots
                                        
                # check if the config file has attribute maxPilotsPerCycle
                if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'maxPilotsPerCycle'):
                        maxPilotsPerCycle = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'maxPilotsPerCycle')
                        self.log.debug('calcSubmitNum: there is a maxPilotsPerCycle number setup to %s' %maxPilotsPerCycle)
                        out = min(out, maxPilotsPerCycle)

                self.log.debug('calcSubmitNum (activated_jobs=%s; pending_pilots=%s) : Leaving returning %s' %(nbjobs, pending_pilots, out))
                return out
