#! /usr/bin/env python
#

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
                By default, returns nb of Activated Jobs - nb of Pending Pilots

                But, if MAX_JOBS_RUNNING is defined, 
                it can impose a limit on the number of new pilots,
                to prevent passing that limit on max nb of running jobs.

                If there is a MAX_PILOTS_PER_CYCLE defined, 
                it can impose a limit too.

                If there is a MIN_PILOTS_PER_CYCLE defined, 
                and the final decission was a lower number, 
                then this MIN_PILOTS_PER_CYCLE is the number of pilots 
                to be submitted.
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
                        nbpilots = pending_pilots + running_pilots

                        out = max(0, nbjobs - pending_pilots)
                
                        # check if the config file has attribute MAX_JOBS_TORUN
                        if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'MAX_JOBS_TORUN'):
                                MAX_JOBS_TORUN = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'MAX_JOBS_TORUN')
                                self.log.debug('calcSubmitNum: there is a MAX_JOBS_TORUN number setup to %s' %MAX_JOBS_TORUN)
                                out_2 = max(0, MAX_JOBS_TORUN - nbpilots)
                                out = min(out, out_2)

                # check if the config file has attribute MAX_PILOTS_PER_CYCLE 
                if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'MAX_PILOTS_PER_CYCLE'):
                        MAX_PILOTS_PER_CYCLE = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'MAX_PILOTS_PER_CYCLE')
                        self.log.debug('calcSubmitNum: there is a MAX_PILOTS_PER_CYCLE number setup to %s' %MAX_PILOTS_PER_CYCLE)
                        out = min(out, MAX_PILOTS_PER_CYCLE)

                # check if the config file has attribute MIN_PILOTS_PER_CYCLE 
                if self.wmsqueue.qcl.has_option(self.wmsqueue.apfqueue, 'MIN_PILOTS_PER_CYCLE'):
                        MIN_PILOTS_PER_CYCLE = self.wmsqueue.qcl.getint(self.wmsqueue.apfqueue, 'MIN_PILOTS_PER_CYCLE')
                        self.log.debug('calcSubmitNum: there is a MIN_PILOTS_PER_CYCLE number setup to %s' %MIN_PILOTS_PER_CYCLE)
                        out = max(out, MIN_PILOTS_PER_CYCLE)

                self.log.debug('calcSubmitNum (activated_jobs=%s; pending_pilots=%s; running_pilots=%s;) : Leaving returning %s' %(nbjobs, pending_pilots, running_pilots, out))
                return out
