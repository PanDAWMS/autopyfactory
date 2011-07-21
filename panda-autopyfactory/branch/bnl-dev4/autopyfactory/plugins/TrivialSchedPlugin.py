#! /usr/bin/env python
#
#
#


from autopyfactory.factory import SchedInterface
import logging

class SchedPlugin(SchedInterface):
        
        def __init__(self, wmsqueue):
                self.log = logging.getLogger("main.schedplugin[%s]" %wmsqueue.siteid)
                self.log.info("SchedPlugin: Object initialized.")

        def calcSubmitNum(self, status):
                """ 
                is number of actived jobs == 0 ?
                        yes -> return 0
                        no ->
                                is number of activated > number of idle+running pilots?
                                        yes -> return nbjobs - nbpilots
                                        no -> return 0
                """

                self.log.debug('calcSubmitNum: Starting with input %s' %status)

                if not status:
                        out = 0
                else:
                        nbjobs = status.jobs.get('activated', 0)
                        nbpilots = status.batch.get('1', 0) + status.batch.get('2', 0)
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

                self.log.debug('calcSubmitNum: Leaving returning %s' %out)
                return out
