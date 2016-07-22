'''
  APF Module to handle plugins dispatch, config.
  Not named 'plugins' because there is a lib directory with that name.  

  Model is plugins are kept in hierarchy by 'category':
    factory: plugins that the factory uses
    queue:   plugins that APFQueues use
    profile:  plugins looked up by label
    
  Beneath the catetogory, there are types
    factory:
       config:
         file
         agis
       monitor:
         apfmon
    queue:
       wmsstatus:
         condor
         panda
       batchstatus
         condor
       batchsubmit
         condor
       sched
         <various>
    profile:
       auth:
         x509
         ssh
         cloud
      

'''


import logging
import logging.handlers
import traceback

from pprint import pprint





