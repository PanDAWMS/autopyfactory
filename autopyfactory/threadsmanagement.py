#! /usr/bin/env python

import logging
import logging.handlers

from autopyfactory.apfexceptions import ThreadRegistryInvalidKind


class ThreadsRegistry(object):

    def __init__(self, kinds=['plugin','queue','util','core']):

        self.log = logging.getLogger('autopyfactory')

        # the kinds of threads allowed
        # to be registered,
        # sorted in the order they will be join()'ed
        self.kinds = kinds

        # initialization of the registry
        self.threads = {}
        for kind in self.kinds:
            self.threads[kind] = []
            
    def add(self, kind, thread):
        """
        adds a new thread to the registry.

        Inputs:
        -------
        - kind: the type of thread
                It must be one of the keys in the self.threads dictionary.
        - thread: the object to be added to the registry
        """
        self.log.debug('adding a thread of type %s: %s' %(kind, thread.__class__.__name__))
        if kind not in self.kinds:
            raise ThreadRegistryInvalidKind(kind, thread)
        self.threads[kind].append(thread)

    def join(self):
        """ 
        stops all threads registered, in the right order.
        """
        for kind in self.kinds:
            self.join_kind(kind)

    def join_kind(self, kind):
        """
        stops all threads registered of a given kind

        Inputs:
        -------
        - kind: the type of threads to join(). 
                It must be one of the keys in the self.threads dictionary.
        """
        threads = self.threads[kind]
        msg = 'stopping %s %s thread(s)' %(len(threads), kind)
        self.log.debug(msg)
        for thread in threads:
            msg = 'stopping another %s thread' %kind
            self.log.debug(msg)
            thread.join(5)
                
