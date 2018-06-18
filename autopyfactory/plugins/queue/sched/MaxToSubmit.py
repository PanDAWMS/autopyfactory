#! /usr/bin/env python
#
import logging

from autopyfactory.interfaces import SchedInterface


class MaxToSubmit(SchedInterface):
    """
    Keep the number of jobs submitted during the whole history
    of the APFQueue below some limit
    """
    id = 'maxtosubmit'


    # TO BE IMPLEMENTED
