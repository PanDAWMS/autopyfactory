#
# 
'''
   Simple classes for serving up logs via HTTP
   
   @note log_message overridden in order to log to standard logs rather than stderr.
   @note extensions_map expanded to handle Condor logfile extensions so user sees them as text rather
      than being offered a download. 
'''

import logging
import mimetypes
import os
import posixpath
import sys
import threading
import SimpleHTTPServer
import SocketServer
import time

__author__ = "John Hover"
__copyright__ = "2010,2011, John Hover"
__credits__ = []
__license__ = "GPL"
__version__ = "2.1.0"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

class MySimpleHTTPRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):

    def guess_type(self, path):
        """Guess the type of a file.

        Argument is a PATH (a filename).

        Return value is a string of the form type/subtype,
        usable for a MIME Content-type header.

        The default implementation looks the file's extension
        up in the table self.extensions_map, using application/octet-stream
        as a default; however it would be permissible (if
        slow) to look inside the data to make a better guess.

        """
        self.extensions_map = mimetypes.types_map.copy()
        self.extensions_map.update({
                '': 'application/octet-stream', # Default
                '.py': 'text/plain',
                '.c': 'text/plain',
                '.h': 'text/plain',
                '.log': 'text/plain',
                '.out': 'text/plain',
                '.err': 'text/plain',
                '.jdl': 'text/plain',
                '.conf': 'text/plain',
                '.txt': 'text/plain',
                '.sh': 'text/plain',
        })
        
        base, ext = posixpath.splitext(path)
        if ext in self.extensions_map:
            return self.extensions_map[ext]
        ext = ext.lower()
        if ext in self.extensions_map:
            return self.extensions_map[ext]
        else:
            return self.extensions_map['']

    def log_message(self, format, *args):
            """Log an arbitrary message.
    
            This is used by all other logging functions.  Override
            it if you have specific logging wishes.
    
            The first argument, FORMAT, is a format string for the
            message to be logged.  If the format string contains
            any % escapes requiring parameters, they should be
            specified as subsequent arguments (it's just like
            printf!).
    
            The client host and current date/time are prefixed to
            every message.
    
            """
            
            log = logging.getLogger('main.logserver')
            log.info("%s - - [%s] %s\n" %
                             (self.address_string(),
                              self.log_date_time_string(),
                              format%args))
            
            #sys.stderr.write("%s - - [%s] %s\n" %
            #                 (self.address_string(),
            #                  self.log_date_time_string(),
            #                  format%args))

class MyNoListingHTTPRequestHandler(MySimpleHTTPRequestHandler):
    
    def list_directory(self, path):
        return None
        


class LogServer(threading.Thread):
    
    def __init__(self, port=25880, docroot="/home/apf/factory/logs", index = True):
        '''
        docroot is the path to the base directory of the files to be served. 
        '''
        threading.Thread.__init__(self)
        self.log= logging.getLogger('main.logserver')
        self.docroot = docroot
        self.port = int(port)
        self.index = index
        self.stopevent = threading.Event()
        if index:
            self.handler = MySimpleHTTPRequestHandler
        else:
            self.handler = MyNoListingHTTPRequestHandler
        self.httpd = None
        self.log.debug("Initialized Logserver: port=%d, root=%s, index=%s" %(self.port,
                                                                             self.docroot,
                                                                             self.index))
        
    
    def _init_socketserver(self):
        while not self.httpd:
            try:
                self.log.debug("Attempting to bind to socket for HTTP server on port %s" % self.port)
                self.httpd = SocketServer.TCPServer(("", self.port), self.handler)
                self.log.info("Initialized HTTP SocketServer port=%d, root=%s, index = %s" % (self.port, 
                                                                                             self.docroot, 
                                                                                             self.index)) 
            except Exception, e:
                self.log.warning("Socket server exception: %s" % str(e))
                self.log.warning("Attempt to initialize HTTP server failed. Will wait 60s and try again.")         
                time.sleep(60)
    
    def run(self):
        self.log.info("Initializing HTTP server...")
        self._init_socketserver()
        
        
        os.chdir(self.docroot)
        self.log.debug("Changing working dir to %s"%  self.docroot)
        while not self.stopevent.isSet():
            try:
                self.httpd.serve_forever()
            except Exception, e:
                self.log.error("HTTP Server threw exception: %s" % str(e))

    def join(self,timeout=None):
        '''
        Stop the thread. Overriding this method required to handle Ctrl-C from console.
        '''        
        self.stopevent.set()
        self.log.info('Stopping thread...')
        self.httpd.shutdown()
        threading.Thread.join(self, timeout)

                

# simple main for testing during development                
if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout,level=logging.DEBUG)
    # ls = LogServer()
    ls = LogServer(index = False)
    ls.start()
        
        
