#
# 
# Simple classes for serving up logs via HTTP
# 
#

import SimpleHTTPServer
import SocketServer
import mimetypes
import logging
import os
import sys
import threading
import posixpath


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
        })
        
        base, ext = posixpath.splitext(path)
        if ext in self.extensions_map:
            return self.extensions_map[ext]
        ext = ext.lower()
        if ext in self.extensions_map:
            return self.extensions_map[ext]
        else:
            return self.extensions_map['']


class LogServer(threading.Thread):
    
    def __init__(self, port=25880, docroot="/home/apf/factory/logs"):
        '''
        docroot is the path to the base directory of the files to be served. 
        relpath is the URL pattern at which the docroot should be served. 
        I.e. if you want to serve /home/apf/factory/logs at http://localhost:23456/my/logdir
        docroot=/home/apf/factory/logs and relpath = /my/logdir
                
        '''
        threading.Thread.__init__(self)
        self.log= logging.getLogger('main.logserver')
        self.docroot = docroot
        self.port = int(port)
        #self.handler = SimpleHTTPServer.SimpleHTTPRequestHandler
        self.handler = MySimpleHTTPRequestHandler
        self.httpd = SocketServer.TCPServer(("", self.port), self.handler)
        
        self.log.debug("Initialized HTTP server. port=%d, root=%s" % (self.port, self.docroot)) 
    
    def run(self):
        self.log.debug("Starting HTTP server")
        os.chdir(self.docroot)
        self.log.debug("Changing working dir to %s"%  self.docroot)
        self.httpd.serve_forever()
        
        
        
        
if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout,level=logging.DEBUG)
    ls = LogServer()
    ls.start()
        
        