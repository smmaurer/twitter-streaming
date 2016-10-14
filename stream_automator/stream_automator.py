__author__ = "Sam Maurer"
__date__ = "October 14, 2016"
__license__ = "MIT"


import json
import os
import time
import zipfile
from datetime import datetime as dt
from TwitterAPI import TwitterAPI

from keys import *   # keys.py in same directory


OUTPUT_PATH = 'data/'  # output path relative to the script calling this class
FNAME_BASE = 'stream-'  # default filename prefix (timestamp will be appended)
TIME_LIMIT = 0  # default time limit in seconds, 0 for none
ROWS_PER_FILE = 500000  # 500k tweets is about 1.6 GB uncompressed
DELAY = 5.0  # initial reconnection delay in seconds
BBOX = '-126,29,-113,51'  # default bounding box (US west coast)
COMPRESS = True  # whether to zip the resulting data file


class Stream(object):

    def __init__(
            self,
            fname_base = FNAME_BASE,
            time_limit = TIME_LIMIT,
            bbox = BBOX ):
    
        self.api = TwitterAPI(consumer_key, consumer_secret, access_token, access_token_secret)

        self.fname_base = fname_base
        self.time_limit = time_limit
        self.bbox = bbox

        self.t0 = None  # initialization time
        self.fpath = ''  # output filepath
        self.f = None  # output file
        self.tcount = 0  # tweet count in current file
        self._reset_delay()
    
    
    def _reset_delay(self):
        self.delay = DELAY/2
        return
    
    
    def begin_stream(self):
        '''Initialize the streaming connection and reconnect if needed'''
        
        while True:
            try:
                r = self.api.request('statuses/filter', 
                        {'locations': self.bbox, 'stall_warnings': 'true'})
                _test = r.get_iterator()
                print "\n" + str(dt.now())
                print "Connected to streaming endpoint"
                self.t0 = time.time()
                self._reset_delay()  # reset the delay after a successful connection
                self._save_tweets(r)  # save tweets to disk
                r.close()
                return
        
            except KeyboardInterrupt:
                try:
                    self._close_files()
                except:
                    pass
                print
                return
            
            except IOError, e:
                print "%s: %s" % (type(e).__name__, e)
                return

            except Exception, e:
                # catch dropped stream and try to reconnect:
                # - TwitterAPI throws TwitterRequestError, TwitterConnectionError
                # - httplib.IncompleteRead triggers the latter
                self.delay = self.delay * 2
                print "\n" + str(dt.now())
                print "%s: %s" % (type(e).__name__, e)
                print "Attempting to reconnect after %d sec. delay" % (self.delay,)
                time.sleep(self.delay)
                continue
    
    
    def _save_tweets(self, r):
        '''Read tweets from the open connection and save to disk'''

        for tweet in r.get_iterator():

            # log stall warnings
            if 'warning' in tweet:
                print "\n" + str(dt.now())
                print tweet['warning']
                continue

            # initialize new output file if tweet count is 0
            if (self.tcount == 0):
                ts = dt.now().strftime('%Y%m%d-%H%M%S')
                self.fpath = OUTPUT_PATH + self.fname_base + ts + '.json'
                self.f = open(self.fpath, 'w')
    
            # save to file, or skip if the data is incomplete or has encoding errors
            try:
                self.f.write(json.dumps(tweet) + '\n')
                self.tcount += 1
            except:
                continue
    
            # close the output file when it fills up
            if (self.tcount >= ROWS_PER_FILE):
                self._close_files()
                self.tcount = 0

            # if we reach a time limit, end the script
            if (self.time_limit > 0) & ((time.time() - self.t0) >= self.time_limit):
                self._close_files()
                return


    def _close_files(self):
        '''Close output file and compress if requested'''
        
        # close the output file
        self.f.close()
        
        if COMPRESS:
        
            # zip the output file
            with zipfile.ZipFile(self.fpath + '.zip', 'w', zipfile.ZIP_DEFLATED) as z:
                arcname = self.fpath.split('/')[-1]  # name for file inside archive
                z.write(self.fpath, arcname)
            
            # delete the uncompressed copy
            os.remove(self.fpath)     
    

