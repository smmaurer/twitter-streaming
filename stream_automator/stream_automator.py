__author__ = "Sam Maurer"
__date__ = "October 1, 2015"
__license__ = "MIT"


from TwitterAPI import TwitterAPI
from datetime import datetime as dt
import json
import time

from keys import *   # keys.py in same directory


OUTPUT_PATH = 'data/'  # output path relative to the script calling this class
FNAME_BASE = 'stream-'  # filename prefix (timestamp will be appended)
TIME_LIMIT = 0  # in seconds, 0 for none
ROWS_PER_FILE = 500000  # 500k tweets is about 1.6 GB uncompressed
DELAY = 1.0  # initial reconnection delay in seconds
BBOX = '-126,29,-113,51'  # SET TO BAY AREA


class Stream(object):

	def __init__(self):
	
		self.api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)
		
		self.t0 = None  # initialization time
		self.tcount = 0  # tweet count in current file
		self._reset_delay()
	
	
	def _reset_delay(self):
		self.delay = DELAY/2
		return
	
	
	def begin_stream(self):
		'''Initialize the streaming connection and reconnect if needed'''
		
		while True:
			try:
				# is it 'stall_warning' singular or plural? documentation disagrees
				r = self.api.request('statuses/filter', {'locations': BBOX, 'stall_warning': 'true'})
				_test = r.get_iterator()
				print dt.now()
				print "Connected to streaming endpoint"
				self.t0 = time.time()
				self._reset_delay()  # reset the delay after a successful connection
				self._save_tweets(r)  # save tweets to disk
				r.close()
				return
		
			except KeyboardInterrupt:
				print
				return
			
			except IOError, e:
				print "%s: %s" % (type(e).__name__, e)
				return

			except Exception, e:
				# catch dropped streaming and try to reconnect -- typically
				# TwitterRequestError, TwitterConnectionError, httplib.IncompleteRead
				self.delay = self.delay * 2
				print dt.now()
				print "%s: %s" % (type(e).__name__, e)
				print "Attempting to reconnect after %d sec. delay" % (self.delay,)
				time.sleep(self.delay)
				continue
	
	
	def _save_tweets(self, r):
		'''Read tweets from the open connection and save to disk'''

		try:
			for tweet in r.get_iterator():
	
				# log stall warnings
				if 'warning' in tweet:
					print dt.now()
					print tweet['warning']
					continue

				# initialize new output file if tweet count is 0
				if (self.tcount == 0):
					ts = dt.now().strftime('%Y%m%d-%H%M%S')
					fname = OUTPUT_PATH + FNAME_BASE + ts + '.json'
					f = open(fname, 'w')
		
				# save to file, or skip if the data is incomplete or has encoding errors
				try:
					f.write(json.dumps(tweet) + '\n')
					self.tcount += 1
				except:
					continue
		
				# close the output file when it fills up
				if (self.tcount >= ROWS_PER_FILE):
					f.close()
					self.tcount = 0

				# if we reach a time limit, end the script
				if (TIME_LIMIT > 0) & ((time.time() - self.t0) >= TIME_LIMIT):
					f.close()
					return

		except KeyboardInterrupt:
			f.close()
			print
			return

