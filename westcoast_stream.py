# Sam Maurer, Sept 2015
# Streams tweets to a file

# Twitter REST API: https://dev.twitter.com/docs/api/1.1
# Python 'TwitterAPI' module: https://github.com/geduldig/TwitterAPI

from TwitterAPI import TwitterAPI
from datetime import datetime as dt
import json
import time

from keys import *   # keys.py in same directory

api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

OUTPUT_PATH = 'data/'
FNAME_BASE = 'westcoast-'
TIME_LIMIT = 0  # in seconds, 0 for none
ROWS_PER_FILE = 500000  # 500k tweets is about 1.6 GB uncompressed

BBOX = '-126,29,-113,51'  # West Coast from Tijuana to Vancouver and east to edge of CA

t0 = time.time()  # initialization time
tcount = 0  # tweet count in current file
delay = 0.5  # reconnection delay in seconds


def stream():

	while True:
		try:
			# is it 'stall_warning' singular or plural? documentation disagrees
			r = api.request('statuses/filter', {'locations': BBOX, 'stall_warning': 'true'})
			_test = r.get_iterator()
			print dt.now()
			print "Connected to streaming endpoint"
			delay = 0.5  # reset the delay after a successful connection
			save_tweets(r)
			r.close()
			return
		
		except Exception, e:
			# common: TwitterRequestError, TwitterConnectionError, httplib.IncompleteRead
			r.close()  # try to close old stream
			delay = delay * 2
			print dt.now()
			print "%s: %s" % (type(e).__name__, e)
			print "Attempting to reconnect after %d sec. delay" % (delay,)
			time.sleep(delay)
			continue


def save_tweets(r):

	try:
		for tweet in r.get_iterator():
	
			# log stall warnings
			if 'warning' in tweet:
				print dt.now()
				print tweet['warning']
				continue

			# initialize new output file if tweet count is 0
			if (tcount == 0):
				ts = dt.now().strftime('%Y%m%d-%H%M%S')
				fname = OUTPUT_PATH + FNAME_BASE + ts + '.json'
				f = open(fname, 'w')
		
			# save to file, or skip if the data is incomplete or has encoding errors
			try:
				f.write(json.dumps(tweet) + '\n')
				tcount += 1
			except:
				continue
		
			# close the output file when it fills up
			if (tcount >= ROWS_PER_FILE):
				f.close()
				tcount = 0

			# if we reach a time limit, end the script
			if (TIME_LIMIT > 0) & ((time.time() - t0) >= TIME_LIMIT):
				f.close()
				return

	except KeyboardInterrupt:
		f.close()
		return


stream()


# TO DO
# - make this into a proper class so we can deal with status variables better
# - print updates about number of tweets collected and length of time
# - allow setting for number of tweets to collect
# - make sure file is closed if there's a KeyboardInterrupt while trying to reconnect
#   after the stream is dropped
