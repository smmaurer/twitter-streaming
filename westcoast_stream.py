# Sam Maurer, Sept 2015
# Streams tweets to a file

# Twitter REST API: https://dev.twitter.com/docs/api/1.1
# Python 'TwitterAPI' module: https://github.com/geduldig/TwitterAPI

from TwitterAPI import TwitterAPI
import json
from datetime import datetime as dt
import time

import keys  # from keys.py in same directory
api = TwitterAPI(consumer_key, consumer_secret, access_token_key, access_token_secret)

PATH = 'data/'
FNAME_BASE = 'westcoast-'
TIME_LIMIT = 2  # in seconds, 0 for none
ROWS_PER_FILE = 500000  # 500k tweets is about 1.6 GB uncompressed

BBOX = '-126,29,-113,51'  # West Coast from Tijuana to Vancouver and east to edge of CA
stream()

def stream():
	r = api.request('statuses/filter', {'locations': BBOX})

	tcount = 0 # tweets in file
	t0 = time.time()

	while True:
		try:
			for tweet in r.get_iterator():
		
				if (tcount == 0):
					ts = dt.now().strftime('%Y%m%d-%H%M%S')
					fname = PATH + FNAME_BASE + ts + '.json'
					f = open(fname, 'w')
				
				try:
					f.write(json.dumps(tweet) + '\n')
					tcount += 1
				except:
					pass
				
				if (tcount >= ROWS_PER_FILE):
					f.close()
					tcount = 0

				if (TIME_LIMIT > 0) & ((time.time() - t0) >= TIME_LIMIT):
					f.close()
					break

		except KeyboardInterrupt:
			f.close()
			break
	
		# Catch exceptions occasionally thrown by r.get_iterator() - eg ChunkedEncodingError
		except Exception, e:
			print e
			continue
		
		break
	
# to do - how to capture diagnostic info about stream quality?
