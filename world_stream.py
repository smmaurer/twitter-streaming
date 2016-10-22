__author__ = "Sam Maurer"
__date__ = "October 1, 2015"
__license__ = "MIT"

# runtime hack to import code from a subfolder
import sys
sys.path.insert(0, 'stream_automator/')

import stream_automator 


s = stream_automator.Stream(
		fname_base = 'world-',
		time_limit = 0,
		bbox = '-180,-90,180,90')

s.begin_stream()