__author__ = "Sam Maurer"
__date__ = "September 20, 2015"
__license__ = "MIT"

# runtime hack to import code from a subfolder
import sys
sys.path.insert(0, 'stream_automator/')

import stream_automator 


s = stream_automator.Stream()
s.begin_stream()