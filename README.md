# twitter-streaming

This is some code to automate the process of streaming geolocated tweets to disk, for research purposes. It's built on top of Jonas Geduldig's [TwitterAPI](https://github.com/geduldig/TwitterAPI) python wrapper. 

The `Stream()` class in `stream_automator.py` initializes a streaming connection that filters for public tweets within a geographic bounding box. It saves the tweets to a json file, and attempts to handle a variety of common errors including dropped connections.

#### How to get started:

* Install the [TwitterAPI](https://github.com/geduldig/TwitterAPI) python package
* Clone this repository to your machine (or fork it, etc)
* [Register](https://dev.twitter.com/apps/new) to obtain Twitter API credentials
* Using `keys-example.py` as a template, make a new file called `keys.py` and paste your credentials into it
* If you've forked this repo, don't post your keys to GitHub! The `.gitignore` file should catch it, but double check before pushing any commits
* Try running `westcoast_stream.py` as a demo, and take a look at the code in `stream_automator.py` to see what it's doing and what some of the options are