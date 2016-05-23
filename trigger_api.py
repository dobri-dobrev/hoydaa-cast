__author__ = 'dobrev.d10@gmail.com (Dobri Dobrev)'

import logging
import urllib
import urlparse
import os
import webapp2
import time
from webapp2_extras import json
from datetime import timedelta

from google.appengine.api import memcache
_HLS_MASTER_BASE_URI = os.getenv('HLS_MASTER_BASE_URI').rstrip('/') + '/'
_TTL_1_HOUR = 3600

# there should be a better way than keeping these in sourcecode
# im thinking maybe env variables but no idea how to set those in gcloud
_AUTH_KEY_FOR_TV8 = 'DO0PktvfU0OfNGmgW6fARXGrP'
_CHANNEL_ID_FOR_TV8 = 'tv8acdg'

class APIHandler(webapp2.RequestHandler):

  def get(self):
    timestamp = int(round(time.time()*1000))
    channel_id_param = self.request.get('channel_id')
    auth_key_param = self.request.get('auth_key')
    duration_param = self.request.get('duration')
    if (channel_id_param == _CHANNEL_ID_FOR_TV8 
    	and auth_key_param == _AUTH_KEY_FOR_TV8
    	and duration_param):
      memcache.set(_HLS_MASTER_BASE_URI, timestamp, time=_TTL_1_HOUR)
      logging.info('received ad trigger with channel_id %s, auth_key %s, timestamp %i, duration %s' %
        (channel_id_param, auth_key_param, timestamp, duration_param))
      self.response.content_type = 'application/json'
      # for debugging purposes
      # obj = {
      #     'duration_param': duration_param, 
      #     'auth_key_param': auth_key_param,
      #     'channel_id_param': channel_id_param,
      #     'timestamp': timestamp
      #   } 
      obj = {
        'success': True
      }
      self.response.write(json.encode(obj))
    else:
      logging.info('INVALID ad trigger with channel_id %s, auth_key %s, timestamp %i, duration %s' %
        (channel_id_param, auth_key_param, timestamp, duration_param))
      self.response.set_status(500)
      self.response.write('A server error occurred!')

app = webapp2.WSGIApplication([
  (r'/trigger', APIHandler)
], debug=True)