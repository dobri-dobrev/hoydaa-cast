__author__ = 'utku@hoydaa.com (Utku Utkan)'

import logging
import webapp2

from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.runtime import apiproxy_errors


class ProxyHandler(webapp2.RequestHandler):

  def get(self, url):
    fetch_url = self.request.scheme + '://' + url
    redirect_url = memcache.get(fetch_url)

    if redirect_url is not None:
      fetch_url = redirect_url

    try:
      response = urlfetch.fetch(fetch_url)
    except (urlfetch.Error, apiproxy_errors.Error):
      logging.exception('Could not fetch URL')
      self.abort(400)

    self.response.status_int = response.status_code

    for key, value in response.headers.iteritems():
      self.response.headers[key] = value

    self.response.out.write(response.content)


app = webapp2.WSGIApplication([
  (r'/proxy/(.*)', ProxyHandler),
])
