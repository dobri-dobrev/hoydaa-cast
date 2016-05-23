__author__ = 'utku@hoydaa.com (Utku Utkan)'

import logging
import os
import urllib
import urlparse

import jinja2
import m3u8
import webapp2

from datetime import timedelta

from google.appengine.api import memcache
from google.appengine.api import users


_HLS_MASTER_BASE_URI = os.getenv('HLS_MASTER_BASE_URI').rstrip('/') + '/'

_TARGET_MEDIA_SEGMENT_OFFSET = 1
_TARGET_PREFIX_GENDER = 'gender_'

_ADS_DIRNAME = 'ads'

_TTL_1_HOUR = 3600

_USER_WHITELIST = os.getenv('USER_WHITELIST').split(',')

_JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

_TEMPLATES_DIRNAME = 'templates'


def restricted(handler_method):

  def check_user(self, *args, **kwargs):
    if users.is_current_user_admin():
      return handler_method(self, *args, **kwargs)

    user = users.get_current_user()

    if user and user.nickname() in _USER_WHITELIST:
      return handler_method(self, *args, **kwargs)

    self.abort(403)

  return check_user


class BroadcastHandler(webapp2.RequestHandler):

  @restricted
  def post(self):
    ad_filename_param = self.request.get('ad_filename')
    ad_duration_param = self.request.get('ad_duration')
    channel_param = self.request.get('channel')
    gender_params = self.request.get_all('gender')

    if ad_duration_param:
      ad_duration = timedelta(seconds=int(ad_duration_param))
    else:
      ad_duration = None

    master_playlist_url = urlparse.urljoin(
        _HLS_MASTER_BASE_URI, channel_param + '/index.m3u8')
    master_playlist = m3u8.load(master_playlist_url)

    target_segment_name = None

    for playlist in master_playlist.playlists:
      if target_segment_name is None:
        media_playlist = m3u8.load(playlist.absolute_uri)

        target_media_sequence = (media_playlist.media_sequence +
                                 len(media_playlist.segments) +
                                 _TARGET_MEDIA_SEGMENT_OFFSET)
        target_segment_name = 'segm%06d.ts' % target_media_sequence

      cue_out_segment_url = urlparse.urljoin(
          playlist.absolute_uri, target_segment_name)

      # We let BroadcastWorker know about the new cue out point
      memcache.set(cue_out_segment_url, True, time=_TTL_1_HOUR)
      logging.info("Adding a cue out point to the segment %s"
                   % cue_out_segment_url)

      # If an ad is not choosen, one will be requested from DFP
      # in the HlsHandler for each user within a unique session
      if ad_filename_param:
        if ad_filename_param.endswith('.ts'):
          # TS files are pre-transcoded for each bandwidth available
          ads_bandwidth_dirname = str(playlist.stream_info.bandwidth)

          ad_filepath = os.path.join(
              _ADS_DIRNAME, ads_bandwidth_dirname, ad_filename_param)
        else:
          ad_filepath = os.path.join(_ADS_DIRNAME, ad_filename_param)

        for gender in gender_params:
          targeting_group = _TARGET_PREFIX_GENDER + gender

          target_segment_path = (
              targeting_group + '/' + target_segment_name)
          target_segment_url = urlparse.urljoin(
              playlist.absolute_uri, target_segment_path)

          # This is for the thread-based model which is going away soon
          memcache.set(target_segment_url, ad_filepath, time=_TTL_1_HOUR)

          # We let HlsHandler know about the ad we would like to inject
          memcache.set(cue_out_segment_url, (ad_filepath, ad_duration),
                       time=_TTL_1_HOUR, namespace=targeting_group)

          logging.info("Injecting '%s' starting with the segment %s"
                       % (ad_filepath, target_segment_url))

    self.redirect('/studio')

  def _update_query_string(self, uri, **kwargs):
    uri_parts = list(urlparse.urlparse(uri))

    uri_query = urlparse.parse_qs(uri_parts[4])
    uri_query.update(kwargs)

    uri_parts[4] = urllib.urlencode(uri_query)

    return urlparse.urlunparse(uri_parts)


class HomeHandler(webapp2.RequestHandler):

  @restricted
  def get(self):
    template = _JINJA_ENVIRONMENT.get_template(
        os.path.join(_TEMPLATES_DIRNAME, 'studio.html'))

    self.response.write(template.render())


app = webapp2.WSGIApplication([
  (r'/studio', HomeHandler),
  (r'/studio/broadcast', BroadcastHandler),
], debug=True)
