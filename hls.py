__author__ = 'utku@hoydaa.com (Utku Utkan)'

import Cookie
import decimal
import logging
import math
import os
import posixpath
import re
import time
import urllib
import urlparse

import m3u8
import webapp2

from datetime import datetime
from datetime import timedelta

from lxml import etree

from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.api import urlfetch


_CORS_ORIGIN_HEADER = 'Access-Control-Allow-Origin'

_HLS_MASTER_BASE_URL = os.getenv('HLS_MASTER_BASE_URI').rstrip('/') + '/'
_HLS_MEDIA_BASE_URL = os.getenv('HLS_MEDIA_BASE_URL').rstrip('/') + '/'
_HLS_PLAYLIST_EXTENSION = '.m3u8'
_HLS_PLAYLIST_INDEX_FILE = 'index.m3u8'
_HLS_EXT_CUE_OUT = '#EXT-X-CUE-OUT-CONT'
_HLS_EXT_VAST_URL = '#EXT-X-VAST-URL'

_LOOKAHEAD_DURATION = float(os.getenv('LOOKAHEAD_IN_SECS'))

_DFP_BASE_URL = 'http://pubads.g.doubleclick.net/gampad/ads'
_DFP_NETWORK_ID = os.getenv('DFP_NETWORK_ID')
_DFP_AD_SIZE = '640x480'
_DFP_COOKIES = ['test_cookie', 'id', 'IDE']

_XPATH_VAST_AD_IDS = "//Ad/@id"
_XPATH_VAST_CREATIVE_IDS = "//Creative/@id"
_XPATH_VAST_CREATIVE_DURATIONS = "//Creative//Duration/text()"
_XPATH_VAST_MP4_URLS = "//MediaFile[@type='video/mp4']/text()"

_AD_FILENAME_REGEX = re.compile(r'ads/\d+/(.+)')

_TTL_1_HOUR = 3600

_TASKQUEUE_NAME = 'injection'
_TASKQUEUE_URL = '/inject'

_AD_START_TIME = decimal.Decimal('1.500000')

_VAST_CACHE_KEY = 'vast:%s'


class AdInfo(object):

  def __init__(self, source, duration, vast=None):
    self.source = source
    self.duration = duration
    self.vast = vast


class HlsHandler(webapp2.RequestHandler):

  def get(self, path):
    user_id = self.request.get('user_id')

    if not user_id:
      raise ValueError("Request param 'user_id' is required.")

    # For ease of testing we let the world access it
    self.response.headers[_CORS_ORIGIN_HEADER] = '*'

    media_url = urlparse.urljoin(_HLS_MEDIA_BASE_URL, path)

    if path.endswith(_HLS_PLAYLIST_EXTENSION):
      playlist_m3u8 = m3u8.load(media_url)

      if playlist_m3u8.is_variant:
        self.response.out.write(playlist_m3u8.dumps())
      else:
        # Process lookahead segments in the consumed playlist and return
        # the ad info if one was choosen for the current user in session.
        ad_info = self._process_lookahead_segments(
            media_url, user_id, playlist=playlist_m3u8)
        logging.debug("Processed lookahead segments in %s." % media_url)

        # If an ad is returned for the consumed playlist, we also
        # process lookahead segments in the alternative playlists.
        if ad_info is not None:
          master_playlist = m3u8.load(
              self._get_master_playlist_url(media_url))

          for playlist in master_playlist.playlists:
            # Current playlist is already processed
            if playlist.absolute_uri != media_url:
              self._process_lookahead_segments(
                  playlist.absolute_uri, user_id, ad_info=ad_info)
              logging.debug("Processed lookahead segments in %s." %
                            playlist.absolute_uri)

        # We pass user ID down to the segments
        for segment in playlist_m3u8.segments:
          segment.uri = self._update_query_string(segment.uri, user_id=user_id)

        # After we update all the segment URIs
        playlist_content = playlist_m3u8.dumps()

        for segment in playlist_m3u8.segments:
          if segment.cue_out:
            segment_path = self._get_segment_path(segment)

            # Get the task name from a previous HlsHandler request
            task_name = memcache.get(segment_path, namespace=user_id)

            if task_name is not None:
              hls_cue_out_start = playlist_content.find(_HLS_EXT_CUE_OUT + "\n")
              hls_cue_out_end = hls_cue_out_start + len(_HLS_EXT_CUE_OUT + "\n")

              vast_url = self.uri_for('vast', task_name=task_name, _full=True)

              # VAST URL tag with the format of '#EXT-X-VAST-URL:<vast_url>'
              hls_ext_vast_url_tag = "%s:%s" % (_HLS_EXT_VAST_URL, vast_url)

              # M3U8 Python library has no support for our custom tag,
              # and it also doesn't let us add comments. So we instead
              # do a string manipulation to populate the VAST URL tag.
              playlist_content = (playlist_content[:hls_cue_out_end] +
                                  hls_ext_vast_url_tag + "\n" +
                                  playlist_content[hls_cue_out_end:])

        self.response.out.write(playlist_content)
    else:
      # Get the task name from a previous HlsHandler request
      task_name = memcache.get(path, namespace=user_id)

      if task_name is not None:
        # Get the injected ad segment URL from InjectionWorker if
        # available, otherwise redirect to the original media URL
        redirect_dict = memcache.get(task_name) or {}
        redirect_url = redirect_dict.get(path, media_url)
      else:
        # We redirect to the original media URL (no ad injection)
        redirect_url = media_url

      self.redirect(redirect_url)

  def _process_lookahead_segments(self, playlist_url, user_id,
                                  playlist=None, ad_info=None):
    if playlist is None:
      playlist = m3u8.load(playlist_url)

    # Index of the first and the oldest lookahead segment
    lookahead_start = self._find_lookahead_start(playlist)

    # Iterate until there are no lookahead segments
    while lookahead_start < len(playlist.segments):
      # Process the lookahead segments backwards
      segment_index = len(playlist.segments) - 1
      segment = playlist.segments[segment_index]

      if segment.cue_out:
        ad_start_path = self._get_segment_path(segment)

        # Get the task name from a previous HlsHandler request
        task_name = memcache.get(ad_start_path, namespace=user_id)

        # Skip if the task is already added to the queue
        if task_name is None:
          if ad_info is None:
            # Get the ad source and the ad duration pair from the Studio UI
            ad_info_args = memcache.get(segment.absolute_uri, namespace=user_id)

            if ad_info_args is not None:
              ad_info = AdInfo(*ad_info_args)

          # Request ad from DFP if one is not specified
          if ad_info is None:
            dfp_response = self._request_ad_from_dfp(
                self._get_master_playlist_path(playlist_url))

            self._copy_cookies(dfp_response, self.response)
            ad_elem = etree.fromstring(dfp_response.content)

            if ad_elem is not None:
              ad_info = self._get_ad_info(ad_elem)

          # DFP may also return no ads, so we have to check for it
          if ad_info is not None:
            task_name = self._generate_task_name(segment, ad_info)
            ad_start_seq = playlist.media_sequence + segment_index

            ad_segment_path = ad_start_path
            ad_segment_seq = ad_start_seq

            while ad_info.duration > timedelta(seconds=0):
              # Pass the task name to the subsequent HlsHandler requests
              memcache.set(ad_segment_path, task_name, time=_TTL_1_HOUR,
                           namespace=user_id)

              ad_segment_path = ad_segment_path.replace(
                  str(ad_segment_seq), str(ad_segment_seq + 1))
              ad_segment_seq += 1

              ad_info.duration -= timedelta(seconds=playlist.target_duration)

            try:
              taskqueue.add(queue_name=_TASKQUEUE_NAME,
                            name=task_name,
                            url=_TASKQUEUE_URL,
                            params={'playlist_url': playlist_url,
                                    'ad_source': ad_info.source,
                                    'ad_start_seq': ad_start_seq,
                                    'ad_start_time': _AD_START_TIME})
            except (taskqueue.TaskAlreadyExistsError,
                    taskqueue.TombstonedTaskError) as e:
              logging.debug("Task '%s' already queued." % task_name)
            finally:
              if ad_info.vast is not None:
                vast_cache_key = _VAST_CACHE_KEY % task_name
                vast_content = etree.tostring(ad_info.vast)

                memcache.set(vast_cache_key, vast_content, time=_TTL_1_HOUR)

      # Drop the last segment
      playlist.segments.pop()

    return ad_info

  def _find_lookahead_start(self, playlist):
    lookahead_segment_count = int(math.ceil(
        _LOOKAHEAD_DURATION / playlist.target_duration))

    # Lookahead segments are conceptually the trailing ones
    return len(playlist.segments) - lookahead_segment_count

  def _request_ad_from_dfp(self, master_playlist_path):
    ad_unit_name = (master_playlist_path.replace('/', '-')
                                        .replace('.', '_'))
    ad_unit = '/%s/%s' % (_DFP_NETWORK_ID, ad_unit_name)
    now = int(time.time())

    vast_url = self._update_query_string(_DFP_BASE_URL, env='vp', gdfp_req='1',
                                         impl='s', output='vast', iu=ad_unit,
                                         sz=_DFP_AD_SIZE, correlator=str(now))
    logging.debug("DFP request URL: " + vast_url)

    all_cookies = self.request.cookies.items()
    dfp_cookies = {k: v for k, v in all_cookies if k in _DFP_COOKIES}

    headers = {}

    if dfp_cookies:
      cookie = Cookie.SimpleCookie()

      for key, value in dfp_cookies.items():
        cookie[str(key)] = value

      # We truncate the multi line header to a single line ';' delimited values
      headers['cookie'] = cookie.output(header='').strip().replace('\r\n', ';')
      logging.debug("DFP request cookie: " + headers['cookie'])

    dfp_response = urlfetch.fetch(vast_url, headers=headers)
    logging.debug("DFP response cookie: " + dfp_response.headers['set-cookie'])

    return dfp_response

  def _copy_cookies(self, src_response, dst_response):
    if 'set-cookie' in src_response.headers:
      cookie = Cookie.SimpleCookie()
      cookie.load(src_response.headers['set-cookie'])

      for cookie_item in cookie.values():
        # We take ownership of the cookie by overriding its domain
        parsed_host_url = urlparse.urlparse(self.request.host_url)
        cookie_item['domain'] = parsed_host_url.hostname

        cookie_header = cookie_item.output(header='').strip()
        dst_response.headers.add('set-cookie', cookie_header)

  def _update_query_string(self, uri, **kwargs):
    uri_parts = list(urlparse.urlparse(uri))

    uri_query = urlparse.parse_qs(uri_parts[4])
    uri_query.update(kwargs)

    uri_parts[4] = urllib.urlencode(uri_query)

    return urlparse.urlunparse(uri_parts)

  def _get_ad_info(self, ad_elem):
    mp4_urls = ad_elem.xpath(_XPATH_VAST_MP4_URLS)
    ad_source = mp4_urls[0] if mp4_urls else None

    creative_durations = ad_elem.xpath(_XPATH_VAST_CREATIVE_DURATIONS)

    if creative_durations:
      # Durations in the VAST are expected to be in the format of 'hh:mm:ss'
      ad_duration_time = datetime.strptime(creative_durations[0], "%H:%M:%S")
      ad_duration = timedelta(hours=ad_duration_time.hour,
                              minutes=ad_duration_time.minute,
                              seconds=ad_duration_time.second)
    else:
      ad_duration = None

    return AdInfo(ad_source, ad_duration, ad_elem)

  def _generate_task_name(self, segment, ad_info):
    task_name_parts = [self._get_segment_path(segment)]

    if ad_info.vast is not None:
      task_name_parts.append(ad_info.vast.xpath(_XPATH_VAST_AD_IDS)[0])
      task_name_parts.append(ad_info.vast.xpath(_XPATH_VAST_CREATIVE_IDS)[0])
    elif ad_info.source is not None:
      ad_filename_match = _AD_FILENAME_REGEX.match(ad_info.source)

      if ad_filename_match is not None:
        task_name_parts.append(ad_filename_match.group(1))
      else:
        raise ValueError("Ad filename cannot be recognized: " + ad_info.source)
    else:
      raise ValueError("Either vast or source must be specified.")

    # A task name can contain letters, numbers, underscores, and hyphens
    return ('-'.join(task_name_parts).replace('/', '-').replace('.', '_')
                                     .replace('?', '-').replace('=', '-'))

  def _get_master_playlist_url(self, playlist_url):
    master_playlist_path = self._get_master_playlist_path(playlist_url)
    return urlparse.urljoin(_HLS_MASTER_BASE_URL, master_playlist_path)

  def _get_master_playlist_path(self, playlist_url):
    playlist_path = playlist_url[len(_HLS_MEDIA_BASE_URL):]
    playlist_dirname = posixpath.dirname(playlist_path)

    # Master playlist is the index.m3u8 one level above given playlist
    return urlparse.urljoin(playlist_dirname, _HLS_PLAYLIST_INDEX_FILE)

  def _get_segment_path(self, segment):
    raw_segment_path = segment.absolute_uri[len(_HLS_MEDIA_BASE_URL):]
    parsed_raw_segment_path = urlparse.urlparse(raw_segment_path)

    # We exclude the query string part
    return parsed_raw_segment_path.path


class VastHandler(webapp2.RequestHandler):

  def get(self, task_name):
    vast_content = memcache.get(_VAST_CACHE_KEY % task_name)

    # For ease of testing we let the world access it
    self.response.headers[_CORS_ORIGIN_HEADER] = '*'

    if vast_content is not None:
      self.response.headers['Content-Type'] = 'text/xml'
      self.response.out.write(vast_content)
    else:
      self.abort(404)


app = webapp2.WSGIApplication([
  webapp2.Route('/hls/<path:.+>', handler=HlsHandler, name='hls'),
  webapp2.Route('/vast/<task_name:[\w-]+>', handler=VastHandler, name='vast'),
])
