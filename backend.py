__author__ = 'utku@hoydaa.com (Utku Utkan)'

import jinja2
import logging
import m3u8
import os
import shutil
import subprocess
import tempfile
import threading
import webapp2

import cloudstorage as gcs

from decimal import Decimal
from lxml import etree
from subprocess import Popen
from subprocess import PIPE
from urlparse import urljoin

from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.api import urlfetch_errors
from google.appengine.api import users
from google.appengine.runtime import apiproxy_errors


_ADS_DIRNAME = 'ads'
_GCS_HLS_DIRNAME = 'hls'

_AD_SEGMENT_OFFSET = 7
_AD_SEGMENT_TIME_OFFSET = Decimal('1.400000')
_CUT_TIME_OFFSET = Decimal('1.500000')

_TTL_1_HOUR = 3600 # seconds

_RETRIABLE_EXCEPTIONS = (urlfetch.DownloadError,
                         urlfetch_errors.InternalTransientError,
                         apiproxy_errors.Error,
                         app_identity.InternalError,
                         app_identity.BackendDeadlineExceeded)

_WHITELISTED_NICKNAMES = os.getenv('WHITELISTED_NICKNAMES').split(',')

_JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)

_TEMPLATES_DIRNAME = 'templates'


def get_media_packets(media_filepath):
  p = subprocess.Popen(['ffprobe', '-i', media_filepath, '-show_packets',
                        '-print_format', 'xml'], stdout=subprocess.PIPE)

  ffprobe_xml_string, stderr = p.communicate()
  tree = etree.fromstring(ffprobe_xml_string)

  packet_function = lambda packet: (Decimal(packet.get('pts_time')),
                                    Decimal(packet.get('duration_time')))

  video_packets = tree.xpath("/ffprobe/packets/packet[@codec_type='video']")
  video_packets = map(packet_function, video_packets[::len(video_packets)-1])

  audio_packets = tree.xpath("/ffprobe/packets/packet[@codec_type='audio']")
  audio_packets = map(packet_function, audio_packets[::len(audio_packets)-1])

  return video_packets, audio_packets


def find_ltrim_packet(media_filepath, codec_type, trim_time):
  p = subprocess.Popen(['ffprobe', '-i', media_filepath, '-show_packets',
                        '-print_format', 'xml'], stdout=subprocess.PIPE)

  ffprobe_xml_string, stderr = p.communicate()
  tree = etree.fromstring(ffprobe_xml_string)

  packet = tree.xpath("/ffprobe/packets/packet[@codec_type='%s' and "
                      "@pts_time>=%s]" % (codec_type, trim_time))[0]

  return Decimal(packet.get('pts_time')), Decimal(packet.get('duration_time'))


def find_rtrim_packet(media_filepath, codec_type, trim_time):
  p = subprocess.Popen(['ffprobe', '-i', media_filepath, '-show_packets',
                        '-print_format', 'xml'], stdout=subprocess.PIPE)

  ffprobe_xml_string, stderr = p.communicate()
  tree = etree.fromstring(ffprobe_xml_string)

  packets = tree.xpath("/ffprobe/packets/packet[@codec_type='%s' and "
                       "@pts_time<%s]" % (codec_type, trim_time))

  for packet in reversed(packets):
    packet_time = Decimal(packet.get('pts_time'))
    packet_duration = Decimal(packet.get('duration_time'))

    if (packet_time + packet_duration) <= trim_time:
      return Decimal(packet_time), Decimal(packet_duration)

  return None


def download_media(media_url, media_filepath):
  for n in range(0, 5):
    try:
      response = urlfetch.fetch(media_url, deadline=30)

      with open(media_filepath, 'w') as media_file:
        media_file.write(response.content); return

    except _RETRIABLE_EXCEPTIONS:
      time.sleep((2 ** n) + (random.randint(0, 1000) / 1000))
      logging.warning('Retrying to download media: ' + media_url)


def broadcast(ad_name, channel_name, playlist):
  temp_dirpath = tempfile.mkdtemp()

  try:
    media_playlist = m3u8.load(playlist.absolute_uri)
    logging.debug('Loaded media playlist: ' + playlist.absolute_uri)

    playlist_dirname = os.path.dirname(playlist.uri)
    ad_filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                               _ADS_DIRNAME, playlist_dirname, ad_name + '.ts')

    ad_video_packets, ad_audio_packets = get_media_packets(ad_filepath)
    logging.debug('Ad video packets: ' + str(ad_video_packets))
    logging.debug('Ad audio packets: ' + str(ad_audio_packets))

    ad_video_duration = ad_video_packets[-1][0] + ad_video_packets[-1][1]
    ad_audio_duration = ad_audio_packets[-1][0] + ad_audio_packets[-1][1]
    ad_video_audio_delay = ad_video_packets[0][0] - ad_audio_packets[0][0]

    segm_packets = []
    segm_filepaths = []

    for idx, segm in enumerate(media_playlist.segments[_AD_SEGMENT_OFFSET:]):
      segm_sequence = (media_playlist.media_sequence + _AD_SEGMENT_OFFSET + idx)
      segm_filepath = os.path.join(temp_dirpath, 'segm%d.ts' % segm_sequence)

      download_media(segm.absolute_uri, segm_filepath)
      logging.debug('Downloaded media segment: ' + segm.absolute_uri)

      segm_packets.append(get_media_packets(segm_filepath))
      segm_filepaths.append(segm_filepath)

    lsegm_video_packets, lsegm_audio_packets = segm_packets[0]
    tsegm_video_packets, tsegm_audio_packets = segm_packets[-1]

    lsegm_filepath = segm_filepaths[0]
    tsegm_filepath = segm_filepaths[-1]

    cut_time = lsegm_audio_packets[0][0] + _CUT_TIME_OFFSET

    audio_cut_time, _ = find_ltrim_packet(lsegm_filepath, 'audio', cut_time)
    audio_ltrim_pkt = find_rtrim_packet(lsegm_filepath, 'audio', audio_cut_time)
    logging.debug('Leading segment audio trim packet: ' + str(audio_ltrim_pkt))

    audio_ltrim_start_time = lsegm_audio_packets[0][0]
    audio_ltrim_end_time = audio_ltrim_pkt[0] + audio_ltrim_pkt[1]
    audio_ltrim_duration = audio_ltrim_end_time - audio_ltrim_start_time

    video_cut_time = audio_cut_time + ad_video_audio_delay
    video_ltrim_pkt = find_rtrim_packet(lsegm_filepath, 'video', video_cut_time)
    logging.debug('Leading segment video trim packet: ' + str(video_ltrim_pkt))

    video_ltrim_start_time = lsegm_video_packets[0][0]
    video_ltrim_end_time = video_ltrim_pkt[0] + video_ltrim_pkt[1]
    video_ltrim_duration = video_ltrim_end_time - video_ltrim_start_time

    lsegm_video_audio_delay = lsegm_video_packets[0][0] - lsegm_audio_packets[0][0]

    lsegm_raw_video_filepath = os.path.join(temp_dirpath, 'lsegm.h264')
    lsegm_trimmed_raw_video_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed.h264')
    lsegm_trimmed_video_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed_video.ts')
    lsegm_trimmed_raw_audio_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed.aac')
    lsegm_trimmed_audio_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed_audio.ts')
    lsegm_trimmed_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed.ts')

    subprocess.call(['ffmpeg',
                     '-i', lsegm_filepath,
                     '-codec:v', 'copy',
                     '-f', 'mpeg2video',
                     lsegm_raw_video_filepath])
    subprocess.call(['ffmpeg',
                     '-i', lsegm_raw_video_filepath,
                     '-t', str(video_ltrim_duration),
                     '-codec:v', 'copy',
                     '-f', 'mpegts',
                     lsegm_trimmed_raw_video_filepath])
    subprocess.call(['ffmpeg',
                     '-i', lsegm_trimmed_raw_video_filepath,
                     '-codec:v', 'copy',
                     '-mpegts_copyts', '1',
                     '-f', 'mpegts',
                     '-copyts',
                     lsegm_trimmed_video_filepath])
    subprocess.call(['ffmpeg',
                     '-i', lsegm_filepath,
                     '-t', str(audio_ltrim_duration),
                     '-codec:a', 'copy',
                     '-f', 'mp2',
                     lsegm_trimmed_raw_audio_filepath])
    subprocess.call(['ffmpeg',
                     '-i', lsegm_trimmed_raw_audio_filepath,
                     '-codec:a', 'copy',
                     '-mpegts_copyts', '1',
                     '-f', 'mpegts',
                     '-copyts',
                     lsegm_trimmed_audio_filepath])
    subprocess.call(['ffmpeg',
                     '-itsoffset', str(lsegm_video_audio_delay),
                     '-i', lsegm_trimmed_video_filepath,
                     '-i', lsegm_trimmed_audio_filepath,
                     '-codec', 'copy',
                     lsegm_trimmed_filepath])

    logging.debug('Leading segment video/audio trim times: %f/%f' %
                  (video_ltrim_duration, audio_ltrim_duration))

    audio_rtrim_cut_time = audio_cut_time + ad_audio_duration
    audio_rtrim_pkt = find_ltrim_packet(tsegm_filepath, 'audio', audio_rtrim_cut_time)
    logging.debug('Trailing segment audio trim packet: ' + str(audio_rtrim_pkt))

    audio_rtrim_start_time = tsegm_audio_packets[0][0]
    audio_rtrim_end_time = audio_rtrim_pkt[0]
    audio_rtrim_duration = audio_rtrim_end_time - audio_rtrim_start_time

    video_rtrim_cut_time = audio_cut_time + ad_video_duration
    video_rtrim_pkt = find_ltrim_packet(tsegm_filepath, 'video', video_rtrim_cut_time)
    logging.debug('Trailing segment video trim packet: ' + str(video_rtrim_pkt))

    video_rtrim_start_time = tsegm_video_packets[0][0]
    video_rtrim_end_time = video_rtrim_pkt[0]
    video_rtrim_duration = video_rtrim_end_time - video_rtrim_start_time

    tsegm_video_audio_delay = video_rtrim_end_time - audio_rtrim_end_time

    tsegm_raw_video_filepath = os.path.join(temp_dirpath, 'tsegm.h264')
    tsegm_trimmed_raw_video_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed.h264')
    tsegm_trimmed_video_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed_video.ts')
    tsegm_trimmed_raw_audio_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed.aac')
    tsegm_trimmed_audio_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed_audio.ts')
    tsegm_trimmed_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed.ts')

    subprocess.call(['ffmpeg',
                     '-i', tsegm_filepath,
                     '-codec:v', 'copy',
                     '-f', 'mpeg2video',
                     tsegm_raw_video_filepath])
    subprocess.call(['ffmpeg',
                     '-i', tsegm_raw_video_filepath,
                     '-ss', str(video_rtrim_duration),
                     '-codec:v', 'libx264',
                     '-profile:v', 'baseline',
                     '-level', '30',
                     '-b:v', playlist_dirname + 'k',
                     tsegm_trimmed_raw_video_filepath])
    subprocess.call(['ffmpeg',
                     '-i', tsegm_trimmed_raw_video_filepath,
                     '-codec:v', 'copy',
                     '-mpegts_copyts', '1',
                     '-f', 'mpegts',
                     '-copyts',
                     tsegm_trimmed_video_filepath])
    subprocess.call(['ffmpeg',
                     '-i', tsegm_filepath,
                     '-ss', str(audio_rtrim_duration),
                     '-codec:a', 'copy',
                     '-f', 'mp2',
                     tsegm_trimmed_raw_audio_filepath])
    subprocess.call(['ffmpeg',
                     '-i', tsegm_trimmed_raw_audio_filepath,
                     '-codec:a', 'copy',
                     '-mpegts_copyts', '1',
                     '-f', 'mpegts',
                     '-copyts',
                     tsegm_trimmed_audio_filepath])
    subprocess.call(['ffmpeg',
                     '-itsoffset', str(tsegm_video_audio_delay),
                     '-i', tsegm_trimmed_video_filepath,
                     '-i', tsegm_trimmed_audio_filepath,
                     '-codec', 'copy',
                     tsegm_trimmed_filepath])

    logging.debug('Trailing segment video/audio seek times: %f/%f' %
                  (video_rtrim_duration, audio_rtrim_duration))

    concat_filepath = os.path.join(temp_dirpath, 'concat.txt')

    with open(concat_filepath, 'w') as concat_file:
      concat_file.write("file '%s'\n" % lsegm_trimmed_filepath)
      concat_file.write("duration %f\n" % audio_ltrim_duration)

      concat_file.write("file '%s'\n" % ad_filepath)

      if tsegm_video_audio_delay < 0:
        concat_file.write("duration %f\n" % ad_video_duration)
      else:
        concat_file.write("duration %f\n" % ad_audio_duration)

      concat_file.write("file '%s'\n" % tsegm_trimmed_filepath)

    merged_filepath = os.path.join(temp_dirpath, 'merged.ts')

    subprocess.call(['ffmpeg',
                     '-f', 'concat',
                     '-i', concat_filepath,
                     '-codec', 'copy',
                     '-mpegts_copyts', '1',
                     '-f', 'mpegts',
                     merged_filepath])

    segment_times = []

    for _, audio_packets in segm_packets[:-1]:
      segment_end_time = audio_packets[-1][0] + audio_packets[-1][1]
      segment_times.append(segment_end_time - lsegm_audio_packets[0][0])

    logging.debug('Segment times: ' + str(segment_times))

    segment_start_number = media_playlist.media_sequence + _AD_SEGMENT_OFFSET
    initial_offset = lsegm_audio_packets[0][0] - _AD_SEGMENT_TIME_OFFSET

    segm_filepath_pattern = os.path.join(temp_dirpath, 'segm%d.ts')

    subprocess.call(['ffmpeg',
                     '-i', merged_filepath,
                     '-codec', 'copy',
                     '-f', 'ssegment',
                     '-reference_stream', 'a:0',
                     '-segment_times', ','.join(map(str, segment_times)),
                     '-segment_start_number', str(segment_start_number),
                     '-initial_offset', str(initial_offset),
                     segm_filepath_pattern])

    logging.debug('Segmented the merged media file')

    for idx, segm in enumerate(media_playlist.segments[_AD_SEGMENT_OFFSET:]):
      _, segm_audio_packets = segm_packets[idx]
      output_ts_offset = segm_audio_packets[0][0] - _AD_SEGMENT_TIME_OFFSET

      segm_sequence = (media_playlist.media_sequence + _AD_SEGMENT_OFFSET + idx)

      segm_filename = 'segm%d.ts' % segm_sequence
      segm_filepath = os.path.join(temp_dirpath, segm_filename)

      fixed_segm_filename = 'segm%d_fixed.ts' % segm_sequence
      fixed_segm_filepath = os.path.join(temp_dirpath, fixed_segm_filename)

      subprocess.call(['ffmpeg',
                       '-i', segm_filepath,
                       '-codec', 'copy',
                       '-mpegts_service_id', '2',
                       '-streamid', '0:102',
                       '-streamid', '1:202',
                       '-output_ts_offset', str(output_ts_offset),
                       fixed_segm_filepath])

      logging.debug('Fixed the media segment')

      gcs_segm_filepath = os.path.join(
          os.getenv('GCS_BUCKET_PATH'), _GCS_HLS_DIRNAME,
          channel_name, playlist_dirname, segm_filename)

      with open(fixed_segm_filepath, 'r') as fixed_segm_file, \
           gcs.open(gcs_segm_filepath, 'w', content_type='video/mp2t',
                    options={'x-goog-acl': 'public-read'}) as gcs_segm_file:
        shutil.copyfileobj(fixed_segm_file, gcs_segm_file)
        logging.info('Copied media segment to GCS: ' + gcs_segm_filepath)

      original_segm_url = urljoin(playlist.absolute_uri, segm_filename)
      gcs_segm_url = os.getenv('GCS_BASE_URL') + gcs_segm_filepath

      memcache.set(original_segm_url, gcs_segm_url, time=_TTL_1_HOUR)
      logging.info('Created a new media segment redirect: %s -> %s' %
                   (original_segm_url, gcs_segm_url))

  finally:
    shutil.rmtree(temp_dirpath)


def restricted(handler_method):

  def check_user(self, *args, **kwargs):
    if users.is_current_user_admin():
      return handler_method(self, *args, **kwargs)

    user = users.get_current_user()

    if user and user.nickname() in _WHITELISTED_NICKNAMES:
      return handler_method(self, *args, **kwargs)

    self.abort(403)

  return check_user


class StartHandler(webapp2.RequestHandler):

  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write('ok')


class BroadcastHandler(webapp2.RequestHandler):

  @restricted
  def post(self):
    ad_name = self.request.get('ad')
    channel_name = self.request.get('channel')

    master_playlist_url = urljoin(os.getenv('HLS_BASE_URL') + '/',
                                  '%s/index.m3u8' % channel_name)
    master_playlist = m3u8.load(master_playlist_url)

    for playlist in master_playlist.playlists:
      broadcast_args = ad_name, channel_name, playlist

      thread = threading.Thread(target=broadcast, args=broadcast_args)
      thread.daemon = True
      thread.start()

    self.redirect('/dashboard')


class DashboardHandler(webapp2.RequestHandler):

  @restricted
  def get(self):
    template = _JINJA_ENVIRONMENT.get_template(
        os.path.join(_TEMPLATES_DIRNAME, 'dashboard.html'))

    self.response.write(template.render())


app = webapp2.WSGIApplication([
  ('/_ah/start', StartHandler),
  ('/broadcast', BroadcastHandler),
  ('/dashboard', DashboardHandler),
], debug=True)
