__author__ = 'utku@hoydaa.com (Utku Utkan)'

import copy
import itertools
import logging
import math
import os
import posixpath
import random
import re
import shutil
import subprocess
import tempfile
import threading
import time
import urllib2
import urlparse

from collections import namedtuple
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

import cloudstorage as gcs
import m3u8
import webapp2

from lxml import etree

from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.api import urlfetch
from google.appengine.api import urlfetch_errors
from google.appengine.runtime import apiproxy_errors


# For the numeric values of all the logging levels, please visit
# https://docs.python.org/2/library/logging.html#logging-levels.
_LOG_LEVEL_WARNING = 30
_LOG_LEVEL_DEBUG = 10

_HLS_CHANNELS = os.getenv('HLS_CHANNELS').split(',')
_HLS_SOURCE_BASE_URL = os.getenv('HLS_SOURCE_BASE_URL').rstrip('/') + '/'
_HLS_MASTER_BASE_URL = os.getenv('HLS_MASTER_BASE_URL').rstrip('/') + '/'
_HLS_MEDIA_BASE_URL = os.getenv('HLS_MEDIA_BASE_URL').rstrip('/') + '/'
_HLS_MEDIA_BUCKET = os.getenv('HLS_MEDIA_BUCKET')

_TASK_ETA_HEADER = 'X-AppEngine-TaskETA'
_TASK_NAME_HEADER = 'X-AppEngine-TaskName'

_TASK_ETA_DELAY_WARNING_THRESHOLD = timedelta(seconds=1)
_TASK_EXEC_DELAY_WARNING_THRESHOLD = timedelta(seconds=1)

_TASKQUEUE_NAME = 'broadcast'
_TASKQUEUE_URL = '/worker'

_DONT_COPY_SEGMENTS = False
_DONT_INJECT_ADS = False

_LOOKAHEAD_DURATION = float(os.getenv('LOOKAHEAD_IN_SECS'))

_START_AD_ZERO_MILLIS_IN = Decimal('0.000000')
_START_AD_1500_MILLIS_IN = Decimal('1.500000')

_AD_TIMESTAMP_OFFSET = Decimal('1.400000')

_TTL_1_HOUR = 3600

_TARGETING_DIMENSIONS = [['gender_unknown', 'gender_male', 'gender_female']]

_RETRIABLE_EXCEPTIONS = (urlfetch.DownloadError,
                         urlfetch_errors.InternalTransientError,
                         apiproxy_errors.Error,
                         app_identity.InternalError,
                         app_identity.BackendDeadlineExceeded)


_media_playlist_copy_threads = []


MediaInfo = namedtuple('MediaInfo',
                       ['program_id', 'video_stream', 'audio_stream'])

StreamInfo = namedtuple('StreamInfo',
                        ['index', 'id', 'duration', 'avg_frame_rate', 'level',
                         'sample_aspect_ratio', 'display_aspect_ratio', 'scale',
                         'sample_rate', 'packets'])

PacketInfo = namedtuple('PacketInfo',
                        ['time', 'duration'])


def _get_stream_info(ffprobe_tree, codec_type):
  packet_elems = ffprobe_tree.xpath("//packet[@codec_type='%s']" % codec_type)
  packet_infos = []

  for packet_elem in packet_elems:
    packet_time = Decimal(packet_elem.get('pts_time'))
    packet_duration = Decimal(packet_elem.get('duration_time'))

    packet_infos.append(PacketInfo(time=packet_time, duration=packet_duration))

  stream_elem = ffprobe_tree.xpath("//stream[@codec_type='%s']" % codec_type)[0]

  stream_index = int(stream_elem.get('index'))
  stream_id = int(stream_elem.get('id'), base=0)
  duration = Decimal(stream_elem.get('duration'))
  avg_frame_rate = stream_elem.get('avg_frame_rate')

  if 'level' in stream_elem.keys():
    video_level = int(stream_elem.get('level'))
  else:
    video_level = None

  if 'sample_aspect_ratio' in stream_elem.keys():
    video_sample_aspect_ratio = (stream_elem.get('sample_aspect_ratio')
                                            .replace(':', '/'))
  else:
    video_sample_aspect_ratio = None

  if 'display_aspect_ratio' in stream_elem.keys():
    video_display_aspect_ratio = (stream_elem.get('display_aspect_ratio')
                                             .replace(':', '/'))
  else:
    video_display_aspect_ratio = None

  if set(['width', 'height']).issubset(stream_elem.keys()):
    video_scale = '%sx%s' % (stream_elem.get('width'),
                             stream_elem.get('height'))
  else:
    video_scale = None

  if 'sample_rate' in stream_elem.keys():
    audio_sample_rate = int(stream_elem.get('sample_rate'))
  else:
    audio_sample_rate = None

  return StreamInfo(index=stream_index, id=stream_id, duration=duration,
                    avg_frame_rate=avg_frame_rate, level=video_level,
                    sample_aspect_ratio=video_sample_aspect_ratio,
                    display_aspect_ratio=video_display_aspect_ratio,
                    scale=video_scale, sample_rate=audio_sample_rate,
                    packets=packet_infos)


def _get_media_info(media_filepath):
  p = subprocess.Popen(['ffprobe',
                        '-i', media_filepath,
                        '-show_programs',
                        '-show_packets',
                        '-print_format', 'xml'],
                       stdout=subprocess.PIPE)

  ffprobe_tree = etree.fromstring(p.communicate()[0])

  program_elem = ffprobe_tree.xpath("//program")[0]
  program_id = int(program_elem.get('program_id'))

  video_stream_info = _get_stream_info(ffprobe_tree, 'video')
  audio_stream_info = _get_stream_info(ffprobe_tree, 'audio')

  return MediaInfo(program_id=program_id,
                   video_stream=video_stream_info,
                   audio_stream=audio_stream_info)


def _find_ltrim_packet(media_filepath, codec_type, trim_time):
  p = subprocess.Popen(['ffprobe', '-i', media_filepath, '-show_packets',
                        '-print_format', 'xml'], stdout=subprocess.PIPE)

  ffprobe_xml_string, stderr = p.communicate()
  tree = etree.fromstring(ffprobe_xml_string)

  packet = tree.xpath("/ffprobe/packets/packet[@codec_type='%s' and "
                      "@pts_time>=%s]" % (codec_type, trim_time))[0]

  return Decimal(packet.get('pts_time')), Decimal(packet.get('duration_time'))


def _find_rtrim_packet(media_filepath, codec_type, trim_time):
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


def _base_uri(absolute_uri):
  parsed_url = urlparse.urlparse(absolute_uri)

  prefix = parsed_url.scheme + '://' + parsed_url.netloc
  base_path = posixpath.normpath(parsed_url.path + '/..')

  return urlparse.urljoin(prefix, base_path + '/')


def _load_media_playlist(media_playlist_url):
  for retry in range(0, 5):
    try:
      return m3u8.load(media_playlist_url)
    except _RETRIABLE_EXCEPTIONS:
      time.sleep((2 ** retry) + (random.randint(0, 1000) / 1000))
      logging.warning('Retrying to load: ' + media_playlist_url)
    except urllib2.HTTPError as e:
      if e.code == 404:
        time.sleep((2 ** retry) + (random.randint(0, 1000) / 1000))
        logging.warning('Retrying to load: ' + media_playlist_url)


def _load_playlist_m3u8(playlist_url):
  # We do not use m3u8.load() as it makes two
  # subsequent requests loading the playlists
  m3u8_response = urlfetch.fetch(playlist_url)

  playlist = m3u8.loads(m3u8_response.content)
  playlist.base_uri = _base_uri(playlist_url)

  return playlist

def _download_media_segment(media_segment_url, local_filepath):
  for retry in range(0, 5):
    try:
      response = urlfetch.fetch(media_segment_url, deadline=30)

      with open(local_filepath, 'w') as local_file:
        local_file.write(response.content); return

    except _RETRIABLE_EXCEPTIONS:
      time.sleep((2 ** retry) + (random.randint(0, 1000) / 1000))
      logging.warning('Retrying to download: ' + media_segment_url)


def _upload_media_segment(media_segment_filepath, gcs_path):
  gcs_filepath = os.path.join(os.sep, _HLS_MEDIA_BUCKET, gcs_path)

  with open(media_segment_filepath, 'r') as media_segment_file, \
       gcs.open(gcs_filepath, mode='w', content_type='video/mp2t',
                options={'x-goog-acl': 'public-read'}) as gcs_file:
    shutil.copyfileobj(media_segment_file, gcs_file)


def _upload_media_playlist(media_playlist, gcs_path):
  gcs_filepath = os.path.join(os.sep, _HLS_MEDIA_BUCKET, gcs_path)

  with gcs.open(gcs_filepath, mode='w', content_type='audio/mpegurl',
                options={'x-goog-acl': 'public-read',
                         'cache-control': 'no-cache'}) as gcs_file:
    gcs_file.write(media_playlist.dumps() + '\n')

  logging.info('Uploaded media playlist to GCS: ' + gcs_filepath)


def _transcode_media_file(input_filepath, output_filepath, template_filepath):
  media_info = _get_media_info(template_filepath)

  video_level = media_info.video_stream.level

  template_filestat = os.stat(template_filepath)
  template_filesize = template_filestat.st_size * 8
  video_duration = media_info.video_stream.duration
  video_bit_rate = int(template_filesize / video_duration)

  video_filter = 'scale=' + media_info.video_stream.scale
  video_filter += ',setsar=' + media_info.video_stream.sample_aspect_ratio
  video_filter += ',setdar=' + media_info.video_stream.display_aspect_ratio

  video_frame_rate = media_info.video_stream.avg_frame_rate

  audio_sample_rate = media_info.audio_stream.sample_rate

  subprocess.call(['ffmpeg',
                   '-i', input_filepath,
                   '-codec:v', 'libx264',
                   '-profile:v', 'baseline',
                   '-level', str(video_level),
                   '-b:v', str(video_bit_rate),
                   '-filter:v', video_filter,
                   '-r', str(video_frame_rate),
                   '-codec:a', 'libvo_aacenc',
                   '-ar', str(audio_sample_rate),
                   '-mpegts_copyts', '1',
                   '-f', 'mpegts',
                   '-copyts',
                   output_filepath, '-y'])


def _inject_ad(media_segment_filepath, ad_filepath, ad_start_time):
  temp_dirpath = tempfile.mkdtemp()

  try:
    ad_media_info = _get_media_info(ad_filepath)

    ad_video_packets = ad_media_info.video_stream.packets
    ad_audio_packets = ad_media_info.audio_stream.packets

    logging.debug('Ad video packets: ' + str(ad_video_packets))
    logging.debug('Ad audio packets: ' + str(ad_audio_packets))

    ad_media_start_time = min(ad_video_packets[0].time, ad_audio_packets[0].time)
    ad_video_audio_delay = ad_video_packets[0].time - ad_audio_packets[0].time

    ad_video_end_time = ad_video_packets[-1].time + ad_video_packets[-1].duration
    ad_video_duration = ad_video_end_time - ad_media_start_time

    ad_audio_end_time = ad_audio_packets[-1].time + ad_audio_packets[-1].duration
    ad_audio_duration = ad_audio_end_time - ad_media_start_time

    media_info = _get_media_info(media_segment_filepath)

    media_segment_video_packets = media_info.video_stream.packets
    media_segment_audio_packets = media_info.audio_stream.packets

    logging.debug('Media segment video packets: ' + str(media_segment_video_packets))
    logging.debug('Media segment audio packets: ' + str(media_segment_audio_packets))

    media_segment_audio_start_time = media_segment_audio_packets[0].time
    media_segment_audio_end_time = media_segment_audio_packets[-1].time + media_segment_audio_packets[-1].duration
    media_segment_audio_duration = media_segment_audio_end_time - media_segment_audio_start_time

    lsegm_trimmed_filepath = None
    tsegm_trimmed_filepath = None

    if ad_start_time > Decimal('0.000000'):
      cut_time = media_segment_audio_packets[0].time + ad_start_time

      audio_cut_time, _ = _find_ltrim_packet(media_segment_filepath, 'audio', cut_time)
      audio_ltrim_pkt = _find_rtrim_packet(media_segment_filepath, 'audio', audio_cut_time)
      logging.debug('Leading segment audio trim packet: ' + str(audio_ltrim_pkt))

      audio_ltrim_start_time = media_segment_audio_packets[0].time
      audio_ltrim_end_time = audio_ltrim_pkt[0] + audio_ltrim_pkt[1]
      audio_ltrim_duration = audio_ltrim_end_time - audio_ltrim_start_time

      video_cut_time = audio_cut_time + ad_video_audio_delay
      video_ltrim_pkt = _find_rtrim_packet(media_segment_filepath, 'video', video_cut_time)
      logging.debug('Leading segment video trim packet: ' + str(video_ltrim_pkt))

      video_ltrim_start_time = media_segment_video_packets[0].time
      video_ltrim_end_time = video_ltrim_pkt[0] + video_ltrim_pkt[1]
      video_ltrim_duration = video_ltrim_end_time - video_ltrim_start_time

      lsegm_video_audio_delay = media_segment_video_packets[0].time - media_segment_audio_packets[0].time
      logging.debug('lsegm_video_audio_delay: ' + str(lsegm_video_audio_delay))

      lsegm_raw_video_filepath = os.path.join(temp_dirpath, 'lsegm.h264')
      lsegm_trimmed_raw_video_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed.h264')
      lsegm_trimmed_video_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed_video.ts')
      lsegm_trimmed_raw_audio_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed.aac')
      lsegm_trimmed_audio_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed_audio.ts')
      lsegm_trimmed_filepath = os.path.join(temp_dirpath, 'lsegm_trimmed.ts')

      subprocess.call(['ffmpeg',
                       '-i', media_segment_filepath,
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
                       '-i', media_segment_filepath,
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

    if ad_audio_duration < media_segment_audio_duration:
      # audio_rtrim_cut_time = audio_cut_time + ad_audio_duration
      audio_rtrim_cut_time = media_segment_audio_packets[0].time + ad_audio_duration
      audio_rtrim_pkt = _find_ltrim_packet(media_segment_filepath, 'audio', audio_rtrim_cut_time) # IndexError: list index out of range
      logging.debug('Trailing segment audio trim packet: ' + str(audio_rtrim_pkt))

      audio_rtrim_start_time = media_segment_audio_packets[0].time
      audio_rtrim_end_time = audio_rtrim_pkt[0]
      audio_rtrim_duration = audio_rtrim_end_time - audio_rtrim_start_time

      # video_rtrim_cut_time = audio_cut_time + ad_video_duration
      video_rtrim_cut_time = media_segment_audio_packets[0].time + ad_video_duration
      video_rtrim_pkt = _find_ltrim_packet(media_segment_filepath, 'video', video_rtrim_cut_time)
      logging.debug('Trailing segment video trim packet: ' + str(video_rtrim_pkt))

      video_rtrim_start_time = media_segment_video_packets[0].time
      video_rtrim_end_time = video_rtrim_pkt[0]
      video_rtrim_duration = video_rtrim_end_time - video_rtrim_start_time

      tsegm_video_audio_delay = video_rtrim_end_time - audio_rtrim_end_time
      logging.debug('tsegm_video_audio_delay: ' + str(tsegm_video_audio_delay))

      tsegm_raw_video_filepath = os.path.join(temp_dirpath, 'tsegm.h264')
      tsegm_trimmed_raw_video_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed.h264')
      tsegm_trimmed_video_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed_video.ts')
      tsegm_trimmed_raw_audio_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed.aac')
      tsegm_trimmed_audio_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed_audio.ts')
      tsegm_trimmed_filepath = os.path.join(temp_dirpath, 'tsegm_trimmed.ts')

      subprocess.call(['ffmpeg',
                       '-i', media_segment_filepath,
                       '-codec:v', 'copy',
                       '-f', 'mpeg2video',
                       tsegm_raw_video_filepath])
      subprocess.call(['ffmpeg',
                       '-i', tsegm_raw_video_filepath,
                       '-ss', str(video_rtrim_duration),
                       '-codec:v', 'libx264',
                       '-profile:v', 'baseline',
                       '-level', '30',
                       '-b:v', '128k',
                       tsegm_trimmed_raw_video_filepath])
      subprocess.call(['ffmpeg',
                       '-i', tsegm_trimmed_raw_video_filepath,
                       '-codec:v', 'copy',
                       '-mpegts_copyts', '1',
                       '-f', 'mpegts',
                       '-copyts',
                       tsegm_trimmed_video_filepath])
      subprocess.call(['ffmpeg',
                       '-i', media_segment_filepath,
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

    if lsegm_trimmed_filepath is not None or tsegm_trimmed_filepath is not None:
      concat_filepath = os.path.join(temp_dirpath, 'concat.txt')

      with open(concat_filepath, 'w') as concat_file:
        if lsegm_trimmed_filepath is not None:
          concat_file.write("file '%s'\n" % lsegm_trimmed_filepath)
          concat_file.write("duration %f\n" % audio_ltrim_duration)

        concat_file.write("file '%s'\n" % ad_filepath)

        if tsegm_trimmed_filepath is not None:
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

    segment_end_time = media_segment_audio_packets[-1].time + media_segment_audio_packets[-1].duration
    segment_time = segment_end_time - media_segment_audio_packets[0].time

    logging.debug('Segment time: ' + str(segment_time))

    # segment_start_number = media_playlist.media_sequence + _AD_SEGMENT_OFFSET
    # initial_offset = lsegm_audio_packets[0][0] - _AD_SEGMENT_TIME_OFFSET

    segm_filepath_pattern = os.path.join(temp_dirpath, 'segm%d.ts')

    subprocess.call(['ffmpeg',
                     '-i', merged_filepath if lsegm_trimmed_filepath is not None or tsegm_trimmed_filepath is not None else ad_filepath,
                     '-codec', 'copy',
                     '-f', 'ssegment',
                     '-reference_stream', 'a:0',
                     '-segment_times', str(segment_time),
                     # '-segment_start_number', str(segment_start_number),
                     # '-initial_offset', str(initial_offset),
                     segm_filepath_pattern])

    logging.debug('Segmented the merged media file')

    output_ts_offset = media_segment_audio_packets[0].time - _AD_TIMESTAMP_OFFSET

    # segm_sequence = media_playlist.media_sequence + _AD_SEGMENT_OFFSET
    segm_sequence = 0

    segm_filename = 'segm%d.ts' % segm_sequence
    segm_filepath = os.path.join(temp_dirpath, segm_filename)

    video_streamid = '%d:%d' % (media_info.video_stream.index,
                                media_info.video_stream.id)
    audio_streamid = '%d:%d' % (media_info.audio_stream.index,
                                media_info.audio_stream.id)

    subprocess.call(['ffmpeg',
                     '-i', segm_filepath if tsegm_trimmed_filepath is None else merged_filepath,
                     '-codec', 'copy',
                     '-mpegts_service_id', str(media_info.program_id),
                     '-streamid', video_streamid,
                     '-streamid', audio_streamid,
                     '-output_ts_offset', str(output_ts_offset),
                     media_segment_filepath, '-y'])

    logging.debug('Fixed the media segment')

    if tsegm_trimmed_filepath is None:
      segm_filename = 'segm%d.ts' % (segm_sequence + 1)
      segm_filepath = os.path.join(temp_dirpath, segm_filename)

      shutil.copyfile(segm_filepath, ad_filepath)

      return media_segment_filepath, ad_filepath
    else:
      os.remove(ad_filepath)
      return media_segment_filepath, None

  finally:
    shutil.rmtree(temp_dirpath)


def _create_temp_filepath(*args, **kwargs):
  fd = None

  try:
    fd, filepath = tempfile.mkstemp(*args, **kwargs); return filepath
  finally:
    if fd is not None:
      os.close(fd)


def _copy_media_playlist(src_playlist, dst_playlist,
                         dst_playlist_base_uri,
                         copy_segments, inject_ads):
  temp_ad_filepath = None

  try:
    curr_media_seq = None
    dst_media_playlist = None

    while True:
      iter_start_time = time.time()

      # URLError: <urlopen error [Errno 101] Network is unreachable>
      src_media_playlist = _load_media_playlist(src_playlist.absolute_uri)

      if dst_media_playlist is None:
        dst_media_playlist = m3u8.loads(src_media_playlist.dumps())
        dst_media_playlist.media_sequence -= len(dst_media_playlist.segments)

      # AttributeError: 'NoneType' object has no attribute 'media_sequence' x 2
      src_media_seq = src_media_playlist.media_sequence

      if curr_media_seq is None or curr_media_seq < src_media_seq:
        curr_media_seq = src_media_seq

      for src_segm in src_media_playlist.segments[curr_media_seq-src_media_seq:]:
        dst_segm = copy.deepcopy(src_segm)
        dst_segm.base_uri = _base_uri(dst_playlist.absolute_uri)

        ad_filepath = memcache.get(dst_segm.absolute_uri)
        ad_start_time = _START_AD_ZERO_MILLIS_IN

        if ad_filepath is not None and inject_ads:
          temp_ad_filepath = _create_temp_filepath(suffix='.ts')
          shutil.copyfile(ad_filepath, temp_ad_filepath)

          ad_start_time = _START_AD_1500_MILLIS_IN

        if temp_ad_filepath is not None or copy_segments:
          with tempfile.NamedTemporaryFile(suffix='.ts') as temp_segm_file:
            _download_media_segment(src_segm.absolute_uri, temp_segm_file.name)

            if temp_ad_filepath is not None:
              _inject_ad(temp_segm_file.name, temp_ad_filepath, ad_start_time)

              if not os.path.isfile(temp_ad_filepath):
                temp_ad_filepath = None

            try:
              dst_segm_rel_uri = (
                  dst_segm.absolute_uri[len(dst_playlist_base_uri):])
              _upload_media_segment(temp_segm_file.name, dst_segm_rel_uri)
            except gcs.ServerError:
              logging.error('Cannot upload media segment to GCS: ' + dst_segm_rel_uri)
        else:
          dst_segm.uri = src_segm.absolute_uri

        dst_media_playlist.segments.pop(0)
        dst_media_playlist.add_segment(dst_segm)
        dst_media_playlist.media_sequence += 1

        curr_media_seq += 1

      try:
        dst_playlist_rel_uri = (
            dst_playlist.absolute_uri[len(dst_playlist_base_uri):])
        _upload_media_playlist(dst_media_playlist, dst_playlist_rel_uri)
      except gcs.ServerError:
        logging.error('Cannot upload media playlist to GCS: ' + dst_playlist_rel_uri)

      iter_time = time.time() - iter_start_time
      logging.debug('Iteration time is %s seconds' % iter_time)

      if iter_time < src_media_playlist.target_duration:
        sleep_time = src_media_playlist.target_duration - iter_time
        logging.debug('Sleeping for %d seconds...' % sleep_time)

        time.sleep(sleep_time)

  finally:
    if temp_ad_filepath is not None:
      os.remove(temp_ad_filepath)


def _copy_master_playlist(src_master_playlist_absolute_uri,
                          dst_master_playlist_absolute_uri,
                          dst_media_playlist_base_uri,
                          copy_segments=True, inject_ads=True):
  src_master_playlist = _load_media_playlist(src_master_playlist_absolute_uri)
  dst_master_playlist = _load_media_playlist(dst_master_playlist_absolute_uri)

  playlist_tuples = zip(src_master_playlist.playlists,
                        dst_master_playlist.playlists)

  for src_playlist, dst_playlist in playlist_tuples:
    target_args = (src_playlist, dst_playlist,
                   dst_media_playlist_base_uri,
                   copy_segments, inject_ads)

    thread = threading.Thread(target=_copy_media_playlist, args=target_args)
    thread.daemon = True
    thread.start()

    _media_playlist_copy_threads.append(thread)


def _new_playlist(source_playlist, media_sequence):
  new_playlist = m3u8.loads(source_playlist.dumps())
  new_playlist.media_sequence = media_sequence
  new_playlist.segments[:] = []

  return new_playlist


class StartHandler(webapp2.RequestHandler):

  def get(self):
    for channel in _HLS_CHANNELS:
      source_master_playlist_absolute_uri = urlparse.urljoin(
          _HLS_SOURCE_BASE_URL, channel + '/index.m3u8')
      mirror_master_playlist_absolute_uri = urlparse.urljoin(
          _HLS_MASTER_BASE_URL, channel + '/index.m3u8')

      _copy_master_playlist(source_master_playlist_absolute_uri,
                            mirror_master_playlist_absolute_uri,
                            _HLS_MEDIA_BASE_URL,
                            inject_ads=_DONT_INJECT_ADS)

      for targeting in itertools.product(*_TARGETING_DIMENSIONS):
        targeted_master_playlist_uri = (
            '%s/%s/index.m3u8' % (channel, '-'.join(targeting)))
        targeted_master_playlist_absolute_uri = urlparse.urljoin(
            _HLS_MASTER_BASE_URL, targeted_master_playlist_uri)

        _copy_master_playlist(mirror_master_playlist_absolute_uri,
                              targeted_master_playlist_absolute_uri,
                              _HLS_MEDIA_BASE_URL,
                              copy_segments=_DONT_COPY_SEGMENTS)


class HealthHandler(webapp2.RequestHandler):

  def get(self):
    self.response.headers['Content-Type'] = 'text/plain'

    for thread in _media_playlist_copy_threads:
      if not thread.isAlive():
        self.response.write('fail')
        self.abort(500)

        return

    self.response.write('ok')


class BroadcastWorker(webapp2.RequestHandler):

  def post(self):
    task_start_time = datetime.now()
    logging.debug("Task Start Time: %s" % task_start_time)

    task_eta_header = self.request.headers[_TASK_ETA_HEADER]
    task_eta = datetime.fromtimestamp(float(task_eta_header))
    logging.debug("Task ETA: %s" % task_eta)

    if task_start_time > task_eta:
      task_eta_delay = task_start_time - task_eta

      # Log a warning if the delay is more than the threshold
      if task_eta_delay >= _TASK_ETA_DELAY_WARNING_THRESHOLD:
        log_level = _LOG_LEVEL_WARNING
      else:
        log_level = _LOG_LEVEL_DEBUG

      logging.log(log_level, "Task ETA Delay: %s" % task_eta_delay)

    exec_time_param = self.request.get('exec_time', task_eta_header)
    task_exec_time = datetime.fromtimestamp(float(exec_time_param))
    logging.debug("Task Execution Time: %s" % task_exec_time)

    # Sleep if the task started before the execution time;
    # otherwise log the the delay in task execution time.
    if task_start_time < task_exec_time:
      sleep_time = task_exec_time - task_start_time
      logging.debug("Sleeping for %s" % sleep_time)
      time.sleep(sleep_time.total_seconds())
    else:
      task_exec_delay = task_start_time - task_exec_time

      # Log a warning if the delay is more than the threshold
      if task_exec_delay >= _TASK_EXEC_DELAY_WARNING_THRESHOLD:
        log_level = _LOG_LEVEL_WARNING
      else:
        log_level = _LOG_LEVEL_DEBUG

      logging.log(log_level, "Task Execution Delay: %s" % task_exec_delay)

    source_playlist_url = self.request.get('source_url')
    target_playlist_url = self.request.get('target_url')

    if not source_playlist_url:
      raise ValueError("Task param 'source_url' is required.")
    if not target_playlist_url:
      raise ValueError("Task param 'target_url' is required.")

    source_playlist = _load_playlist_m3u8(source_playlist_url)

    current_media_seq = self.request.get_range(
        'media_seq', default=source_playlist.media_sequence)

    if current_media_seq < source_playlist.media_sequence:
      logging.warning("Segments [%d-%d) could not be processed in time." %
                      (current_media_seq, source_playlist.media_sequence))
      current_media_seq = source_playlist.media_sequence

    next_media_seq = (source_playlist.media_sequence +
                      len(source_playlist.segments))
    segment_duration = source_playlist.target_duration

    if current_media_seq == next_media_seq:
      random_duration = random.randint(0, segment_duration)
      next_task_wait_time = timedelta(seconds=random_duration)
      next_task_exec_time = task_exec_time + next_task_wait_time

      logging.debug("No new segments to process, adding the next task to "
                    "the queue to be executed at %s." % next_task_exec_time)

      task_retries = self.request.get_range('retries', default=0)
      self._add_next_task(source_playlist_url, target_playlist_url,
                          current_media_seq, next_task_exec_time,
                          task_retries + 1)
    else:
      if current_media_seq > next_media_seq:
        logging.info("Media sequence was resetted to %d." %
                     source_playlist.media_sequence)
        current_media_seq = source_playlist.media_sequence

        target_playlist = _new_playlist(source_playlist, current_media_seq)
      else:
        try:
          target_playlist = _load_playlist_m3u8(target_playlist_url)

          if current_media_seq > (target_playlist.media_sequence +
                                  len(target_playlist.segments)):
            logging.warning("Segments (%d-%d) are skipped in target playlist." %
                            (target_playlist.media_sequence, current_media_seq))
            target_playlist = _new_playlist(source_playlist, current_media_seq)
        except urllib2.HTTPError as e:
          if e.code == 404:
            target_playlist = _new_playlist(source_playlist, current_media_seq)
          else:
            raise e

      source_segment_index = current_media_seq - source_playlist.media_sequence
      source_segment = source_playlist.segments[source_segment_index]

      target_segment = copy.deepcopy(source_segment)
      target_segment.base_uri = _base_uri(target_playlist_url)

      if memcache.get(target_segment.absolute_uri):
        # We let HlsHandler know about the cue out
        target_segment.cue_out = True

      with tempfile.NamedTemporaryFile() as temp_seg_file:
        _download_media_segment(source_segment.absolute_uri, temp_seg_file.name)
        seg_gcs_path = target_segment.absolute_uri[len(_HLS_MEDIA_BASE_URL):]
        _upload_media_segment(temp_seg_file.name, seg_gcs_path)

      target_playlist.add_segment(target_segment)

      if self._should_shift_playlist(source_playlist, target_playlist):
        target_playlist.segments.pop(0)
        target_playlist.media_sequence += 1

      playlist_gcs_path = target_playlist_url[len(_HLS_MEDIA_BASE_URL):]
      _upload_media_playlist(target_playlist, playlist_gcs_path)

      if source_segment_index < len(source_playlist.segments) - 1:
        logging.debug("There are segments to process, adding the next "
                      "task to the queue to be be executed immediately.")
        next_task_exec_time = datetime.now()
      else:
        next_task_wait_time = timedelta(seconds=segment_duration)
        next_task_exec_time = task_exec_time + next_task_wait_time
        logging.debug("Processed the last segment, adding the next task to "
                      "the queue to be executed at %s." % next_task_exec_time)

      task_finish_time = datetime.now()
      logging.debug("Task Finish Time: %s" % task_finish_time)

      task_exec_duration = task_finish_time - task_exec_time
      logging.debug("Task Execution Duration: %s" % task_exec_duration)

      task_duration = task_finish_time - task_start_time
      logging.debug("Task Duration: %s" % task_duration)

      self._add_next_task(source_playlist_url, target_playlist_url,
                          current_media_seq + 1, next_task_exec_time)

  def _add_next_task(self, source_playlist_url, target_playlist_url,
                     next_media_seq, next_task_exec_time, retries=0):
    target_playlist_path = target_playlist_url[len(_HLS_MEDIA_BASE_URL):]

    # Task name consist of the target playlist path, the media sequence to be
    # processed next, and the number of retries so far, which is necessary to
    # prevent a task name conflict when there are no new segments to process.
    task_name_parts = [target_playlist_path, str(next_media_seq), str(retries)]

    # A task name can only contain letters, numbers, underscores, and hyphens
    task_name = ('-'.join(task_name_parts).replace('/', '-').replace('.', '_')
                                          .replace('?', '-').replace('=', '-'))

    # Convert the given execution time as datetime to a timestamp with
    # microsecond precision, and then convert the timestamp to string
    # while maintaining its precision.
    next_task_exec_timestamp = (time.mktime(next_task_exec_time.timetuple()) +
                                next_task_exec_time.microsecond / 1000000.0)
    exec_time_param = "{:.6f}".format(next_task_exec_timestamp)

    try:
      taskqueue.add(queue_name=_TASKQUEUE_NAME, name=task_name, url=_TASKQUEUE_URL,
                    params={'source_url': source_playlist_url,
                            'target_url': target_playlist_url,
                            'media_seq': next_media_seq,
                            'exec_time': exec_time_param,
                            'retries': retries})
    except taskqueue.TaskAlreadyExistsError:
      logging.warning("Task '%s' is already in the queue." % task_name)
    except askqueue.TombstonedTaskError:
      logging.warning("Task '%s' has been executed before." % task_name)

  def _should_shift_playlist(self, source_playlist, target_playlist):
    lookahead_segment_count = int(math.ceil(
        _LOOKAHEAD_DURATION / source_playlist.target_duration))

    # In the target playlist, we should have the normal segments,
    # plus the lookahead segments used to detect cue out points.
    max_target_segment_count = (
        len(source_playlist.segments) + lookahead_segment_count)

    return len(target_playlist.segments) > max_target_segment_count


class InjectionWorker(webapp2.RequestHandler):

  def post(self):
    task_name = self.request.headers[_TASK_NAME_HEADER]

    playlist_url = self.request.get('playlist_url')
    ad_source = self.request.get('ad_source')
    ad_start_seq = self.request.get('ad_start_seq')
    ad_start_time = self.request.get('ad_start_time')

    if not playlist_url:
      raise ValueError("Task param 'playlist_url' is required.")
    if not ad_source:
      raise ValueError("Task param 'ad_source' is required.")
    if not ad_start_seq:
      raise ValueError("Task param 'ad_start_seq' is required.")
    if not ad_start_time:
      raise ValueError("Task param 'ad_start_time' is required.")

    curr_start_seq = int(ad_start_seq)
    curr_start_time = Decimal(ad_start_time)

    ad_segment_redirects = {}

    try:
      temp_ad_filepath = None

      while True:
        playlist = _load_playlist_m3u8(playlist_url)
        starting_segment_index = curr_start_seq - playlist.media_sequence

        for segment in playlist.segments[starting_segment_index:]:
          with tempfile.NamedTemporaryFile(suffix='.ts') as temp_segment_file:
            _download_media_segment(segment.absolute_uri, temp_segment_file.name)

            if temp_ad_filepath is None:
              temp_ad_filepath = _create_temp_filepath(suffix='.ts')

              with tempfile.NamedTemporaryFile() as raw_ad_file:
                if self._is_url(ad_source):
                  _download_media_segment(ad_source, raw_ad_file.name)

                  ad_filepath = raw_ad_file.name
                else:
                  ad_filepath = ad_source

                if ad_filepath.endswith('.ts'):
                  # TS files are pre-transcoded, so just copy it
                  shutil.copyfile(ad_filepath, temp_ad_filepath)
                else:
                  # All other types of media files must be transcoded
                  _transcode_media_file(ad_filepath, temp_ad_filepath,
                                        temp_segment_file.name)

            _inject_ad(temp_segment_file.name, temp_ad_filepath, curr_start_time)

            ad_segment_path = self._get_ad_segment_path(segment, task_name)
            _upload_media_segment(temp_segment_file.name, ad_segment_path)

            segment_path = self._get_segment_path(segment)
            ad_segment_url = self._get_segment_url(ad_segment_path)
            ad_segment_redirects[segment_path] = ad_segment_url

            # We let HlsHandler know about all ad segments injected so far
            memcache.set(task_name, ad_segment_redirects, time=_TTL_1_HOUR)

            if not os.path.isfile(temp_ad_filepath):
              temp_ad_filepath = None
              return

          curr_start_time = _START_AD_ZERO_MILLIS_IN
          curr_start_seq += 1

    finally:
      if temp_ad_filepath is not None:
        os.remove(temp_ad_filepath)

  def _is_url(self, uri):
    return re.match(r'https?://', uri) is not None

  def _get_ad_segment_path(self, segment, task_name):
    segment_path = self._get_segment_path(segment)

    # Task names are in the format of 'segment_path-ad_id-creative_id'
    task_name_segment_prefix = "%s-" % (segment_path.replace('/', '-')
                                                    .replace('.', '_'))

    # Ad segments are placed under the media playlist hierarchy,
    # which makes the segment prefix of the task name redundant
    task_dirname = task_name[len(task_name_segment_prefix):]

    parsed_segment_url = urlparse.urlparse(segment.absolute_uri)
    segment_dirpath = posixpath.dirname(parsed_segment_url.path)
    segment_basename = posixpath.basename(parsed_segment_url.path)

    return posixpath.join(segment_dirpath, task_dirname, segment_basename)

  def _get_segment_path(self, segment):
    return segment.absolute_uri[len(_HLS_MEDIA_BASE_URL):]

  def _get_segment_url(self, segment_path):
    return str(urlparse.urljoin(_HLS_MEDIA_BASE_URL, segment_path))


app = webapp2.WSGIApplication([
  # ('/_ah/start', StartHandler),
  # ('/_ah/health', HealthHandler),
  (_TASKQUEUE_URL, BroadcastWorker),
  ('/inject', InjectionWorker),
], debug=True)
