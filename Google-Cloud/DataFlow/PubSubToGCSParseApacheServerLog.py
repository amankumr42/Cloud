# -*- coding: utf-8 -*-
"""PubSubToGCSParseApacheServerLog.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/13d3qDKPkCFfiYMp7fBFKcKdtLlwyC6j6
"""

#! pip install apache_beam[gcp]

import argparse
import logging
import datetime
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import apache_beam.transforms.window as window


class GroupWindowsIntoBatches(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """

    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message based on its
            # publish timestamp.
            | "Window into Fixed Intervals"
            >> beam.WindowInto(window.FixedWindows(self.window_size))
            # Use a dummy key to group the elements in the same window.
            # Note that all the elements in one window must fit into memory
            # for this. If the windowed elements do not fit into memory,
            # please consider using `beam.util.BatchElements`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements
            | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
            | "Groupby" >> beam.GroupByKey()
            | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
        )

class ParseApacheServerLog(beam.DoFn):
  @classmethod
  def process(self, element):
    HOST = r'^(?P<host>.*?)'
    SPACE = r'\s'
    IDENTITY = r'\S+'
    USER = r'\S+'
    TIME = r'(?P<time>\[.*?\])'
    REQUEST = r'\"(?P<request>.*?)\"'
    STATUS = r'(?P<status>\d{3})'
    SIZE = r'(?P<size>\S+)'
    REGEX = HOST+SPACE+IDENTITY+SPACE+USER+SPACE+TIME+SPACE+REQUEST+SPACE+STATUS+SPACE+SIZE+SPACE
    match = re.search(REGEX, str(element))
    return{
        "host": match.group('host'),
        "time": match.group('time'),
        "request" : match.group('request'),
        "status": match.group('status'),
        "size" : match.group('time')
    }
    '''return "{host},{time},{request},{status},{size}".format(host =match.group('host'), time = match.group('time'), 
      request= match.group('request') , status = match.group('status'), size = match.group('size'))'''

class WriteBatchesToGCS(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, batch, window=beam.DoFn.WindowParam):
        """Write one batch per file to a Google Cloud Storage bucket. """

        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        filename = "-".join([self.output_path, window_start, window_end])

        with beam.io.gcp.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for element in batch:
                f.write("{}\n".format(json.dumps(element)).encode("utf-8"))

def run (input_topic, output_path, window_size=1.0, pipeline_args=None):
  pipeline_options = PipelineOptions(
      pipeline_args, streaming=True, save_main_session=True
  )
  with beam.Pipeline(options=pipeline_options) as p:
    parsing_webserver_logs = (
        p 
        | "read data from pubsub" >> beam.io.ReadFromPubSub(topic=input_topic).with_output_types(bytes)
        | "lines" >> beam.Map(lambda x : x.decode("utf-8"))
        | "group messages to window interval" >> GroupWindowsIntoBatches(window_size)
        | "Parse server log data" >> beam.ParDo(ParseApacheServerLog())
        | "Write to GCS" >> beam.ParDo(WriteBatchesToGCS(output_path))
    )

if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  parser = argparse.ArgumentParser()
  parser.add_argument(
      "--input_topic",
      required = True,
      help = (
          "The cloud pubsub topic, to read data"
          "projects/<PROJECT_NAME>/topics/<TOPIC_NAME>"
      )
  )
  parser.add_argument(
      "--output_path",
      required = True,
      help = (
          "Output Path for GCS"
      )
  )
  parser.add_argument(
      "--window_size",
      type = float,
      default = 1.0,
      help = (
          "Output file's window size in number of minutes."
      )
  )
  known_args, pipeline_args = parser.parse_known_args()
  run (
      known_args.input_topic,
      known_args.output_path,
      known_args.window_size,
      pipeline_args,
  )