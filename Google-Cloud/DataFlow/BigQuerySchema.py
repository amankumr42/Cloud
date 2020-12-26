#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A workflow that writes to a BigQuery table with nested and repeated fields.

Demonstrates how to build a bigquery.TableSchema object with nested and repeated
fields. Also, shows how to generate data to be written to a BigQuery table with
nested and repeated fields.
"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam


def run(argv=None):
  """Run the workflow."""
  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--output',
      required=True,
      help=(
          'Output BigQuery table for results specified as: '
          'PROJECT:DATASET.TABLE or DATASET.TABLE.'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  with beam.Pipeline(argv=pipeline_args) as p:

    from apache_beam.io.gcp.internal.clients import bigquery  # pylint: disable=wrong-import-order, wrong-import-position

    table_schema = bigquery.TableSchema()

    host_schema =  bigquery.TableFieldSchema()
    host_schema.name = 'host'
    host_schema.type = 'string'
    host_schema.mode = 'nullable'
    table_schema.fields.append(host_schema)

    time_schema = bigquery.TableFieldSchema()
    time_schema.name = 'time'
    time_schema.type = 'string'
    time_schema.mode = 'nullable'
    table_schema.fields.append(time_schema)

    request_type_schema = bigquery.TableFieldSchema()
    request_type_schema.name = 'request'
    request_type_schema.type = 'string'
    request_type_schema.mode = 'nullable'
    table_schema.fields.append(request_type_schema)

    status_schema = bigquery.TableFieldSchema()
    status_schema.name = 'status'
    status_schema.type = 'string'
    status_schema.mode = 'nullable'
    table_schema.fields.append(status_schema)   

    size_schema = bigquery.TableFieldSchema()
    size_schema.name = 'size'
    size_schema.type = 'string'
    size_schema.mode = 'nullable'
    table_schema.fields.append(size_schema)

    def create_random_record(record_ids):
      return{
          'host' : '50.140.53.123',
          'time' : '25/Nov/2020:06:33:30',
          'request' : 'GET',
          'status' : '200',
          'size': '2024'
      }   

    # pylint: disable=expression-not-assigned
    record_ids = p | 'CreateIDs' >> beam.Create(['1', '2', '3', '4', '5'])
    records = record_ids | 'CreateRecords' >> beam.Map(create_random_record)
    records | 'write' >> beam.io.WriteToBigQuery(
        known_args.output,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

    # Run the pipeline (all operations are deferred until run() is called).
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
