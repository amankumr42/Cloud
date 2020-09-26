import os
package = 'apache-beam[gcp]'
try:
  __import__(package)
except ImportError:
  os.system('pip install ' + str(package))

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse

parser = argparse.ArgumentParser()
parser.add_argument ('--input', default = 'gs://amank-dataflow/input-dir/dataflowwithbeam.py' , dest = 'input', required=False, help = 'Input File to process')
parser.add_argument('--output', default = 'gs://amank-dataflow/output-dir/', dest = 'output', required=False, help = 'Output file to write results to Cloud Storage')

path_args, pipeline_args = parser.parse_known_args()

input_pattern = path_args.input
output_pattern = path_args.output

options = PipelineOptions(pipeline_args)

p = beam.Pipeline(options=options)

#Implement word count in beam
word_count_logic = (
    p
    | 'Read lines' >> beam.io.ReadFromText(input_pattern)
    | 'Split Word' >> beam.FlatMap(lambda line : line.split(" "))
    | 'Get word and value' >> beam.Map(lambda word : (word, 1) )
    | 'Group and Sum ' >> beam.CombinePerKey(sum)
    | 'Write Result' >> beam.io.WriteToText(output_pattern)

)

p.run()

'''
Run the python script
 python DataFlowWithBeam.py 
 --input gs://amank-dataflow/input-dir/* 
 --output gs://amank-dataflow/output-dir/output1 
 --runner DataflowRunner 
 --project my-project-3-150520 
 --temp_location gs://amank-dataflow/temo-dir --region us-east1
'''