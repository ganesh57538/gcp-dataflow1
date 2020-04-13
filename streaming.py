import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
def run():
        options = PipelineOptions()
        p = beam.Pipeline(options=options)
        lines = p | beam.io.ReadFromPubSub(topic='projects/gs-org-prj-01/topics/streamingdata') | beam.Map(lambda s: json.loads(s)) | beam.io.WriteToBigQuery('testproject2-261014:mydataset.mydata3',schema='name:string,complain:string',create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
        p.run().wait_until_finish()
if __name__ == '__main__':
  run()
