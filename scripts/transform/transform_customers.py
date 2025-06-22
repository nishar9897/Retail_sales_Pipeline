import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
from io import StringIO

class CleanCustomers(beam.DoFn):
    def process(self, line):
        f = StringIO(line)
        reader = csv.DictReader(f, fieldnames=['CustomerID', 'CustomerName', 'Email', 'Phone', 'City', 'Country'])
        for row in reader:
            if row['CustomerName'] and row['Email'] and row['Country']:
                yield {
                    'CustomerID': int(float(row['CustomerID'])),
                    'CustomerName': row['CustomerName'].strip().title(),
                    'Email': row['Email'].strip().lower(),
                    'Phone': row['Phone'].strip() if row['Phone'] else 'Unknown',
                    'City': row['City'].strip().title() if row['City'] else 'Unknown',
                    'Country': row['Country'].strip().title()
                }

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='de-practices',  # ✅ Update with your actual project ID
        region='us-central1',
        temp_location='gs://retail-bronze-layer/temp/',
        staging_location='gs://retail-bronze-layer/staging/',
        job_name='clean-customers'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read Customers' >> beam.io.ReadFromText('gs://retail-bronze-layer/customers_sample.csv', skip_header_lines=1)
            | 'Clean Customers' >> beam.ParDo(CleanCustomers())
            | 'Write Clean Customers' >> beam.io.WriteToText('gs://retail-silver-layer/customers_cleaned', file_name_suffix='.json')
        )

if __name__ == '__main__':
    run()
