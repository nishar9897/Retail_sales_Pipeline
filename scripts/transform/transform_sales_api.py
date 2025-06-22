import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import requests
import json
from datetime import datetime

class FetchSalesData(beam.DoFn):
    def process(self, _):
        url = 'https://raw.githubusercontent.com/nishar9897/Retail_sales_Pipeline/refs/heads/main/datasets/sales_sample.json'
        response = requests.get(url)
        data = json.loads(response.text)
        for row in data:
            yield row

class CleanSales(beam.DoFn):
    def process(self, record):
        try:
            if record['CustomerID'] and record['ProductID'] and record['Quantity']:
                yield {
                    'SaleID': int(float(record['SaleID'])),
                    'CustomerID': int(float(record['CustomerID'])),
                    'ProductID': int(float(record['ProductID'])),
                    'SaleDate': record['SaleDate'].strip(),
                    'Quantity': int(record['Quantity']),
                    'TotalAmount': round(float(record['TotalAmount']), 2) if record['TotalAmount'] else 0.00,
                    'ingestion_time': datetime.utcnow().isoformat()
                }
        except Exception as e:
            print("Skipping record due to error:", e)

def run():
    options = PipelineOptions(
        runner='DataflowRunner',
        project='de-practices',
        region='us-central1',
        temp_location='gs://retail-bronze-layer/temp/',
        staging_location='gs://retail-bronze-layer/staging/',
        job_name='clean-sales-api'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Trigger API' >> beam.Create([None])
            | 'Fetch Sales JSON' >> beam.ParDo(FetchSalesData())
            | 'Clean Sales Records' >> beam.ParDo(CleanSales())
            | 'Convert to JSON' >> beam.Map(lambda x: json.dumps(x))
            | 'Write Sales Cleaned' >> beam.io.WriteToText('gs://retail-silver-layer/sales_cleaned', file_name_suffix='.json')
        )

if __name__ == '__main__':
    run()
