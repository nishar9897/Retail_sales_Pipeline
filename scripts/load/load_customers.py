import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import ast

import psycopg2

class InsertIntoPostgres(beam.DoFn):
    def __init__(self, host, dbname, user, password, port):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port

    def start_bundle(self):
        self.conn = psycopg2.connect(
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            port=self.port
        )
        self.cursor = self.conn.cursor()

    def process(self, element):
        data = ast.literal_eval(element)
        insert_query = """
            INSERT INTO Customers (CustomerID, CustomerName, Email, Phone, City, Country)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (CustomerID) DO NOTHING;
        """
        values = (
            data.get('CustomerID'),
            data.get('CustomerName'),
            data.get('Email'),
            data.get('Phone'),
            data.get('City'),
            data.get('Country')
        )
        self.cursor.execute(insert_query, values)
        self.conn.commit()

    def finish_bundle(self):
        self.cursor.close()
        self.conn.close()

def run():
    options = PipelineOptions([
    '--runner=DataflowRunner',
    '--project=de-practices',
    '--region=us-central1',
    '--temp_location=gs://retail-bronze-layer/temp/',
    '--staging_location=gs://retail-bronze-layer/staging/',
    '--job_name=load-customers-cloudsql'
])

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read Cleaned JSON from GCS' >> beam.io.ReadFromText('gs://retail-silver-layer/customers_cleaned-*.json')
            | 'Insert into PostgreSQL' >> beam.ParDo(InsertIntoPostgres(
                host='34.10.103.44',       
                dbname='retail_db',       
                user='postgres',          
                password='Nishardiva10!',  
                port=5432
            ))
        )

if __name__ == '__main__':
    run()
