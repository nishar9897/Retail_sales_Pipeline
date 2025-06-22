import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import psycopg2

class InsertIntoProducts(beam.DoFn):
    def __init__(self):
        self.conn = None
        self.cursor = None

    def setup(self):
        self.conn = psycopg2.connect(
            host='34.10.103.44',  # Replace with your Cloud SQL public IP
            dbname='retail_db',
            user='postgres',
            password='Nishardiva10!',
            port=5432
        )
        self.cursor = self.conn.cursor()

    def process(self, element):
        try:
            data = json.loads(element)

            # Defensive check for null ProductID
            if not data.get("ProductID"):
                print("Skipping row with null ProductID:", data)
                return

            insert_query = """
                INSERT INTO Products (ProductID, ProductName, Category, Price, StockQuantity)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (ProductID) DO NOTHING;
            """
            values = (
                int(data['ProductID']),
                data['ProductName'],
                data['Category'],
                float(data['Price']),
                int(data['StockQuantity'])
            )
            self.cursor.execute(insert_query, values)
            self.conn.commit()
        except Exception as e:
            print("Insert failed for row:", element)
            print("Error:", e)

    def teardown(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

def run():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read Product Data' >> beam.io.ReadFromText('gs://retail-silver-layer/products_cleaned_from_pg-00000-of-00001.json')
            | 'Insert into PostgreSQL' >> beam.ParDo(InsertIntoProducts())
        )

if __name__ == '__main__':
    run()
