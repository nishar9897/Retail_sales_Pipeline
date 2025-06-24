import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import psycopg2

class EnrichWithCustomerProduct(beam.DoFn):
    def setup(self):
        self.conn = psycopg2.connect(
            host="34.10.103.44",
            dbname="retail_db",
            user="postgres",
            password="Nishardiva10!",
            port="5432"
        )
        cursor = self.conn.cursor()

        # Load customer data
        cursor.execute("SELECT CustomerID, CustomerName FROM Customers")
        self.customers = {row[0]: row[1] for row in cursor.fetchall()}

        # Load product data
        cursor.execute("SELECT ProductID, ProductName FROM Products")
        self.products = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.close()
        self.conn.close()

    def process(self, record):
        customer_name = self.customers.get(record.get('CustomerID'))
        product_name = self.products.get(record.get('ProductID'))

        if customer_name and product_name:
            enriched_record = {
                'SaleID': record['SaleID'],
                'SaleDate': record['SaleDate'],
                'CustomerID': record['CustomerID'],
                'CustomerName': customer_name,
                'ProductID': record['ProductID'],
                'ProductName': product_name,
                'Quantity': record['Quantity'],
                'TotalAmount': record['TotalAmount']
            }
            yield enriched_record


class InsertIntoSalesMerged(beam.DoFn):
    def setup(self):
        self.conn = psycopg2.connect(
            host="34.10.103.44",
            dbname="retail_db",
            user="postgres",
            password="Nishardiva10!",
            port="5432"
        )
        self.cursor = self.conn.cursor()

    def process(self, record):
        try:
            insert_query = """
                INSERT INTO Sales_Merged (
                    SaleID, SaleDate, CustomerID, CustomerName,
                    ProductID, ProductName, Quantity, TotalAmount
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (SaleID) DO NOTHING;
            """
            self.cursor.execute(insert_query, (
                record['SaleID'],
                record['SaleDate'],
                record['CustomerID'],
                record['CustomerName'],
                record['ProductID'],
                record['ProductName'],
                record['Quantity'],
                record['TotalAmount']
            ))
            self.conn.commit()
        except Exception as e:
            print("Error inserting record:", record)
            print("Exception:", e)
            self.conn.rollback()

    def teardown(self):
        self.cursor.close()
        self.conn.close()


def run():
    options = PipelineOptions([
        '--runner=DataflowRunner',
        '--project=de-practices',
        '--region=us-central1',
        '--temp_location=gs://retail-bronze-layer/temp/',
        '--staging_location=gs://retail-bronze-layer/staging/',
        '--job_name=merge-sales-customers-products'
    ])

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read Sales Data' >> beam.io.ReadFromText('gs://retail-silver-layer/sales_cleaned-*.json')
            | 'Parse JSON' >> beam.Map(json.loads)
            | 'Enrich with Customer and Product Info' >> beam.ParDo(EnrichWithCustomerProduct())
            | 'Insert into Sales_Merged Table' >> beam.ParDo(InsertIntoSalesMerged())
        )

if __name__ == '__main__':
    run()
