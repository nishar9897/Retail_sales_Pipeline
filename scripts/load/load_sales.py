import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import psycopg2


# DoFn to filter valid sales records based on foreign keys
class FilterValidSales(beam.DoFn):
    def __init__(self):
        # These will be initialized later in setup()
        self.valid_product_ids = set()
        self.valid_customer_ids = set()

    def setup(self):
        conn = psycopg2.connect(
            host="34.10.103.44",  # Your actual DB IP
            dbname="retail_db",
            user="postgres",
            password="Nishardiva10!",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT productid FROM products")
        self.valid_product_ids = set(r[0] for r in cursor.fetchall())

        cursor.execute("SELECT customerid FROM customers")
        self.valid_customer_ids = set(r[0] for r in cursor.fetchall())

        cursor.close()
        conn.close()

    def process(self, record):
        if (
            record.get('ProductID') in self.valid_product_ids and
            record.get('CustomerID') in self.valid_customer_ids
        ):
            yield record


# DoFn to insert records into sales table
class InsertIntoSales(beam.DoFn):
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
            self.cursor.execute("""
                INSERT INTO sales (SaleID, CustomerID, ProductID, SaleDate, Quantity, TotalAmount)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                record['SaleID'],
                record['CustomerID'],
                record['ProductID'],
                record['SaleDate'],
                record['Quantity'],
                record['TotalAmount']
            ))
            self.conn.commit()
        except Exception as e:
            print("Error inserting record:", json.dumps(record))
            print("Exception:", e)
            self.conn.rollback()

    def teardown(self):
        self.cursor.close()
        self.conn.close()


def run():
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    (
        p
        | 'Read Cleaned Sales' >> beam.io.ReadFromText(
            'gs://retail-silver-layer/sales_cleaned-00000-of-00001.json')
        | 'Parse JSON' >> beam.Map(json.loads)
        | 'Filter Valid Records' >> beam.ParDo(FilterValidSales())
        | 'Insert Sales to DB' >> beam.ParDo(InsertIntoSales())
    )

    p.run().wait_until_finish()


if __name__ == '__main__':
    run()
