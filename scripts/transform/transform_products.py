import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2
import json

# Step 1: Read from PostgreSQL using psycopg2
def fetch_data_from_postgres():
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="retail_local",
        user="postgres",
        password="Nishardiva10!"
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM products_raw;")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    data = [dict(zip(columns, row)) for row in rows]
    cur.close()
    conn.close()
    print(f"Fetched {len(data)} rows from PostgreSQL")
    return data

# Step 2: Clean one row
class CleanProducts(beam.DoFn):
    def process(self, row):
        try:
            if not row.get('price') or not row.get('stockquantity') or not row.get('category'):
                return []  # Skip invalid rows

            return [{
                'ProductID': int(float(row['productid'])),
                'ProductName': row['productname'].strip().title() if row.get('productname') else None,
                'Category': row['category'].strip().title(),
                'Price': round(float(row['price']), 2),
                'StockQuantity': int(float(row['stockquantity']))
            }]
        except Exception as e:
            print("Skipping row due to error:", e)
            return []

# Step 3: Main pipeline logic
def run():
    raw_data = fetch_data_from_postgres()

    options = PipelineOptions(
        runner='DataflowRunner',
        project='de-practices',
        region='us-central1',
        temp_location='gs://retail-bronze-layer/temp/',
        staging_location='gs://retail-silver-layer/staging/',
        job_name='products-from-postgres'
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Load Raw Data' >> beam.Create(raw_data)
            | 'Clean Products' >> beam.ParDo(CleanProducts())
            | 'Convert to JSON' >> beam.Map(lambda x: json.dumps(x))  # ✅ FIX ADDED HERE
            | 'Write to GCS' >> beam.io.WriteToText(
                'gs://retail-silver-layer/products_cleaned_from_pg',
                file_name_suffix='.json'
            )
        )

if __name__ == '__main__':
    run()
