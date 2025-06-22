## 📌 Challenges Faced During Data Cleaning & Loading

- **Name Length Limit**  
  Names longer than 10 characters caused schema issues – trimmed them before load.

- **Phone Format Issues**  
  Phone numbers with `+1`, dashes, or brackets had to be cleaned to valid 10-digit format.

- **Data Type Mismatch**  
  Quantity and amount fields were strings instead of numbers – required type conversion.

- **Missing Product Fields**  
  Product dataset had nulls – only 57 valid records remained after filtering.

- **Invalid Dates**  
  Malformed or empty `saleDate` values needed date parsing or default values.

- **Foreign Key Mismatches**  
  Sales records referred to non-existent products/customers – filtered out.

- **Cloud SQL Timeout**  
  Got `connection timed out` errors – fixed by allowing IP in authorized networks.

- **Beam DoFn Warning**  
  “No iterator returned” warning – resolved by yielding data properly.

- **Overwrite Risk**  
  Overwriting Parquet caused data loss – switched to append with deduplication.

- **Beam Setup DB Failure**  
  Psycopg2 failed in setup() – fixed by ensuring network access and correct IP.
